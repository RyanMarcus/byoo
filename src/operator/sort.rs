// < begin copyright > 
// Copyright Ryan Marcus 2018
// 
// This file is part of byoo.
// 
// byoo is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// byoo is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with byoo.  If not, see <http://www.gnu.org/licenses/>.
// 
// < end copyright > 
use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer, PeekableOperatorReadBuffer};
use operator::ConstructableOperator;
use spillable_store::WritableSpillableStore;
use data::{Data, DataType};
use std::mem;
use std::cmp::Ordering;
use binary_heap_plus::*;
use std::fs::File;
use serde_json;

pub struct Sort {
    input: Option<OperatorReadBuffer>,
    output: OperatorWriteBuffer,
    by_cols: Vec<usize>,
    buf: Vec<Vec<Data>>,
    buf_size: usize
}

impl Sort {
    fn new(input: OperatorReadBuffer, output: OperatorWriteBuffer,
           by_cols: Vec<usize>, buf_size: usize) -> Sort {
        return Sort {
            input: Some(input),
            output, by_cols, buf_size,
            buf: Vec::with_capacity(buf_size)
        };
    }

    fn sort_and_dump_buf<F>(&mut self,
                            types: &[DataType],
                            sort_fn: F) -> WritableSpillableStore
    where F: Fn(&Vec<Data>, &Vec<Data>) -> Ordering {

        self.buf.sort_unstable_by(sort_fn);
            
        let mut store = WritableSpillableStore::new(
            1024, types.to_vec());
            
        for row in self.buf.iter() {
            store.push_row(row);
        }
        self.buf.clear();
        
        return store;
    }
    
    pub fn start(mut self) {
        let types = self.input.as_ref().unwrap().types().to_vec();
        let by_cols = self.by_cols.clone();
        let mut chunks = Vec::new();

        let sort_fn = |el1: &Vec<Data>, el2: &Vec<Data>| {
            for &col_idx in by_cols.iter() {
                match el1[col_idx].partial_cmp(&el2[col_idx]).unwrap() {
                    Ordering::Greater => { return Ordering::Greater; },
                    Ordering::Less =>  { return Ordering::Less; }
                    _ => {}
                };
            }
            return Ordering::Equal;
        };

        let sort_fn2 = |el1: &[Data], el2: &[Data]| {
            for &col_idx in by_cols.iter() {
                match el1[col_idx].partial_cmp(&el2[col_idx]).unwrap() {
                    Ordering::Greater => { return Ordering::Greater; },
                    Ordering::Less =>  { return Ordering::Less; }
                    _ => {}
                };
            }
            return Ordering::Equal;
        };

        let mut row_count = 0;
        let mut inp_buf = self.input.take().unwrap();
        iterate_buffer!(inp_buf, row, {
            row_count += 1;
            self.buf.push(row.to_vec());

            if self.buf.len() >= self.buf_size {
                // time to dump this chunk to disk.
                let r = self.sort_and_dump_buf(&types, sort_fn);
                chunks.push(r);
            }
        });

        // dump the remaining rows in the buffer, if any.
        if !self.buf.is_empty() {
            let r = self.sort_and_dump_buf(&types, sort_fn);
            chunks.push(r);
        }

        // next, we have to merge the sorted fragments.
        let readers: Vec<PeekableOperatorReadBuffer> = chunks.iter_mut()
            .map(|r| r.read())
            .map(|(_, r)| PeekableOperatorReadBuffer::new(r))
            .collect();

        let mut bheap = BinaryHeap::new_by(
            |h1: &PeekableOperatorReadBuffer,
             h2: &PeekableOperatorReadBuffer| -> Ordering {
                
                 let r1 = h1.peek();
                 let r2 = h2.peek();

                 // reverse here because the heap is a max heap
                 return sort_fn2(r1.unwrap(),
                                 r2.unwrap()).reverse();
            });
        
        for r in readers {
            if r.peek().is_none() {
                continue;
            }
            bheap.push(r);
        }

        let mut out_row_count = 0;
        while !bheap.is_empty() {
            let mut next_reader = bheap.pop().unwrap();
            {
                let next_row = next_reader.pop().unwrap();
                // write the row to the output, add the reader
                // back into the heap.
                out_row_count += 1;
                self.output.write(next_row);
            }

            if next_reader.peek().is_some() {
                bheap.push(next_reader);
            }
        }

        assert_eq!(row_count, out_row_count);
    }
}

impl ConstructableOperator for Sort {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    mut input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    options: serde_json::Value) -> Self {
        
        assert!(file.is_none());
        let ob = output.unwrap();

        assert_eq!(input.len(), 1);
        let ib = input.remove(0);

        assert!(options["cols"].is_array(),
                "Sort operator requires cols array option");
        
        let cols = options["cols"].as_array().unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap() as usize)
            .collect();
        
        return Sort::new(ib, ob, cols, 4096*4);
    }
}

#[cfg(test)]
mod tests {
    use operator::Sort;
    use operator_buffer::{make_buffer_pair};
    use data::{Data, DataType};
    use rand::prelude::*;
    use std::thread;
    
    #[test]
    fn sorts_single_col() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        let (mut r2, w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(-50)]);
        w.write(vec![Data::Integer(-90)]);
        w.write(vec![Data::Integer(40)]);
        w.write(vec![Data::Integer(0)]);
        w.write(vec![Data::Integer(19)]);
        drop(w);

        let s = Sort::new(r, w2, vec![0], 100);
        s.start();

        let mut res = Vec::new();
        iterate_buffer!(r2, row, {
            if let Data::Integer(i) = row[0] {
                res.push(i);
            } else {
                panic!("wrong datatype");
            }
        });

        assert_eq!(res, vec![-90, -50, 0, 19, 40]);
    }

    #[test]
    fn sorts_spill() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        let (mut r2, w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        let mut data = Vec::new();
        let mut byoo_data = Vec::new();
        for _ in 0..20005 {
            let r = random::<i64>();
            data.push(r);
            byoo_data.push(Data::Integer(r));
        }
        data.sort();

        thread::spawn(move || {
            for d in byoo_data {
                w.write(vec![d]);
            }
        });

        thread::spawn(move || {
            let s = Sort::new(r, w2, vec![0], 100);
            s.start();
        });

        let mut res = Vec::new();
        iterate_buffer!(r2, row, {
            if let Data::Integer(i) = row[0] {
                res.push(i);
            } else {
                panic!("wrong datatype");
            }
        });

        assert_eq!(res, data);
    }

    #[test]
    fn sorts_spill_multicol() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER]);
        let (mut r2, w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER]);

        let mut data = Vec::new();
        let mut byoo_data = Vec::new();
        for _ in 0..20005 {
            let r = random::<i64>();
            data.push(r);
            byoo_data.push(vec![Data::Integer(r), Data::Integer(1000)]);
        }
        data.sort();

        thread::spawn(move || {
            for d in byoo_data {
                w.write(d);
            }
        });

        thread::spawn(move || {
            let s = Sort::new(r, w2, vec![0], 1000);
            s.start();
        });

        let mut res = Vec::new();
        iterate_buffer!(r2, row, {
            if let Data::Integer(i) = row[0] {
                res.push(i);
            } else {
                panic!("wrong datatype");
            }
        });

        assert_eq!(res.len(), data.len());
        assert_eq!(res, data);
    }
}
