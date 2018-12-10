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
    input: OperatorReadBuffer,
    output: OperatorWriteBuffer,
    by_cols: Vec<usize>,
    buf: Vec<Data>,
    buf_size: usize
}

impl Sort {
    fn new(input: OperatorReadBuffer, output: OperatorWriteBuffer,
           by_cols: Vec<usize>, buf_size: usize) -> Sort {
        return Sort {
            input, output, by_cols, buf_size,
            buf: Vec::with_capacity(buf_size)
        };
    }

    fn sort_and_dump_buf<F>(buf: Vec<Data>,
                            types: &[DataType],
                            sort_fn: F) -> WritableSpillableStore
    where F: Fn(&&[Data], &&[Data]) -> Ordering {

        // TODO at some point we should make push_row take ownership
        // but for now, we will explicitly drop buf here
        let to_r = {
            // sort the buffer
            let mut by_rows: Vec<&[Data]> = buf
                .chunks(types.len())
                .collect();
            
            by_rows.sort_unstable_by(sort_fn);
            
            let mut store = WritableSpillableStore::new(
                1024, types.to_vec());
            
            for row in by_rows {
                store.push_row(row);
            }
            store
        };
        drop(buf);


        return to_r;
    }
    
    pub fn start(mut self) {
        let types = self.input.types().to_vec();
        let by_cols = self.by_cols.clone();
        let mut chunks = Vec::new();

        let sort_fn = |el1: &&[Data], el2: &&[Data]| {
            for &col_idx in by_cols.iter() {
                match el1[col_idx].partial_cmp(&el2[col_idx]).unwrap() {
                    Ordering::Greater => { return Ordering::Greater; },
                    Ordering::Less =>  { return Ordering::Less; }
                    _ => {}
                };
            }
            return Ordering::Equal;
        };
        
        iterate_buffer!(self.input, row, {
            self.buf.extend_from_slice(row);

            if self.buf.len() >= self.buf_size {
                // time to dump this chunk to disk.
                let mut loc_buf = Vec::with_capacity(self.buf_size);
                mem::swap(&mut self.buf, &mut loc_buf);

                let r = Sort::sort_and_dump_buf(loc_buf, &types, sort_fn);
                chunks.push(r);
            }
        });

        // dump the remaining rows in the buffer, if any.
        if !self.buf.is_empty() {
            let mut loc_buf = Vec::with_capacity(0);
            mem::swap(&mut self.buf, &mut loc_buf);

            let r = Sort::sort_and_dump_buf(loc_buf, &types, sort_fn);
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
                 return sort_fn(&r1.unwrap().as_slice(),
                                &r2.unwrap().as_slice()).reverse();
            });
        
        for r in readers {
            if r.peek().is_none() {
                continue;
            }
            bheap.push(r);
        }

        while !bheap.is_empty() {
            let mut next_reader = bheap.pop().unwrap();
            let next_row = next_reader.pop().unwrap();

            // write the row to the output, add the reader
            // back into the heap.
            self.output.write(next_row);

            if next_reader.peek().is_some() {
                bheap.push(next_reader);
            }
        }
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
        
        return Sort::new(ib, ob, cols, 4096);
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
        for _ in 0..10005 {
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
}
