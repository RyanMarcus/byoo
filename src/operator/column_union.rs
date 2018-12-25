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
use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use row_buffer::RowBuffer;
use operator::ConstructableOperator;
use data::{Data};
use std::collections::VecDeque;
use serde_json;
use std::fs::File;
use std::usize;

pub struct ColumnUnion {
    readers: Vec<OperatorReadBuffer>,
    writer: OperatorWriteBuffer
}

impl ColumnUnion {
    pub fn new(readers: Vec<OperatorReadBuffer>,
               writer: OperatorWriteBuffer) -> ColumnUnion {
        
        return ColumnUnion {
            readers, writer
        };
    }
   
    pub fn start(mut self) {
        if self.readers.iter().all(|r| r.types().len() == 1) {
            // use the specialized form
            self.do_single_width_columns();
        } else {
            // use the regular form
            self.do_multi_width_columns();
        }
    }

    fn do_single_width_columns(mut self) {
        let result_width = self.readers.len();

        loop {
            {
                let mut bufs: Vec<&[Data]> = Vec::with_capacity(result_width);
                let mut fewest_rows = std::usize::MAX;
                for rdr in self.readers.iter_mut() {
                    if let Some(buf) = rdr.data() {
                        bufs.push(buf.raw_data());
                        fewest_rows = std::cmp::min(buf.num_rows(), fewest_rows);
                    } else {
                        return;
                    }
                }

                self.writer.copy_and_write_from(fewest_rows, &bufs);
            }

            for rdr in self.readers.iter_mut() {
                rdr.progress();
            }
            
        }
    }

    fn do_multi_width_columns(mut self) {
        let result_width: usize = self.readers.iter()
            .map(|r| r.types().len()).sum();
        let mut row = Vec::with_capacity(result_width);
        loop {
            {
                let mut bufs: Vec<&mut RowBuffer> = Vec::new();
                for rdr in self.readers.iter_mut() {
                    if let Some(buf) = rdr.data() {
                        bufs.push(buf);
                    } else {
                        return;
                    }
                }
                
                let result_height = bufs.iter()
                    .map(|b| b.num_rows())
                    .min().unwrap();


                for row_idx in 0..result_height {
                    for rb in bufs.iter() {
                        row.extend_from_slice(rb.get_row(row_idx));
                    }
                    self.writer.copy_and_write(&row);
                    row.clear();
                }
            }
            
            for rdr in self.readers.iter_mut() {
                rdr.progress();
            }

        }   
    }
}

impl  ConstructableOperator for ColumnUnion {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    _options: serde_json::Value) -> Self {
        
        assert!(file.is_none());
        let o = output.unwrap();

        return ColumnUnion::new(input, o);
    }
}

#[cfg(test)]
mod tests {
    use operator::column_union::ColumnUnion;
    use operator_buffer::{make_buffer_pair};
    use data::{Data, DataType};
    
    #[test]
    fn combines_two_columns() {
        let (mut r, w) = make_buffer_pair(5, 10, vec![DataType::INTEGER,
                                                      DataType::INTEGER]);

        let (r1, mut w1) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        let (r2, mut w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        let cunion = ColumnUnion::new(vec![r1, r2], w);

        w1.write(vec![Data::Integer(-5)]);
        w1.write(vec![Data::Integer(-7)]);
        w1.write(vec![Data::Integer(-9)]);
        drop(w1);

        w2.write(vec![Data::Integer(5)]);
        w2.write(vec![Data::Integer(6)]);
        w2.write(vec![Data::Integer(7)]);
        drop(w2);

        cunion.start();

        let mut rc = 0;
        iterate_buffer!(r, idx, row, {
            rc += 1;
            match idx {
                0 => { assert_eq!(row.to_vec(), vec![Data::Integer(-5), Data::Integer(5)]); },
                1 => { assert_eq!(row.to_vec(), vec![Data::Integer(-7), Data::Integer(6)]); },
                2 => { assert_eq!(row.to_vec(), vec![Data::Integer(-9), Data::Integer(7)]); },
                _ => { panic!("too many rows!"); }
            };
        });

        assert_eq!(rc, 3);
    }

    #[test]
    fn combines_two_columns_multibuf() {
        let (mut r, w) = make_buffer_pair(5, 10, vec![DataType::INTEGER,
                                                      DataType::INTEGER]);

        let (r1, mut w1) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        let (r2, mut w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        let cunion = ColumnUnion::new(vec![r1, r2], w);

        for i in 0..45 {
            w1.write(vec![Data::Integer(i)]);
        }
        drop(w1);

        for i in 0..45 {
            w2.write(vec![Data::Integer(i*2)]);
        }
        drop(w2);

        cunion.start();

        let mut rc = 0;
        iterate_buffer!(r, row, {
            rc += 1;
            if let Data::Integer(i) = row[0] {
                if let Data::Integer(j) = row[1] {
                    assert_eq!(i*2, j);
                } else {
                    panic!("Wrong datatype.");
                }
            } else {
                panic!("Wrong datatype.");
            }
        });

        assert_eq!(rc, 45);
    }

     #[test]
    fn combines_multicolumns_multibuf() {
        let (mut r, w) = make_buffer_pair(5, 10, vec![DataType::INTEGER,
                                                      DataType::INTEGER,
                                                      DataType::INTEGER]);

        let (r1, mut w1) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        let (r2, mut w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER]);

        let cunion = ColumnUnion::new(vec![r1, r2], w);

        for i in 0..45 {
            w1.write(vec![Data::Integer(i)]);
        }
        drop(w1);

        for i in 0..45 {
            w2.write(vec![Data::Integer(i*2), Data::Integer(i*3)]);
        }
        drop(w2);

        cunion.start();

        let mut rc = 0;
        iterate_buffer!(r, row, {
            rc += 1;
            if let Data::Integer(i) = row[0] {
                if let Data::Integer(j) = row[1] {
                    if let Data::Integer(k) = row[2] {
                        assert_eq!(i*2, j);
                        assert_eq!(i*3, k);
                    } else {
                        panic!("Wrong datatype.");
                    }
                } else {
                    panic!("Wrong datatype.");
                }
            } else {
                panic!("Wrong datatype.");
            }
        });

        assert_eq!(rc, 45);
    }
}
