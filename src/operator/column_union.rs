use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use operator::ConstructableOperator;
use data::{Data};
use std::collections::VecDeque;
use serde_json;
use std::fs::File;

pub struct ColumnUnion {
    readers: Vec<OperatorReadBuffer>,
    writer: OperatorWriteBuffer,
    queues: Vec<VecDeque<Vec<Data>>>
        
}

impl ColumnUnion {
    pub fn new(readers: Vec<OperatorReadBuffer>,
               writer: OperatorWriteBuffer) -> ColumnUnion {
        let mut v = Vec::new();
        for _ in 0..readers.len() {
            v.push(VecDeque::new());
        }
        
        return ColumnUnion {
            readers, writer, queues: v
        };
    }

    fn get_value_from_queue(&mut self, idx: usize) -> Option<Vec<Data>> {
        // fetch a row from the deque at the given index. If the deque is empty,
        // try to add another block to it from the underlying readers. If there's
        // no more data to be read, return None.
        
        let q = &mut self.queues[idx];
        match q.pop_front() {
            None => {
                // try to get another block.
                let rdr = &mut self.readers[idx];
                
                let to_r = {
                    let new_block = rdr.data();
                    match new_block {
                        None => None,
                        Some(rb) => {
                            for row in rb.iter() {
                                q.push_back(row.to_vec());
                            }
                            
                            Some(q.pop_front().unwrap())
                        }
                    }
                };

                rdr.progress();
                return to_r;
            },
            Some(row) => { return Some(row); }
        };
    }

    pub fn start(mut self) {
        // loop through each VecDeque, adding more columns to our current row.
        // when we hit an empty VecDeque, try to read more blocks into it. If we
        // don't have any more blocks, we're done! (truncate to shortest relation,
        // meaning the relation with the fewest rows)
        let mut curr_row = Vec::new();
        loop {
            for idx in 0..self.queues.len() {
                match self.get_value_from_queue(idx) {
                    Some(mut row) => { curr_row.append(&mut row); }
                    None => { return; }
                }
            }

            self.writer.copy_and_write(&curr_row);
            curr_row.clear();
        }
        
    }
}

impl ConstructableOperator for ColumnUnion {
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
}
