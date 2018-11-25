use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use operator::ConstructableOperator;
use data::{Data};
use std::cmp::Ordering;
use serde_json;
use std::collections::HashMap;
use spillable_store::WritableSpillableStore;
use std::fs::File;

enum PlainOrSpillable {
    Spillable(WritableSpillableStore),
    Plain(OperatorReadBuffer)
}

impl PlainOrSpillable {
    fn read(&mut self) -> OperatorReadBuffer {
        if let PlainOrSpillable::Spillable(v) = self {
            return v.read().1;
        }

        panic!("Call to PlainOrSpillable::read when plain");
    }

    fn into_read(self) -> OperatorReadBuffer {
        return match self {
            PlainOrSpillable::Spillable(v) => v.into_read_buffer().1,
            PlainOrSpillable::Plain(v) => v
        };
    }
}


const HASHTABLE_SIZE_LIMIT: usize = 4096;

pub struct HashJoin {
    left: OperatorReadBuffer,
    right: Option<OperatorReadBuffer>,
    out: OperatorWriteBuffer,
    left_cols: Vec<usize>,
    right_cols: Vec<usize>
}

fn extract_keys(row: &[Data], cols: &[usize]) -> Vec<Data> {
    let mut to_r = Vec::with_capacity(cols.len());

    for &idx in cols {
        to_r.push(row[idx].clone());
    }

    return to_r;
}

impl HashJoin {
    pub fn new(left: OperatorReadBuffer, right: OperatorReadBuffer,
               out: OperatorWriteBuffer,
               left_cols: Vec<usize>,
               right_cols: Vec<usize>)
               -> HashJoin {

        assert!(left_cols.len() == right_cols.len());
        
        return HashJoin {
            left, right: Some(right),
            out, left_cols, right_cols
        };
    }

    fn emit_rows(mut buf: OperatorReadBuffer,
                 ht: &HashMap<Vec<Data>, Vec<Vec<Data>>>,
                 col_idxes: &[usize],
                 out: &mut OperatorWriteBuffer) {
        iterate_buffer!(buf, row, {
            let key2 = extract_keys(row, col_idxes);
            if let Some(matches) = ht.get(&key2) {
                // all these rows match.
                for matching_row in matches.iter() {
                    let mut out_row = Vec::new();
                    out_row.extend_from_slice(matching_row);
                    out_row.extend_from_slice(row);
                    out.write(out_row);
                }
            }
        });
    }

 
    pub fn start(mut self) {
        let mut ht: HashMap<Vec<Data>, Vec<Vec<Data>>> = HashMap::new();
        let mut rows_added = 0;

        let right_types = self.right.as_ref().unwrap().types().to_vec().clone();
        let mut right_buffer = PlainOrSpillable::Plain(self.right.take().unwrap());

        iterate_buffer!(self.left, row, {
            // add rows to the hashtable ht until it is at capacity.
            let key = extract_keys(row, &self.left_cols);
            ht.entry(key)
                .or_insert(Vec::new())
                .push(row.to_vec());

            rows_added += 1;

            if rows_added >= HASHTABLE_SIZE_LIMIT {
                // we have spilled! if this is the first time we've spilled,
                // we need to write the entire right child into a buffer so
                // we can iterate over it multiple times.
                if let PlainOrSpillable::Plain(mut v) = right_buffer {
                    // this is the first time we've spilled.
                    let mut buf = WritableSpillableStore::new(
                        4096, right_types.clone());

                    iterate_buffer!(v, row2, {
                        buf.push_row(row2);
                    });

                    right_buffer = PlainOrSpillable::Spillable(buf);
                }

                // check every row on the right
                let mut buf = right_buffer.read();

                // this cannot be a self method because we cannot borrow
                // self as mutable more than once
                HashJoin::emit_rows(buf, &ht, &self.right_cols,
                                    &mut self.out);
                
                rows_added = 0;
                ht.clear();
            }
        });
         
        HashJoin::emit_rows(right_buffer.into_read(), &ht,
                            &self.right_cols,
                            &mut self.out);
    }
}

impl ConstructableOperator for HashJoin {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    mut input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    options: serde_json::Value) -> Self {
        
        assert!(file.is_none());
        let o = output.unwrap();

        assert_eq!(input.len(), 2);
        let lb = input.remove(0);
        let rb = input.remove(0);

        assert!(options["left_cols"].is_array(),
                "hash join operator missing left_cols array!");

        assert!(options["right_cols"].is_array(),
                "hash join operator missing right_cols array!");


        let left_cols = options["left_cols"].as_array().unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap() as usize)
            .collect();

        let right_cols = options["right_cols"].as_array().unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap() as usize)
            .collect();

        
        return HashJoin::new(lb, rb, o, left_cols, right_cols);
    }
}


#[cfg(test)]
mod tests {
    use operator::join::HashJoin;
    use operator_buffer::{make_buffer_pair};
    use data::{Data, DataType};

    #[test]
    fn equijoin() {
        let (r1, mut w1) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        let (r2, mut w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        let (mut r, w) = make_buffer_pair(5, 10, vec![DataType::INTEGER,
                                                      DataType::INTEGER]);

        w1.write(vec![Data::Integer(5)]);
        w1.write(vec![Data::Integer(6)]);
        w1.write(vec![Data::Integer(7)]);
        drop(w1);
        
        w2.write(vec![Data::Integer(5)]);
        w2.write(vec![Data::Integer(5)]);
        w2.write(vec![Data::Integer(5)]);
        w2.write(vec![Data::Integer(8)]);
        drop(w2);

        let j = HashJoin::new(r1, r2, w, vec![0], vec![0]);
        j.start();

        let mut rc = 0;
        iterate_buffer!(r, idx, row, {
            rc += 1;
            match idx {
                0 => { assert_eq!(row.to_vec(), vec![Data::Integer(5), Data::Integer(5)]); },
                1 => { assert_eq!(row.to_vec(), vec![Data::Integer(5), Data::Integer(5)]); },
                2 => { assert_eq!(row.to_vec(), vec![Data::Integer(5), Data::Integer(5)]); },
                _ => { panic!("too many rows"); }
            };
        });

        assert_eq!(rc, 3);
    }
}

