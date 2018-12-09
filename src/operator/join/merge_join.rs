use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer, PeekableOperatorReadBuffer};
use operator::ConstructableOperator;
use data::{Data};
use std::cmp::Ordering;
use serde_json;
use std::fs::File;


pub struct MergeJoin {
    left: OperatorReadBuffer,
    right: OperatorReadBuffer,
    out: OperatorWriteBuffer,
    left_cols: Vec<usize>,
    right_cols: Vec<usize>
}


fn matches_on_cols(r1: &[Data], r2: &[Data], cols: &[usize]) -> bool {
    debug_assert!(r1.len() >= cols.len());
    debug_assert!(r2.len() >= cols.len());
    
    for &col in cols {
        if r1[col] != r2[col] {
            return false;
        }
    }

    return true;
}

fn cmp_on_col_sets(r1: &[Data], r2: &[Data], cols1: &[usize], cols2: &[usize]) -> Ordering {
    debug_assert!(cols1.len() == cols2.len());
    debug_assert!(r1.len() >= cols1.len());
    debug_assert!(r2.len() >= cols2.len());
    
    for (&col1, &col2) in cols1.iter().zip(cols2) {
        match r1[col1].partial_cmp(&r2[col2]).unwrap() {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => {}
        };
    }

    return Ordering::Equal;
}

impl MergeJoin {
    pub fn new(left: OperatorReadBuffer, right: OperatorReadBuffer,
               out: OperatorWriteBuffer,
               left_cols: Vec<usize>,
               right_cols: Vec<usize>)
               -> MergeJoin {
        return MergeJoin {
            left, right,
            out, left_cols, right_cols
        };
    }

    fn read_matching(buf: &mut PeekableOperatorReadBuffer,
                     cols: &[usize]) -> Option<Vec<Vec<Data>>> {
        let first_row = match buf.pop() {
            None => { return None; },
            Some(e) => e
        };

        let mut to_r = Vec::new();
        to_r.push(first_row);

        loop {
            if let Some(r) = buf.peek() {
                if !matches_on_cols(&to_r[0], r, cols) {
                    // make sure the input is sorted
                    debug_assert_matches!(cmp_on_col_sets(
                        &to_r[0], &r,
                        cols, cols), Ordering::Less);

                    break;
                }
            } else {
                // out of rows!
                break;
            }

            // we match the previous rows on the predicate columns.
            to_r.push(buf.pop().unwrap());
        }

        return Some(to_r);
    }
    
    pub fn start(mut self) {
        let mut pleft = PeekableOperatorReadBuffer::new(self.left);
        let mut pright = PeekableOperatorReadBuffer::new(self.right);
        
        let mut left_set = match MergeJoin::read_matching(&mut pleft, &self.left_cols) {
            Some(v) => v,
            None => { return; }
        };
        
        let mut right_set = match MergeJoin::read_matching(&mut pright, &self.right_cols) {
            Some(v) => v,
            None => { return; }
        };

        loop {
            // check to see if the left and right match
            match cmp_on_col_sets(&left_set[0], &right_set[0],
                                      &self.left_cols, &self.right_cols) {
                Ordering::Equal => {
                    // emit the product
                    for l in left_set.iter() {
                        for r in right_set.iter() {
                            let mut result = Vec::new();
                            result.extend_from_slice(l);
                            result.extend_from_slice(r);
                            self.out.write(result);
                        }
                    }
                    // progress both the left and the right
                    left_set = match MergeJoin::read_matching(&mut pleft, &self.left_cols) {
                        Some(v) => v,
                        None => { return; }
                    };
                    
                    right_set = match MergeJoin::read_matching(&mut pright, &self.right_cols) {
                        Some(v) => v,
                        None => { return; }
                    };
                },

                Ordering::Greater => {
                    // progress the right
                    right_set = match MergeJoin::read_matching(&mut pright, &self.right_cols) {
                        Some(v) => v,
                        None => { return; }
                    };
                },

                Ordering::Less => {
                    // progress the left
                    left_set = match MergeJoin::read_matching(&mut pleft, &self.left_cols) {
                        Some(v) => v,
                        None => { return; }
                    };
                }
            }
        }
    }
}

impl ConstructableOperator for MergeJoin {
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
                "merge join operator missing left_cols array!");

        assert!(options["right_cols"].is_array(),
                "merge join operator missing right_cols array!");


        let left_cols = options["left_cols"].as_array().unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap() as usize)
            .collect();

        let right_cols = options["right_cols"].as_array().unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap() as usize)
            .collect();

        
        return MergeJoin::new(lb, rb, o, left_cols, right_cols);
    }
}

#[cfg(test)]
mod tests {
    use operator::join::MergeJoin;
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

        let j = MergeJoin::new(r1, r2, w, vec![0], vec![0]);
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

    #[test]
    fn wide_equijoin() {
        let (r1, mut w1) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::TEXT]);
        let (r2, mut w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER]);
        let (mut r, w) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::TEXT,
                                                      DataType::INTEGER, DataType::INTEGER]);

        w1.write(vec![Data::Integer(5), Data::Text(String::from("hello!"))]);
        w1.write(vec![Data::Integer(5), Data::Text(String::from("world"))]);
        w1.write(vec![Data::Integer(7), Data::Text(String::from("does not appear"))]);
        drop(w1);
        
        w2.write(vec![Data::Integer(5), Data::Integer(2)]);
        w2.write(vec![Data::Integer(5), Data::Integer(3)]);
        w2.write(vec![Data::Integer(5), Data::Integer(4)]);
        w2.write(vec![Data::Integer(8), Data::Integer(5)]);
        drop(w2);

        let j = MergeJoin::new(r1, r2, w, vec![0], vec![0]);
        j.start();

        let mut rc = 0;
        iterate_buffer!(r, row, {
            rc += 1;
            assert!(row[1] != Data::Text(String::from("does not appear")));
            assert!(row[3] != Data::Integer(5));
        });

        assert_eq!(rc, 6);
    }
    
}

