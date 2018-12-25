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
use operator::ConstructableOperator;
use spillable_store::WritableSpillableStore;
use data::{Data};
use serde_json;
use predicate::Predicate;
use either::*;
use std::fs::File;

type PredicateOrFunc = Either<Predicate, fn(&[Data], &[Data]) -> bool>;

pub struct LoopJoin {
    left: OperatorReadBuffer,
    right: OperatorReadBuffer,
    out: OperatorWriteBuffer,
    predicate: PredicateOrFunc
}

impl LoopJoin {
    #[cfg(test)]
    pub fn new(left: OperatorReadBuffer, right: OperatorReadBuffer,
           out: OperatorWriteBuffer, predicate: fn(&[Data], &[Data]) -> bool)
           -> LoopJoin {
        return LoopJoin {
            left, right, out, predicate: Right(predicate)
        };
    }

    pub fn new_with_interp(left: OperatorReadBuffer, right: OperatorReadBuffer,
                           out: OperatorWriteBuffer, predicate: Predicate)
                           -> LoopJoin {
        return LoopJoin {
            left, right, out, predicate: Left(predicate)
        };
    }

    pub fn start(mut self) {
        let mut buf = WritableSpillableStore::new(
            4096, self.left.types().to_vec());
        
        // first, read the left relation into the buffer
        iterate_buffer!(self.left, row, {
            buf.push_row(row);
        });

        // next, iterate over the right hand relation
        match self.predicate {
            Right(f) => {
                iterate_buffer!(self.right, right_row, {
                    let (_, mut left_data) = buf.read();
                    iterate_buffer!(left_data, left_row, {
                        if (f)(left_row, right_row) {
                            // it's a match! it is in the join result.
                            let mut out_row = Vec::new();
                            out_row.extend_from_slice(left_row);
                            out_row.extend_from_slice(right_row);
                            self.out.write(out_row);
                        }
                    });
                });
            },

            Left(p) => {
                iterate_buffer!(self.right, right_row, {
                    let (_, mut left_data) = buf.read();
                    iterate_buffer!(left_data, left_row, {
                        if p.eval_with_2(left_row, right_row) {
                            // it's a match! it is in the join result.
                            let mut out_row = Vec::new();
                            out_row.extend_from_slice(left_row);
                            out_row.extend_from_slice(right_row);
                            self.out.write(out_row);
                        }
                    });
                });
            }
        }
    }
}

impl ConstructableOperator for LoopJoin {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    mut input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    options: serde_json::Value) -> Self {
        
        assert!(file.is_none());
        let o = output.unwrap();

        assert_eq!(input.len(), 2);
        let lb = input.remove(0);
        let rb = input.remove(0);

        let pred = Predicate::from_json(&options["predicate"]);

        return LoopJoin::new_with_interp(lb, rb, o, pred);
    }
}

#[cfg(test)]
mod tests {
    use operator::join::LoopJoin;
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

        let j = LoopJoin::new(r1, r2, w, |d1, d2| d1[0] == d2[0]);
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

        let j = LoopJoin::new(r1, r2, w, |d1, d2| d1[0] == d2[0]);
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

