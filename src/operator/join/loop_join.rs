use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use spillable_store::WritableSpillableStore;
use data::{Data};

pub struct LoopJoin {
    left: OperatorReadBuffer,
    right: OperatorReadBuffer,
    out: OperatorWriteBuffer,
    predicate: fn(&[Data], &[Data]) -> bool,
}

impl LoopJoin {
    pub fn new(left: OperatorReadBuffer, right: OperatorReadBuffer,
           out: OperatorWriteBuffer, predicate: fn(&[Data], &[Data]) -> bool)
           -> LoopJoin {
        return LoopJoin {
            left, right, out, predicate
        };
    }

    pub fn start(mut self) {
        let mut buf = WritableSpillableStore::new(
            4096, self.left.types().to_vec());
        
        // first, read the left relation into the buffer
        iterate_buffer!(self.left, row, {
            buf.push_row(row.to_vec());
        });

        // next, iterate over the right hand relation
        iterate_buffer!(self.right, right_row, {
            let (_, mut left_data) = buf.read();
            iterate_buffer!(left_data, left_row, {
                if (self.predicate)(left_row, right_row) {
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

#[cfg(test)]
mod tests {
    use operator::join::LoopJoin;
    use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer, make_buffer_pair};
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

