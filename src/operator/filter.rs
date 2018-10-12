use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use data::Data;
use operator::ConstructableOperator;

pub struct Filter {
    input: OperatorReadBuffer,
    output: OperatorWriteBuffer,
    predicate: fn(&[Data]) -> bool
}


impl Filter {
    pub fn new(input: OperatorReadBuffer, output: OperatorWriteBuffer,
               predicate: fn(&[Data]) -> bool) -> Filter {
        return Filter {
            input, output, predicate
        };
    }

    pub fn start(mut self) {
        iterate_buffer!(self.input, row, {
            if !(self.predicate)(row) {
                continue;
            }

            self.output.write(row.to_vec());
        });
        self.output.flush();
    }
}

impl ConstructableOperator for Filter {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    options: serde_json::Value) -> Self {
        let o = output.unwrap();
        let i = output.unwrap();
    }
}


#[cfg(test)]
mod tests {
    use operator::filter::Filter;
    use operator_buffer::make_buffer_pair;
    use data::{Data, DataType};

    #[test]
    fn filters_odds() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(-100)]);
        w.flush();
        drop(w);

        let (mut r2, w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        let filter = Filter::new(r, w2, |d| {
            match d[0] {
                Data::Integer(i) => i % 2 == 0,
                _ => { panic!("wrong datatype!"); }
            }
        });

        filter.start();

        let mut num_items = 0;
        iterate_buffer!(r2, idx, row, {
            num_items += 1;
            match idx {
                0 => { assert_eq!(row[0], Data::Integer(6)); }
                1 => { assert_eq!(row[0], Data::Integer(-100)); }
                _ => { panic!("Too many values!"); }
            }
        });

        assert_eq!(num_items, 2);

    }

    #[test]
    fn filters_many_odds() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        for i in 0..50 {
            w.write(vec![Data::Integer(i)]);
        }
        w.flush();
        drop(w);

        let (mut r2, w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        let filter = Filter::new(r, w2, |d| {
            match d[0] {
                Data::Integer(i) => i % 2 == 0,
                _ => { panic!("wrong datatype!"); }
            }
        });

        filter.start();
        
        let mut num_items = 0;
        iterate_buffer!(r2, row, {
            num_items += 1;
            if let Data::Integer(i) = row[0] {
                assert!(i % 2 == 0);
            } else {
                panic!("invalid data type!");
            }
        });

        assert_eq!(num_items, 25);

    }
}
