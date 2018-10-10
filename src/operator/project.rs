use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use serde_json::Value;

pub struct Project {
    input: OperatorReadBuffer,
    output: OperatorWriteBuffer,
    keep_cols: Vec<usize>
}

impl Project {
    pub fn new(input: OperatorReadBuffer, output: OperatorWriteBuffer,
               keep_cols: Vec<usize>) -> Project {
        return Project {
            input, output, keep_cols
        };
    }

    pub fn start(mut self) {
        iterate_buffer!(self.input, row, {
            let mut new_row = Vec::with_capacity(self.keep_cols.len());

            for &col in self.keep_cols.iter() {
                new_row.push(row[col].clone());
            }

            self.output.write(new_row);
        });
    }
}


#[cfg(test)]
mod tests {

    use operator_buffer::make_buffer_pair;
    use operator::{Project};
    use data::{DataType, Data};
    
    #[test]
    fn simple_test() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::TEXT]);
        let (mut r2, w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(5), Data::Text(String::from("hello"))]);
        w.write(vec![Data::Integer(6), Data::Text(String::from("hello1"))]);
        w.write(vec![Data::Integer(7), Data::Text(String::from("hello2"))]);
        drop(w);

        let p = Project::new(r, w2, vec![0]);
        p.start();
        let mut res = Vec::new();

        iterate_buffer!(r2, row, {
            assert_eq!(row.len(), 1);
            if let Data::Integer(i) = row[0] {
                res.push(i);
            } else {
                panic!("wrong datatype");
            }
        });

        assert_eq!(res, vec![5, 6, 7]);
    }
}
