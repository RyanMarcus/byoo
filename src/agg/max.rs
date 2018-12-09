use agg::Aggregate;
use data::Data;
use std::cmp::Ordering;

pub struct MaxAggregate {
    row_idx: usize,
    curr_max: Option<Data>
}

impl MaxAggregate {
    pub fn new(row_idx: usize) -> MaxAggregate {
        return MaxAggregate {
            row_idx,
            curr_max: None
        };
    }
}

impl Aggregate for MaxAggregate {
    fn consume(&mut self, row: &[Data]) {
        let nxt = &row[self.row_idx];
        let curr = self.curr_max.take();
        self.curr_max = match curr {
            None => Some(nxt.clone()),
            Some(d) => {
                match d.partial_cmp(nxt).unwrap() {
                    Ordering::Equal => Some(d),
                    Ordering::Greater => Some(d),
                    Ordering::Less => Some(nxt.clone())
                }
            }
        };
    }

    fn produce(&mut self) -> Data {
        return self.curr_max.take().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use agg::{Aggregate};
    use agg::max::MaxAggregate;
    use data::Data;
    
    #[test]
    fn simple_test() {
        let mut max_agg = MaxAggregate::new(0);

        let data = vec![
            vec![Data::Integer(5), Data::Integer(-100)],
            vec![Data::Integer(-2), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
        ];

        for row in data.iter() {
            max_agg.consume(row);
        }

        assert_eq!(max_agg.produce(), Data::Integer(10));
    }

    #[test]
    fn multi_test() {
        let mut max_agg = MaxAggregate::new(0);

        let data = vec![
            vec![Data::Integer(500), Data::Integer(-100)],
            vec![Data::Integer(-200), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
            vec![Data::Integer(-10), Data::Integer(100)],
            vec![Data::Integer(30), Data::Integer(-100)],
            vec![Data::Integer(-30), Data::Integer(-100)],
        ];

        
        max_agg.consume(&data[0]);
        max_agg.consume(&data[1]);
        max_agg.consume(&data[2]);
        assert_eq!(max_agg.produce(), Data::Integer(500));

        max_agg.consume(&data[3]);
        max_agg.consume(&data[4]);
        max_agg.consume(&data[5]);
        assert_eq!(max_agg.produce(), Data::Integer(30));

    }
}
