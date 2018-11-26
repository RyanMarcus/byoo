use agg::Aggregate;
use data::Data;
use std::cmp::Ordering;

pub struct MinAggregate {
    rowIdx: usize,
    curr_min: Option<Data>
}

impl Aggregate for MinAggregate {
    fn new(rowIdx: usize) -> MinAggregate {
        return MinAggregate {
            rowIdx,
            curr_min: None
        };
    }

    fn consume(&mut self, row: &[Data]) {
        let nxt = &row[self.rowIdx];
        let curr = self.curr_min.take();
        self.curr_min = match curr {
            None => Some(nxt.clone()),
            Some(d) => {
                match d.partial_cmp(nxt).unwrap() {
                    Ordering::Equal => Some(d),
                    Ordering::Less => Some(d),
                    Ordering::Greater => Some(nxt.clone())
                }
            }
        };
    }

    fn produce(&mut self) -> Data {
        return self.curr_min.take().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use agg::{Aggregate, MinAggregate};
    use data::Data;
    
    #[test]
    fn simple_test() {
        let mut min_agg = MinAggregate::new(0);

        let data = vec![
            vec![Data::Integer(5), Data::Integer(-100)],
            vec![Data::Integer(-2), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
        ];

        for row in data.iter() {
            min_agg.consume(row);
        }

        assert_eq!(min_agg.produce(), Data::Integer(-2));
    }

    #[test]
    fn multi_test() {
        let mut min_agg = MinAggregate::new(0);

        let data = vec![
            vec![Data::Integer(5), Data::Integer(-100)],
            vec![Data::Integer(-200), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
            vec![Data::Integer(-10), Data::Integer(-100)],
            vec![Data::Integer(-30), Data::Integer(-100)],
            vec![Data::Integer(30), Data::Integer(-100)],
        ];

        
        min_agg.consume(&data[0]);
        min_agg.consume(&data[1]);
        min_agg.consume(&data[2]);
        assert_eq!(min_agg.produce(), Data::Integer(-200));

        min_agg.consume(&data[3]);
        min_agg.consume(&data[4]);
        min_agg.consume(&data[5]);
        assert_eq!(min_agg.produce(), Data::Integer(-30));

    }
}
