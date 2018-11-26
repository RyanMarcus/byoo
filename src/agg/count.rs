use agg::Aggregate;
use data::Data;
use std::cmp::Ordering;

pub struct CountAggregate {
    curr_count: usize
}

impl Aggregate for CountAggregate {
    fn new(_rowIdx: usize) -> CountAggregate {
        return CountAggregate {
            curr_count: 0
        };
    }

    fn consume(&mut self, row: &[Data]) {
        self.curr_count += 1;
    }

    fn produce(&mut self) -> Data {
        let to_r = self.curr_count;
        self.curr_count = 0;
        return Data::Integer(to_r as i64);
    }
}

#[cfg(test)]
mod tests {
    use agg::{Aggregate, CountAggregate};
    use data::Data;
    
    #[test]
    fn simple_test() {
        let mut count_agg = CountAggregate::new(0);

        let data = vec![
            vec![Data::Integer(5), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
        ];

        for row in data.iter() {
            count_agg.consume(row);
        }

        assert_eq!(count_agg.produce(), Data::Integer(2));
    }

    #[test]
    fn multi_test() {
        let mut count_agg = CountAggregate::new(0);

        let data = vec![
            vec![Data::Integer(500), Data::Integer(-100)],
            vec![Data::Integer(-200), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
            vec![Data::Integer(-10), Data::Integer(100)],
            vec![Data::Integer(30), Data::Integer(-100)],
            vec![Data::Integer(-30), Data::Integer(-100)],
        ];

        
        count_agg.consume(&data[0]);
        count_agg.consume(&data[1]);
        count_agg.consume(&data[2]);
        assert_eq!(count_agg.produce(), Data::Integer(3));

        count_agg.consume(&data[3]);
        count_agg.consume(&data[4]);
        count_agg.consume(&data[5]);
        assert_eq!(count_agg.produce(), Data::Integer(3));

    }
}
