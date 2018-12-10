use data::{Data, DataType};

pub trait Aggregate {
    fn consume(&mut self, row: &[Data]);
    fn produce(&mut self) -> Data;
    fn out_type(&self, in_type: &DataType) -> DataType;
}

mod min;
mod max;
mod count;
mod sum;
mod average;

pub fn new(agg_type: &str, row_idx: usize) -> Box<Aggregate + Send> {
    return match agg_type {
        "min" => Box::new(min::MinAggregate::new(row_idx)),
        "max" => Box::new(max::MaxAggregate::new(row_idx)),
        "avg" => Box::new(average::AverageAggregate::new(row_idx)),
        "count" => Box::new(count::CountAggregate::new(row_idx)),
        "sum" => Box::new(sum::SumAggregate::new(row_idx)),
        _ => panic!("Got unknown aggregate type {}", agg_type)
    }
}

#[cfg(test)]
mod tests {

    use agg::new;
    use data::Data;
    
    #[test]
    fn create_new_test() {
        let mut aggs = vec![
            new("min", 0),
            new("max", 0),
            new("avg", 0),
            new("count", 0),
            new("sum", 0)
        ];

        let data = vec![
            vec![Data::Integer(500), Data::Integer(-100)],
            vec![Data::Integer(-200), Data::Integer(-100)],
            vec![Data::Integer(12), Data::Integer(-100)],
        ];


        for agg in aggs.iter_mut() {
            agg.consume(&data[0]);
            agg.consume(&data[1]);
            agg.consume(&data[2]);
        }

        
        assert_eq!(aggs[0].produce(), Data::Integer(-200));
        assert_eq!(aggs[1].produce(), Data::Integer(500));
        assert_eq!(aggs[2].produce(), Data::Real(312.0 / 3.0));
        assert_eq!(aggs[3].produce(), Data::Integer(3));
        assert_eq!(aggs[4].produce(), Data::Integer(312));

    }
}
