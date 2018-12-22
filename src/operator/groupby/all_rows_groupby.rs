use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use operator::ConstructableOperator;
use operator::groupby;
use serde_json;
use std::fs::File;
use agg::Aggregate;

pub struct AllRowsGroupBy {
    child: OperatorReadBuffer,
    out: OperatorWriteBuffer,
    aggs: Vec<Box<Aggregate + Send>>
}


impl AllRowsGroupBy {
    fn new(child: OperatorReadBuffer, out: OperatorWriteBuffer,
           aggs: Vec<Box<Aggregate + Send>>)
               -> AllRowsGroupBy {
        return AllRowsGroupBy {
            child, out,  
            aggs
        };
    }
    
    pub fn start(mut self) {
        let mut any_row = None;
        iterate_buffer!(self.child, row, {
            if let None = any_row {
                any_row = Some(row.to_vec());
            }
            
            for agg in self.aggs.iter_mut() {
                agg.consume(row);
            }
        });

        if let Some(mut row) = any_row {
            for agg in self.aggs.iter_mut() {
                row.push(agg.produce());
            }
            self.out.write(row);
        }
            
    }
}

impl ConstructableOperator for AllRowsGroupBy {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    mut input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    options: serde_json::Value) -> Self {
        
        assert!(file.is_none());
        let o = output.unwrap();

        assert_eq!(input.len(), 1);
        let child = input.remove(0);

        assert!(options["aggregates"].is_array(),
                "all rows group by missing aggregates");

        let aggs = groupby::json_to_aggs(&options["aggregates"]);
        
        return AllRowsGroupBy::new(child, o,
                                   aggs);
    }
}

#[cfg(test)]
mod tests {
    use operator::groupby::AllRowsGroupBy;
    use agg;
    use operator_buffer::{make_buffer_pair};
    use data::{Data, DataType};

    #[test]
    fn one_agg_test() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER]);

        w.write(vec![Data::Integer(1), Data::Integer(1)]);
        w.write(vec![Data::Integer(1), Data::Integer(10)]);
        w.write(vec![Data::Integer(2), Data::Integer(-15)]);
        w.flush();
        drop(w);

        let (r2, w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER,
                                                        DataType::INTEGER]);
        
        let gb = AllRowsGroupBy::new(r, w2, vec![agg::new("count", 1)]);
        gb.start();

        let results = r2.into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][2], Data::Integer(3));
    }

    #[test]
    fn two_agg_test() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER]);

        w.write(vec![Data::Integer(1), Data::Integer(1)]);
        w.write(vec![Data::Integer(1), Data::Integer(10)]);
        w.write(vec![Data::Integer(2), Data::Integer(-15)]);
        w.flush();
        drop(w);

        let (r2, w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER,
                                                    DataType::INTEGER, DataType::INTEGER]);
        
        let gb = AllRowsGroupBy::new(r, w2, vec![agg::new("count", 1),
                                                 agg::new("sum", 1)]);
        gb.start();

        let results = r2.into_vec();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][2], Data::Integer(3));
        assert_eq!(results[0][3], Data::Integer(-4));
    }
    
}

