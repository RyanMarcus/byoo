use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use operator::ConstructableOperator;
use operator::groupby;
use data::{Data};
use serde_json;
use std::fs::File;
use agg::Aggregate;

pub struct SortedGroupBy {
    child: OperatorReadBuffer,
    out: OperatorWriteBuffer,
    group_by_col_idx: usize,
    aggs: Vec<Box<Aggregate + Send>>
}


impl SortedGroupBy {
    fn new(child: OperatorReadBuffer, out: OperatorWriteBuffer,
               group_by_col_idx: usize, aggs: Vec<Box<Aggregate + Send>>)
               -> SortedGroupBy {
        return SortedGroupBy {
            child, out,  
            group_by_col_idx, aggs
        };
    }
    
    pub fn start(mut self) {
        let mut last: Option<Vec<Data>> = None;
        iterate_buffer!(self.child, row, {
            let curr = &row[self.group_by_col_idx];
            last = match last.take() {
                None => Some(row.to_vec()),
                Some(mut last_row) => {
                    if &last_row[self.group_by_col_idx] != curr {
                        // we have a new value! we need to emit a result.
                        for agg in self.aggs.iter_mut() {
                            last_row.push(agg.produce());
                        }

                        self.out.write(last_row);
                        Some(row.to_vec())
                    } else {
                        Some(last_row)
                    }
                }
            };

            for agg in self.aggs.iter_mut() {
                agg.consume(row);
            }
        });

        // unless we were empty, emit the last row
        if let Some(mut last_row) = last {
            for agg in self.aggs.iter_mut() {
                last_row.push(agg.produce());
            }
            self.out.write(last_row);
        }
            
    }
}

impl ConstructableOperator for SortedGroupBy {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    mut input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    options: serde_json::Value) -> Self {
        
        assert!(file.is_none());
        let o = output.unwrap();

        assert_eq!(input.len(), 1);
        let child = input.remove(0);

        assert!(options["col"].is_i64(),
                "sorted group by missing column index");

        assert!(options["aggregates"].is_array(),
                "sorted group by missing aggregates");

        let group_by_idx = options["col"].as_i64().unwrap()
            as usize;

        let aggs = groupby::json_to_aggs(&options["aggregates"]);
        
        return SortedGroupBy::new(child, o,
                                  group_by_idx, aggs);
    }
}

#[cfg(test)]
mod tests {
    use operator::groupby::SortedGroupBy;
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
        
        let gb = SortedGroupBy::new(r, w2, 0, vec![agg::new("count", 1)]);
        gb.start();

        let results = r2.into_vec();
        assert_eq!(results[0], vec![Data::Integer(1), Data::Integer(1), Data::Integer(2)]);
        assert_eq!(results[1], vec![Data::Integer(2), Data::Integer(-15), Data::Integer(1)]);
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
        
        let gb = SortedGroupBy::new(r, w2, 0, vec![agg::new("count", 1),
                                                   agg::new("sum", 1)]);
        gb.start();

        let results = r2.into_vec();
        assert_eq!(results[0], vec![Data::Integer(1), Data::Integer(1),
                                    Data::Integer(2), Data::Integer(11)]);
        assert_eq!(results[1], vec![Data::Integer(2), Data::Integer(-15),
                                    Data::Integer(1), Data::Integer(-15)]);
    }
    
}

