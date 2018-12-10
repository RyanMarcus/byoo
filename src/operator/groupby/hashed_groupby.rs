use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use operator::ConstructableOperator;
use operator::groupby;
use hash_partition_store::ReadableHashPartitionStore;
use data::{Data};
use std::collections::HashMap;
use serde_json;
use std::fs::File;
use agg;
use agg::Aggregate;

pub struct HashedGroupBy {
    child: OperatorReadBuffer,
    out: OperatorWriteBuffer,
    group_by_col_idx: usize,
    aggs: serde_json::Value
}


impl HashedGroupBy {
    fn new(child: OperatorReadBuffer, out: OperatorWriteBuffer,
           group_by_col_idx: usize, aggs: serde_json::Value)
               -> HashedGroupBy {
        return HashedGroupBy {
            child, out,  
            group_by_col_idx, aggs
        };
    }

    pub fn start(mut self) {
        let mut rhps = ReadableHashPartitionStore::new(
            4096, self.child, vec![self.group_by_col_idx]);

        let aggs_config = self.aggs.take();
        
        loop {
            let nxt_buf = rhps.next_buf();
            if nxt_buf.is_none() {
                return;
            }

            // do the aggregation with a hashmap
            let mut m: HashMap<Data, (Vec<Data>, Vec<Box<Aggregate + Send>>)> = HashMap::new();

            let mut buf = nxt_buf.unwrap();
            iterate_buffer!(buf, row, {
                let aggs_for_key = m.entry(row[self.group_by_col_idx].clone())
                    .or_insert_with(|| {
                        return (row.to_vec(),
                                groupby::json_to_aggs(&aggs_config));
                    });

                for agg in aggs_for_key.1.iter_mut() {
                    agg.consume(row);
                }
            });

            // dump all the results from the hashmap
            for (_, (mut witness, mut aggs)) in m.into_iter() {
                witness.extend(
                    aggs.into_iter()
                        .map(|mut agg| agg.produce()));
                self.out.write(witness);
            }
            
        }
        
            
    }
}

impl ConstructableOperator for HashedGroupBy {
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
        
        return HashedGroupBy::new(child, o,
                                  group_by_idx, options["aggregates"].clone());
    }
}

#[cfg(test)]
mod tests {
    use operator::groupby::HashedGroupBy;
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

        let aggs = json!({ "aggregates": [{"op": "count", "col": 1}] });
        let gb = HashedGroupBy::new(r, w2, 0, aggs["aggregates"].clone());
        gb.start();

        let mut results = r2.to_vec();
        results.sort_by(|a, b| a[0].as_i64().cmp(&b[0].as_i64()));
        
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

        let aggs = json!({ "aggregates": [{"op": "count", "col": 1},
                                          {"op": "sum", "col": 1}] });
        let gb = HashedGroupBy::new(r, w2, 0, aggs["aggregates"].clone());
        gb.start();

        let mut results = r2.to_vec();
        results.sort_by(|a, b| a[0].as_i64().cmp(&b[0].as_i64()));

        assert_eq!(results[0], vec![Data::Integer(1), Data::Integer(1),
                                    Data::Integer(2), Data::Integer(11)]);
        assert_eq!(results[1], vec![Data::Integer(2), Data::Integer(-15),
                                    Data::Integer(1), Data::Integer(-15)]);
    }
    
}

