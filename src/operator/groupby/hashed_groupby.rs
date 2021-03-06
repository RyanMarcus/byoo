// < begin copyright > 
// Copyright Ryan Marcus 2018
// 
// This file is part of byoo.
// 
// byoo is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// byoo is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with byoo.  If not, see <http://www.gnu.org/licenses/>.
// 
// < end copyright > 
use agg::Aggregate;
use data::Data;
use hash_partition_store::ReadableHashPartitionStore;
use operator::groupby;
use operator::ConstructableOperator;
use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use serde_json;
use std::collections::HashMap;
use std::fs::File;

pub struct HashedGroupBy {
    child: OperatorReadBuffer,
    out: OperatorWriteBuffer,
    group_by_col_idx: usize,
    aggs: serde_json::Value,
}

type RowAndAggs = (Vec<Data>, Vec<Box<Aggregate + Send>>);
impl HashedGroupBy {
    fn new(
        child: OperatorReadBuffer,
        out: OperatorWriteBuffer,
        group_by_col_idx: usize,
        aggs: serde_json::Value,
    ) -> HashedGroupBy {
        return HashedGroupBy {
            child,
            out,
            group_by_col_idx,
            aggs,
        };
    }

    pub fn start(mut self) {
        let mut rhps = ReadableHashPartitionStore::new(4096, self.child, &[self.group_by_col_idx]);

        let aggs_config = self.aggs.take();

        loop {
            let nxt_buf = rhps.next_buf();
            if nxt_buf.is_none() {
                return;
            }

            // do the aggregation with a hashmap
            let mut m: HashMap<Data, RowAndAggs> = HashMap::new();

            let mut buf = nxt_buf.unwrap();
            iterate_buffer!(buf, row, {
                let aggs_for_key =
                    m.entry(row[self.group_by_col_idx].clone())
                        .or_insert_with(|| {
                            return (row.to_vec(), groupby::json_to_aggs(&aggs_config));
                        });

                for agg in aggs_for_key.1.iter_mut() {
                    agg.consume(row);
                }
            });

            // dump all the results from the hashmap
            for (_, (mut witness, mut aggs)) in m.into_iter() {
                witness.extend(aggs.into_iter().map(|mut agg| agg.produce()));
                self.out.write(witness);
            }
        }
    }
}

impl ConstructableOperator for HashedGroupBy {
    fn from_buffers(
        output: Option<OperatorWriteBuffer>,
        mut input: Vec<OperatorReadBuffer>,
        file: Option<File>,
        options: serde_json::Value,
    ) -> Self {
        assert!(file.is_none());
        let o = output.unwrap();

        assert_eq!(input.len(), 1);
        let child = input.remove(0);

        assert!(
            options["col"].is_i64(),
            "sorted group by missing column index"
        );

        assert!(
            options["aggregates"].is_array(),
            "sorted group by missing aggregates"
        );

        let group_by_idx = options["col"].as_i64().unwrap() as usize;

        return HashedGroupBy::new(child, o, group_by_idx, options["aggregates"].clone());
    }
}

#[cfg(test)]
mod tests {
    use data::{Data, DataType};
    use operator::groupby::HashedGroupBy;
    use operator_buffer::make_buffer_pair;

    #[test]
    fn one_agg_test() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER]);

        w.write(vec![Data::Integer(1), Data::Integer(1)]);
        w.write(vec![Data::Integer(1), Data::Integer(10)]);
        w.write(vec![Data::Integer(2), Data::Integer(-15)]);
        w.flush();
        drop(w);

        let (r2, w2) = make_buffer_pair(
            5,
            10,
            vec![DataType::INTEGER, DataType::INTEGER, DataType::INTEGER],
        );

        let aggs = json!({ "aggregates": [{"op": "count", "col": 1}] });
        let gb = HashedGroupBy::new(r, w2, 0, aggs["aggregates"].clone());
        gb.start();

        let mut results = r2.into_vec();
        results.sort_by(|a, b| a[0].as_i64().cmp(&b[0].as_i64()));
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            vec![Data::Integer(1), Data::Integer(1), Data::Integer(2)]
        );
        assert_eq!(
            results[1],
            vec![Data::Integer(2), Data::Integer(-15), Data::Integer(1)]
        );
    }

    #[test]
    fn two_agg_test() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::INTEGER]);

        w.write(vec![Data::Integer(1), Data::Integer(1)]);
        w.write(vec![Data::Integer(1), Data::Integer(10)]);
        w.write(vec![Data::Integer(2), Data::Integer(-15)]);
        w.flush();
        drop(w);

        let (r2, w2) = make_buffer_pair(
            5,
            10,
            vec![
                DataType::INTEGER,
                DataType::INTEGER,
                DataType::INTEGER,
                DataType::INTEGER,
            ],
        );

        let aggs = json!({ "aggregates": [{"op": "count", "col": 1},
                                          {"op": "sum", "col": 1}] });
        let gb = HashedGroupBy::new(r, w2, 0, aggs["aggregates"].clone());
        gb.start();

        let mut results = r2.into_vec();
        results.sort_by(|a, b| a[0].as_i64().cmp(&b[0].as_i64()));
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            vec![
                Data::Integer(1),
                Data::Integer(1),
                Data::Integer(2),
                Data::Integer(11)
            ]
        );
        assert_eq!(
            results[1],
            vec![
                Data::Integer(2),
                Data::Integer(-15),
                Data::Integer(1),
                Data::Integer(-15)
            ]
        );
    }

}
