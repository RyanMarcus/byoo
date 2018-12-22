use agg;
use agg::Aggregate;

mod sorted_groupby;
pub use operator::groupby::sorted_groupby::SortedGroupBy;

mod hashed_groupby;
pub use operator::groupby::hashed_groupby::HashedGroupBy;

mod all_rows_groupby;
pub use operator::groupby::all_rows_groupby::AllRowsGroupBy;

fn json_to_aggs(json: &serde_json::Value) -> Vec<Box<Aggregate + Send>> {
        let aggs: Vec<Box<Aggregate + Send>> = json
            .as_array().unwrap().iter()
            .map(|ref agg| {
                let op = agg["op"].as_str().unwrap();
                let col_idx = agg["col"].as_i64().unwrap() as usize;
                
                return agg::new(op, col_idx);
            }).collect();

    return aggs;
}
