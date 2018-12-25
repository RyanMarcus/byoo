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
