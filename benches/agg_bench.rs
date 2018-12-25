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
#![feature(test)]

extern crate test;
use test::Bencher;

extern crate byoo;
use byoo::Data;

fn sorted_group_by_plan() -> Vec<Vec<Data>> {
    
    let json = String::from(r#"

{"op": "project",
 "options": { "cols": [0, 3, 4, 5] }, 
 "input": [
 {
     "op": "sorted group by",
     "options": {
         "col": 0,
         "aggregates": [
             {"op": "min", "col": 1},
             {"op": "max", "col": 1},
             {"op": "count", "col": 1}
         ]
     },

     "input": [
         {"op": "sort",
          "options": { "cols": [0] },
          "input": [
              { "op": "csv read",
                "options": {
                    "file": "res/inputs/agg_test.csv",
                    "types": ["INTEGER", "INTEGER", "REAL"]
                }
              }]
         }
     ]
 }]
}

"#);

    let root = byoo::compile(json);
    let (read_buf, _) = root.start_save();

    let data_vec = read_buf.into_vec();
    return data_vec;
}

fn hashed_group_by_plan() -> Vec<Vec<Data>> {
    
    let json = String::from(r#"
{"op": "project",
 "options": { "cols": [0, 3, 4, 5] }, 
 "input": [
     {
         "op": "hashed group by",
         "options": {
             "col": 0,
             "aggregates": [
                 {"op": "min", "col": 1},
                 {"op": "max", "col": 1},
                 {"op": "count", "col": 1}
             ]
         },

         "input": [
             { "op": "csv read",
               "options": {
                   "file": "res/inputs/agg_test.csv",
                   "types": ["INTEGER", "INTEGER", "REAL"]
               }
             }]
     }
 ]
}
"#);

    let root = byoo::compile(json);
    let (read_buf, _) = root.start_save();

    let data_vec = read_buf.into_vec();
    return data_vec;
}


#[bench]
fn sort_agg(b: &mut Bencher) {
    b.iter(|| {
        sorted_group_by_plan()
    });
}


#[bench]
fn hash_agg(b: &mut Bencher) {
    b.iter(|| {
        hashed_group_by_plan()
    });
}


