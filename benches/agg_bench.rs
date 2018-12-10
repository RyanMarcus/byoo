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


