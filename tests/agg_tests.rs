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
extern crate byoo;

#[macro_use]
extern crate approx;

#[cfg(test)]
mod tests {
    use byoo;
    use byoo::rows_to_string;

    #[test]
    fn sorted_group_by_plan() {
        
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
        assert_eq!(data_vec.len(), 5);
        assert_eq!(data_vec[0].len(), 4);
            
        let as_str = rows_to_string(&data_vec, true);
        assert_eq!(as_str, r#"0  -10000  9999  10144 
1  -9999  9998  9976 
2  -9998  9999  10136 
3  -9998  9996  9921 
4  -9998  9999  9823 "#);
    }

    #[test]
    fn sorted_group_by_plan_numeric() {
        
        let json = String::from(r#"

{"op": "project",
 "options": { "cols": [0, 3, 4] }, 
 "input": [
 {
     "op": "sorted group by",
     "options": {
         "col": 0,
         "aggregates": [
             {"op": "sum", "col": 2},
             {"op": "avg", "col": 2}
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

        let mut data_vec = read_buf.into_vec();
        println!("{:?}", data_vec);
        data_vec.sort_by(|a, b| a[0].as_i64().cmp(&b[0].as_i64()));
        
        assert_eq!(data_vec.len(), 5);
        assert_eq!(data_vec[0].len(), 3);

        assert_eq!(data_vec[0][0].as_i64(), 0);
        assert_abs_diff_eq!(data_vec[0][1].as_f64(), 5056.46, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[0][2].as_f64(), 0.49, epsilon=0.01);

        assert_eq!(data_vec[1][0].as_i64(), 1);
        assert_abs_diff_eq!(data_vec[1][1].as_f64(), 5019.75, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[1][2].as_f64(), 0.50, epsilon=0.01);

        assert_eq!(data_vec[2][0].as_i64(), 2);
        assert_abs_diff_eq!(data_vec[2][1].as_f64(), 5071.37, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[2][2].as_f64(), 0.50, epsilon=0.01);

        assert_eq!(data_vec[3][0].as_i64(), 3);
        assert_abs_diff_eq!(data_vec[3][1].as_f64(), 5002.46, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[3][2].as_f64(), 0.50, epsilon=0.01);

        assert_eq!(data_vec[4][0].as_i64(), 4);
        assert_abs_diff_eq!(data_vec[4][1].as_f64(), 4931.66, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[4][2].as_f64(), 0.50, epsilon=0.01);
    }

    #[test]
    fn hashed_group_by_plan() {
        
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
        assert_eq!(data_vec.len(), 5);
        assert_eq!(data_vec[0].len(), 4);
            
        let as_str = rows_to_string(&data_vec, true);
        assert_eq!(as_str, r#"0  -10000  9999  10144 
1  -9999  9998  9976 
2  -9998  9999  10136 
3  -9998  9996  9921 
4  -9998  9999  9823 "#);
    }

    #[test]
    fn hashed_group_by_plan_numeric() {
        
        let json = String::from(r#"
{"op": "project",
 "options": { "cols": [0, 3, 4] }, 
 "input": [
     {
         "op": "hashed group by",
         "options": {
             "col": 0,
             "aggregates": [
                 {"op": "sum", "col": 2},
                 {"op": "avg", "col": 2}
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

        let mut data_vec = read_buf.into_vec();
        data_vec.sort_by(|a, b| a[0].as_i64().cmp(&b[0].as_i64()));
        
        assert_eq!(data_vec.len(), 5);
        assert_eq!(data_vec[0].len(), 3);

        assert_eq!(data_vec[0][0].as_i64(), 0);
        assert_abs_diff_eq!(data_vec[0][1].as_f64(), 5056.46, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[0][2].as_f64(), 0.49, epsilon=0.01);

        assert_eq!(data_vec[1][0].as_i64(), 1);
        assert_abs_diff_eq!(data_vec[1][1].as_f64(), 5019.75, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[1][2].as_f64(), 0.50, epsilon=0.01);

        assert_eq!(data_vec[2][0].as_i64(), 2);
        assert_abs_diff_eq!(data_vec[2][1].as_f64(), 5071.37, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[2][2].as_f64(), 0.50, epsilon=0.01);

        assert_eq!(data_vec[3][0].as_i64(), 3);
        assert_abs_diff_eq!(data_vec[3][1].as_f64(), 5002.46, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[3][2].as_f64(), 0.50, epsilon=0.01);

        assert_eq!(data_vec[4][0].as_i64(), 4);
        assert_abs_diff_eq!(data_vec[4][1].as_f64(), 4931.66, epsilon=0.01);
        assert_abs_diff_eq!(data_vec[4][2].as_f64(), 0.50, epsilon=0.01);
    }
}
