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


#[cfg(test)]
mod tests {
    use byoo;
    use byoo::rows_to_string;

    #[test]
    fn loop_join_plan() {
        
        let json = String::from(r#"
{
    "op": "loop join",
    "options": {
        "predicate": {"op": "eq", "col": 0, "col2": 2}
    },

    "input": [
        { "op": "csv read",
          "options": {
              "file": "res/inputs/test2.csv",
              "types": ["INTEGER", "TEXT"]
          }
        },
        { "op": "csv read",
          "options": {
              "file": "res/inputs/test1.csv",
              "types": ["INTEGER", "INTEGER", "TEXT", "INTEGER", "REAL"]
          }
        }
    ]
}

"#);

        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.into_vec();
        assert_eq!(data_vec.len(), 3);
        let as_str = rows_to_string(&data_vec, true);

        let expected = String::from(r#"-9672  should appear once  -9672  6025  enBSVZhEGxgRozcGnAvtwIxbYdkGMobLASvvctfDyFXpV  -7930  0.3302230705775109 
8650  should appear twice  8650  -3586  trOHdfAWpVQCRqRrcAVOcbqNMdQLaxmwXvDysINgiZGFBrXdTmjIfplaYvUwbmDfTyuWFoNBfumxemVgNZdYfnmCi  -4296  0.6019379486611963 
8650  should appear twice  8650  -7903  vjLnOMQGHrKBxsEQGNpZQZZeVMIHXsMybZLOBBpfwOtnKedbBzintO  -5791  0.986346000243429 "#);
                

        assert_eq!(as_str, expected);

    }

    #[test]
    fn merge_join_plan() {
        
        let json = String::from(r#"
{
    "op": "merge join",
    "options": {
        "left_cols": [0], "right_cols": [0]
    },

    "input": [
        {"op": "sort",
         "options": {
             "cols": [0]
         },
         "input": [
             { "op": "csv read",
               "options": {
                   "file": "res/inputs/test2.csv",
                   "types": ["INTEGER", "TEXT"]
               }
             }]
        },
        { "op": "sort",
          "options": {
              "cols": [0]
          },
          "input": [
              { "op": "csv read",
                "options": {
                    "file": "res/inputs/test1.csv",
                    "types": ["INTEGER", "INTEGER", "TEXT", "INTEGER", "REAL"]
                }
              }]
        }
    ]
}

"#);

        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.into_vec();
        assert_eq!(data_vec.len(), 3);
        let as_str = rows_to_string(&data_vec, true);

        let expected = String::from(r#"-9672  should appear once  -9672  6025  enBSVZhEGxgRozcGnAvtwIxbYdkGMobLASvvctfDyFXpV  -7930  0.3302230705775109 
8650  should appear twice  8650  -3586  trOHdfAWpVQCRqRrcAVOcbqNMdQLaxmwXvDysINgiZGFBrXdTmjIfplaYvUwbmDfTyuWFoNBfumxemVgNZdYfnmCi  -4296  0.6019379486611963 
8650  should appear twice  8650  -7903  vjLnOMQGHrKBxsEQGNpZQZZeVMIHXsMybZLOBBpfwOtnKedbBzintO  -5791  0.986346000243429 "#);
                

        assert_eq!(as_str, expected);

    }

    #[test]
    fn hash_join_plan() {
        
        let json = String::from(r#"
{
    "op": "hash join",
    "options": {
        "left_cols": [0], "right_cols": [0]
    },

    "input": [
        { "op": "csv read",
          "options": {
              "file": "res/inputs/test2.csv",
              "types": ["INTEGER", "TEXT"]
          }
        },
        { "op": "csv read",
          "options": {
              "file": "res/inputs/test1.csv",
              "types": ["INTEGER", "INTEGER", "TEXT", "INTEGER", "REAL"]
          }
        }
    ]
}

"#);

        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.into_vec();
        assert_eq!(data_vec.len(), 3);
        let as_str = rows_to_string(&data_vec, true);

        let expected = String::from(r#"-9672  should appear once  -9672  6025  enBSVZhEGxgRozcGnAvtwIxbYdkGMobLASvvctfDyFXpV  -7930  0.3302230705775109 
8650  should appear twice  8650  -3586  trOHdfAWpVQCRqRrcAVOcbqNMdQLaxmwXvDysINgiZGFBrXdTmjIfplaYvUwbmDfTyuWFoNBfumxemVgNZdYfnmCi  -4296  0.6019379486611963 
8650  should appear twice  8650  -7903  vjLnOMQGHrKBxsEQGNpZQZZeVMIHXsMybZLOBBpfwOtnKedbBzintO  -5791  0.986346000243429 "#);
                

        assert_eq!(as_str, expected);

    }
}
