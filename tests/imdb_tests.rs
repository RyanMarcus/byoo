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

#[cfg(not(debug_assertions))]
#[cfg(test)]
mod tests {
    use byoo;
    use byoo::Data;

    #[test]
    fn imdb_2010_keywords_hash() {
        
        let json = String::from(r#"
{ "op": "all rows group by",
  "options": { "aggregates": [{"op": "count", "col": 0}] },
  "input": [{
      "op": "hash join",
      "options": {
          "left_cols": [0], "right_cols": [0]
      },
      
      "input": [
          { "op": "filter",
            "options": {
                "predicate": { "op": "eq", "col": 1, "val": 2010 }
            },
            "input": [
                { "op": "csv read",
                  "options": {
                      "file": "res/inputs/imdb/imdb_title.csv",
                      "types": ["INTEGER", "INTEGER", "INTEGER"]
                  }
                }]
          },
          { "op": "project",
            "options": { "cols": [1, 2] },
            "input": [
                { "op": "csv read",
                  "options": {
                      "file": "res/inputs/imdb/imdb_movie_keyword.csv",
                      "types": ["INTEGER", "INTEGER", "INTEGER"]
                  }
                }
            ]
          }
      ]
  }]
}


"#);


        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.into_vec();
        assert_eq!(data_vec.len(), 1);
        assert_eq!(data_vec[0].last().unwrap(), &Data::Integer(176344));

    }

    #[test]
    fn imdb_2010_keywords_merge() {
        
        let json = String::from(r#"
{ "op": "all rows group by",
  "options": { "aggregates": [{"op": "count", "col": 0}] },
  "input": [{
      "op": "merge join",
      "options": {
          "left_cols": [0], "right_cols": [0]
      },

      "input": [
          { "op": "sort",
            "options": { "cols": [0] },
            "input": [
                { "op": "filter",
                  "options": {
                      "predicate": { "op": "eq", "col": 1, "val": 2010 }
                  },
                  "input": [
                      { "op": "csv read",
                        "options": {
                            "file": "res/inputs/imdb/imdb_title.csv",
                            "types": ["INTEGER", "INTEGER", "INTEGER"]
                        }
                      }]
                }
            ]
          },
          { "op": "sort",
            "options": { "cols": [0] },
            "input": [
                { "op": "project",
                  "options": { "cols": [1, 2] },
                  "input": [
                      { "op": "csv read",
                        "options": {
                            "file": "res/inputs/imdb/imdb_movie_keyword.csv",
                            "types": ["INTEGER", "INTEGER", "INTEGER"]
                        }
                      }
                  ]
                }
            ]
          }
      ]
  }]
}
"#);


        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.into_vec();
        assert_eq!(data_vec.len(), 1);
        assert_eq!(data_vec[0].last().unwrap(), &Data::Integer(176344));


    }

    #[test]
    fn imdb_2010_keywords_merge_columnar() {
        
        let json = String::from(r#"
{ "op": "all rows group by",
  "options": { "aggregates": [{"op": "count", "col": 0}] },
  "input": [{
      "op": "merge join",
      "options": {
          "left_cols": [0], "right_cols": [0]
      },

      "input": [
          { "op": "filter",
            "options": {
                "predicate": { "op": "eq", "col": 1, "val": 2010 }
            },
            "input": [
                { "op": "union",
                  "input": [
                      { "op": "columnar read",
                        "options": {
                            "file": "res/inputs/imdb/imdb_title.byoo",
                            "type": "INTEGER",
                            "col": 0
                        }
                      },
                      { "op": "columnar read",
                        "options": {
                            "file": "res/inputs/imdb/imdb_title.byoo",
                            "type": "INTEGER",
                            "col": 1
                        }
                      }
                  ]
                }
            ]
          },
          { "op": "union",
            "input": [
                { "op": "columnar read",
                  "options": {
                      "file": "res/inputs/imdb/imdb_movie_keyword.byoo",
                      "type": "INTEGER",
                      "col": 1
                  }
                },
                { "op": "columnar read",
                  "options": {
                      "file": "res/inputs/imdb/imdb_movie_keyword.byoo",
                      "type": "INTEGER",
                      "col": 2
                  }
                }
            ]
          }
      ]
  }]
}
"#);


        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.into_vec();
        assert_eq!(data_vec.len(), 1);
        assert_eq!(data_vec[0].last().unwrap(), &Data::Integer(176344));


    }
}
