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
}
