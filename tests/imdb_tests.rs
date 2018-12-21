extern crate byoo;

#[cfg(not(debug_assertions))]
#[cfg(test)]
mod tests {
    use byoo;

    #[test]
    fn imdb_2010_keywords() {
        
        let json = String::from(r#"
{
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
}

"#);


        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.into_vec();
        assert_eq!(data_vec.len(), 176344);

    }
}
