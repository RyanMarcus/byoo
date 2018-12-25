#![feature(test)]

extern crate test;
use test::Bencher;

extern crate byoo;
use byoo::Data;


fn imdb_2010_keywords_merge_columnar() -> Vec<Vec<Data>>{
    
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
    return data_vec;
}

#[bench]
fn merge_join(b: &mut Bencher) {
    b.iter(|| {
        imdb_2010_keywords_merge_columnar()
    });
}

