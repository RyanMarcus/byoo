extern crate byoo;

#[cfg(test)]
mod tests {
    use byoo;
    
    #[test]
    fn simple_plan() {
        let json = String::from(r#"
{
    "op": "columnar out",
    "options": {
        "file": "res/outputs/test.dat"
    },
    "input": [
        { "op": "project",
          "options": {
              "cols": [0, 1, 3]
          },

          "input": [
              { "op": "csv read",
                "options": {
                    "file": "res/inputs/test1.csv",
                    "types": ["INTEGER", "INTEGER", "TEXT", "INTEGER", "REAL"]
                }
              }
          ]
        }
    ]
}"#);

        let root = byoo::compile(json);
        root.start().join();
    }
}
