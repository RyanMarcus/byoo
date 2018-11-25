extern crate byoo;


#[cfg(test)]
mod tests {
    use byoo;
    use byoo::Data;

    #[test]
    fn simple_plan() {
        
        let json = String::from(r#"
{
    "op": "project",
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
"#);

        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.to_vec();

        assert_eq!(data_vec.len(), 1000);

        // check the first and last rows
        let r_f = vec![Data::Integer(-7119), Data::Integer(8430), Data::Integer(8771)];
        let r_l = vec![Data::Integer(-4240), Data::Integer(2604), Data::Integer(-9236)];

        assert_eq!(data_vec[0], r_f);
        assert_eq!(data_vec[data_vec.len()-1], r_l);
    }
}
