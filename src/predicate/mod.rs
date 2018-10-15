use serde_json;
use std::boxed::Box;
use data::{Data};
use either::*;


enum Predicate {
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    Lt(usize, Either<i64, f64>),
    Gt(usize, Either<i64, f64>),
    Eq(usize, Either<i64, f64>),
    Contains(usize, String)
}

fn either_from_json(v: &serde_json::Value) -> Either<i64, f64> {
    if v.is_f64() {
        return Right(v.as_f64().unwrap());
    } else if v.is_i64() {
        return Left(v.as_i64().unwrap());
    }

    panic!("Unknown type in numeric predicate operator");
}

macro_rules! apply_op {
    ($val: expr, $data: expr, $op: tt) => {
        match $val {
            Left(int_val) => {
                if let Data::Integer(i) = $data {
                    i $op int_val.clone()
                } else {
                    panic!("Comparing non-integer column to integer value in predicate");
                }
            },
            
            Right(float_val) => {
                if let Data::Real(f) = $data {
                    f $op float_val.clone()
                } else {
                    panic!("Comparing non-float column to float value in predicate");
                }
            }
        }   
    }
}

impl Predicate {
    fn from_json(tree: &serde_json::Value) -> Predicate {
        match tree["op"].as_str().unwrap() {
            "and" => {
                let mut children = tree["children"].as_array().unwrap();
                assert_eq!(children.len(), 2);

                let c1_p = Predicate::from_json(&children[0]);
                let c2_p = Predicate::from_json(&children[1]);

                return Predicate::And(Box::new(c1_p),
                                      Box::new(c2_p));
            },

            "or" => {
                let mut children = tree["children"].as_array().unwrap();
                assert_eq!(children.len(), 2);

                let c1_p = Predicate::from_json(&children[0]);
                let c2_p = Predicate::from_json(&children[1]);

                return Predicate::Or(Box::new(c1_p),
                                      Box::new(c2_p));
            }

            "not" => {
                let mut children = tree["children"].as_array().unwrap();
                assert_eq!(children.len(), 1);

                let c1_p = Predicate::from_json(&children[0]);

                return Predicate::Not(Box::new(c1_p));
            },

            "lt" => {
                let col_idx = tree["col"].as_i64().unwrap() as usize;
                let cmp_val = &tree["val"];

                return Predicate::Lt(col_idx, either_from_json(cmp_val));
            },

            "gt" => {
                let col_idx = tree["col"].as_i64().unwrap() as usize;
                let cmp_val = &tree["val"];
                return Predicate::Gt(col_idx, either_from_json(cmp_val));
            },

            "eq" => {
                let col_idx = tree["col"].as_i64().unwrap() as usize;
                let cmp_val = &tree["val"];
                return Predicate::Eq(col_idx, either_from_json(cmp_val));
            },

            "contains" => {
                let col_idx = tree["col"].as_i64().unwrap() as usize;
                let str_val = tree["val"].as_str().unwrap();
                return Predicate::Contains(col_idx, String::from(str_val));
            },

            _ => panic!("unknown op in predicate")
        };
    }

    pub fn eval(&self, data: &[Data]) -> bool {
        match &self {
            Predicate::And(p1, p2) => {
                return p1.eval(data) && p2.eval(data);
            },

            Predicate::Or(p1, p2) => {
                return p1.eval(data) || p2.eval(data);
            },

            Predicate::Not(p1) => {
                return !p1.eval(data);
            },

            Predicate::Lt(col_idx, val) => {
                return apply_op!(val, data[col_idx.clone()], <);
            },

            Predicate::Gt(col_idx, val) => {
                return apply_op!(val, data[col_idx.clone()], >);
            },

            Predicate::Eq(col_idx, val) => {
                return apply_op!(val, data[col_idx.clone()], ==);
            },

            Predicate::Contains(col_idx, string_val) => {
                if let Data::Text(ref s) = data[col_idx.clone()] {
                    return s.contains(string_val);
                }

                panic!("contains requires a string column");
            }
            
        }
    }
}

#[cfg(test)]
mod tests {

    use predicate::Predicate;
    use serde_json;
    use data::Data;
    
    #[test]
    fn simple_test() {
        let v: serde_json::Value = serde_json::from_str(r#"
{ "op": "and",
  "children": [
    { "op": "lt", "col": 0, "val": 4 },
    { "op": "gt", "col": 1, "val": 5.0 }
  ]
} 
"#).unwrap();

        let p = Predicate::from_json(&v);

        let r1 = vec![Data::Integer(3), Data::Real(8.0)];
        let r2 = vec![Data::Integer(5), Data::Real(8.0)];
        let r3 = vec![Data::Integer(3), Data::Real(2.0)];

        assert!(p.eval(&r1));
        assert!(!p.eval(&r2));
        assert!(!p.eval(&r3));
    }

    #[test]
    fn simple_test_str() {
        let v: serde_json::Value = serde_json::from_str(r#"
{ "op": "or",
  "children": [
    { "op": "contains", "col": 0, "val": "test" },
    { "op": "gt", "col": 1, "val": 5.0 }
  ]
} 
"#).unwrap();

        let p = Predicate::from_json(&v);

        let r1 = vec![Data::Text(String::from("this is a")), Data::Real(8.0)];
        let r2 = vec![Data::Text(String::from("hello")), Data::Real(8.0)];
        let r3 = vec![Data::Text(String::from("world test")), Data::Real(2.0)];
        let r4 = vec![Data::Text(String::from("world")), Data::Real(2.0)];

        assert!(p.eval(&r1));
        assert!(p.eval(&r2));
        assert!(p.eval(&r3));
        assert!(!p.eval(&r4));
    }
}
