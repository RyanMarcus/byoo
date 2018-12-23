#![allow(clippy::float_cmp)]

use serde_json;
use std::boxed::Box;
use data::{Data};
use either::*;


pub enum Predicate {
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    Lt(usize, Either<i64, f64>),
    Gt(usize, Either<i64, f64>),
    Eq(usize, Either<i64, f64>),
    LtCol(usize, usize),
    GtCol(usize, usize),
    EqCol(usize, usize),
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
                    i $op int_val
                } else {
                    panic!("Comparing non-integer column to integer value in predicate");
                }
            },
            
            Right(float_val) => {
                if let Data::Real(f) = $data {
                    f $op float_val
                } else {
                    panic!("Comparing non-float column to float value in predicate");
                }
            }
        }   
    }
}

macro_rules! overflow_access {
    ($data1: expr, $data2: expr, $idx: expr) => {
        if ($data1).len() > $idx {
            &($data1)[$idx]
        } else {
            &($data2)[$idx - ($data1).len()]
        }
    }
}

impl Predicate {
    pub fn from_json(tree: &serde_json::Value) -> Predicate {
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

                if let Some(v) = tree.get("val") {
                    return Predicate::Lt(col_idx, either_from_json(v));
                }

                if let Some(v) = tree.get("col2").map(|v| v.as_i64()) {
                    return Predicate::LtCol(col_idx, v.unwrap() as usize);
                }

                panic!("LT operator must have either a val or col2 attribute");
            },

            "gt" => {
                let col_idx = tree["col"].as_i64().unwrap() as usize;

                if let Some(v) = tree.get("val") {
                    return Predicate::Gt(col_idx, either_from_json(v));
                }

                if let Some(v) = tree.get("col2").map(|v| v.as_i64()) {
                    return Predicate::GtCol(col_idx, v.unwrap() as usize);
                }

                panic!("GT operator must have either a val or col2 attribute");
            },

            "eq" => {
                let col_idx = tree["col"].as_i64().unwrap() as usize;

                if let Some(v) = tree.get("val") {
                    return Predicate::Eq(col_idx, either_from_json(v));
                }

                if let Some(v) = tree.get("col2").map(|v| v.as_i64()) {
                    return Predicate::EqCol(col_idx, v.unwrap() as usize);
                }

                panic!("EQ operator must have either a val or col2 attribute");
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
        return self.eval_with_2(data, &[]);
    }

    pub fn eval_with_accessor<'a, F>(&self, accessor_func: &F) -> bool
    where F: Fn(usize) -> &'a Data {
        match &self {
            Predicate::And(p1, p2) => {
                return p1.eval_with_accessor(accessor_func) && p2.eval_with_accessor(accessor_func);
            }

            Predicate::Or(p1, p2) => {
                return p1.eval_with_accessor(accessor_func) || p2.eval_with_accessor(accessor_func);
            },

            Predicate::Not(p1) => {
                return !p1.eval_with_accessor(accessor_func);
            },

            Predicate::Lt(col_idx, val) => {
                return apply_op!(val, accessor_func(*col_idx), <);
            },

            Predicate::LtCol(col_idx, col2_idx) => {
                return accessor_func(*col_idx) < accessor_func(*col2_idx);
            },
            
            Predicate::Gt(col_idx, val) => {
                return apply_op!(val, accessor_func(*col_idx), >);
            },
            
            Predicate::GtCol(col_idx, col2_idx) => {
                return accessor_func(*col_idx) > accessor_func(*col2_idx);
            },

            Predicate::Eq(col_idx, val) => {
                return apply_op!(val, accessor_func(*col_idx), ==);
            },

            Predicate::EqCol(col_idx, col2_idx) => {
                let v1 = accessor_func(*col_idx);
                let v2 = accessor_func(*col2_idx);
                return v1 == v2;
            },

            Predicate::Contains(col_idx, string_val) => {
                let d = accessor_func(*col_idx);
                if let Data::Text(ref s) = d {
                    return s.contains(string_val);
                }

                panic!("contains requires a string column");
            }
            
        }
    }
    
    pub fn eval_with_2(&self, data: &[Data], data2: &[Data]) -> bool {
        let f = |idx: usize| { overflow_access!(data, data2, idx) };
        return self.eval_with_accessor(&f);
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
    fn simple_test_cols() {
        let v: serde_json::Value = serde_json::from_str(r#"
{ "op": "eq", "col": 0, "col2": 1 }
"#).unwrap();

        let p = Predicate::from_json(&v);

        let r1 = vec![Data::Integer(3), Data::Integer(5)];
        let r2 = vec![Data::Integer(5), Data::Integer(6)];
        let r3 = vec![Data::Integer(3), Data::Integer(3)];

        assert!(!p.eval(&r1));
        assert!(!p.eval(&r2));
        assert!(p.eval(&r3));
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
