use serde_


enum Predicate {
    And(Predicate, Predicate),
    Or(Predicate, Predicate),
    Lt(usize, i64),
    Gt(usize, i64),
    Eq(usize, i64),
    Contains(usize, String)
}

impl Predicate {
    fn from_json(
}
