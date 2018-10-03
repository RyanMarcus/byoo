#[derive(Clone)]
pub enum DataType {
    INTEGER,
    REAL,
    TEXT,
    BLOB
}

#[derive(Debug, PartialEq)]
pub enum Data {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>)
}
