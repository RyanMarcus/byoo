pub mod scan;

mod column_union;
mod sort;

pub mod output;
pub mod join;
pub mod groupby;

pub use operator::column_union::ColumnUnion;
pub use operator::sort::Sort;

use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use std::fs::File;
use serde_json;

pub trait ConstructableOperator {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    options: serde_json::Value) -> Self;
}
