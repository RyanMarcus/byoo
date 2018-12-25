// < begin copyright > 
// Copyright Ryan Marcus 2018
// 
// This file is part of byoo.
// 
// byoo is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// byoo is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with byoo.  If not, see <http://www.gnu.org/licenses/>.
// 
// < end copyright > 
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
