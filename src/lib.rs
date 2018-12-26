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
#![allow(clippy::needless_return)]

#[macro_use]
extern crate matches;
extern crate byteorder;
extern crate csv;
extern crate tempfile;
extern crate base64;
extern crate binary_heap_plus;
extern crate rand;
extern crate fnv;
extern crate hashbrown;
extern crate snap;


#[macro_use] 
extern crate serde_json;

extern crate either;

mod data;

#[macro_use]
mod operator_buffer;

mod row_buffer;
mod spillable_store;
mod hash_partition_store;
mod operator;

mod predicate;
mod agg;
mod compile;

pub use compile::{compile, tree_to_gv};
pub use data::Data;
pub use data::rows_to_string;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
