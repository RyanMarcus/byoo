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

#[cfg_attr(test, macro_use)] 
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
