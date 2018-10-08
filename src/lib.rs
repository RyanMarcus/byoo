#[macro_use]
extern crate matches;
extern crate byteorder;
extern crate csv;
extern crate tempfile;

mod data;

#[macro_use]
mod operator_buffer;

mod columnar_scan;
mod operator;
mod row_buffer;
mod spillable_store;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
