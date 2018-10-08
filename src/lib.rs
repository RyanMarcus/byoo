#[macro_use]
extern crate matches;
extern crate byteorder;
extern crate csv;
extern crate tempfile;

mod data;

#[macro_use]
mod operator_buffer;

mod row_buffer;
mod spillable_store;
mod columnar_scan;
mod operator;


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
