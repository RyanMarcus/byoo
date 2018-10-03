#[macro_use]
extern crate matches;
extern crate byteorder;

mod operator_buffer;
mod row_buffer;
//mod file_scan;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
