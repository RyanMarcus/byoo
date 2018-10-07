use data::DataType;
use std::io::Read;
use csv::Reader;
use operator_buffer::OperatorWriteBuffer;

pub struct CsvScan<T> {
    reader: T,
    output: OperatorWriteBuffer
}

impl <T: Read> CsvScan<T> {

    fn new(reader: T, output: OperatorWriteBuffer) -> CsvScan<T> {
        return CsvScan {
            reader, output
        };
    }
    
    fn start(mut self) {
        let mut rdr = Reader::from_reader(self.reader);
        for result in rdr.records() {
            let record = result.unwrap();
            let row: Vec<String> = record.iter()
                .map(|s| String::from(s))
                .collect();

            self.output.write_strings(row);
        }

        self.output.flush();
    }
}

#[cfg(test)]
mod tests {
    use operator::scan::CsvScan;
    use operator_buffer::make_buffer_pair;
    use data::{Data, DataType};

    #[test]
    fn reads_simple_csv() {
        let csv_data = "header1,header2,header3
1,2,3
4,5,6
7,8,9
".as_bytes();

        
        let (mut r, w) = make_buffer_pair(5, 10, vec![
            DataType::INTEGER, DataType::INTEGER, DataType::INTEGER]);
        
        let filter = CsvScan::new(csv_data, w);
        filter.start();

        let mut num_rows = 0;
        iterate_buffer!(r, idx, row, {
            assert_eq!(row.len(), 3);
            num_rows += 1;
            match idx {
                0 => {
                    assert_matches!(row[0], Data::Integer(1));
                    assert_matches!(row[1], Data::Integer(2));
                    assert_matches!(row[2], Data::Integer(3));
                },
                1 => {
                    assert_matches!(row[0], Data::Integer(4));
                    assert_matches!(row[1], Data::Integer(5));
                    assert_matches!(row[2], Data::Integer(6));

                },
                2 => {
                    assert_matches!(row[0], Data::Integer(7));
                    assert_matches!(row[1], Data::Integer(8));
                    assert_matches!(row[2], Data::Integer(9));
                }
                _ => { panic!("too many rows!"); }
            }
        });

        assert_eq!(num_rows, 3);
    }

    #[test]
    fn reads_multitype_csv() {
        let csv_data = "header1,header2,header3
1,2,3
4,hello,6
7,8,9
".as_bytes();

        
        let (mut r, w) = make_buffer_pair(5, 10, vec![
            DataType::INTEGER, DataType::TEXT, DataType::INTEGER]);
        
        let filter = CsvScan::new(csv_data, w);
        filter.start();

        let mut num_rows = 0;
        iterate_buffer!(r, idx, row, {
            assert_eq!(row.len(), 3);
            num_rows += 1;
            match idx {
                0 => {
                    let s = String::from("2");
                    assert_matches!(row[0], Data::Integer(1));
                    assert_matches!(row[1], Data::Text(ref s));
                    assert_matches!(row[2], Data::Integer(3));
                },
                1 => {
                    let s = String::from("hello");
                    assert_matches!(row[0], Data::Integer(4));
                    assert_matches!(row[1], Data::Text(ref s));
                    assert_matches!(row[2], Data::Integer(6));

                },
                2 => {
                    let s = String::from("8");
                    assert_matches!(row[0], Data::Integer(7));
                    assert_matches!(row[1], Data::Text(ref s));
                    assert_matches!(row[2], Data::Integer(9));
                }
                _ => { panic!("too many rows!"); }
            }
        });

        assert_eq!(num_rows, 3);
    }
}
