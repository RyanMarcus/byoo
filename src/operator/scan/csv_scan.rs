use data::DataType;
use std::io::Read;
use csv::Reader;
use operator_buffer::OperatorWriteBuffer;

struct CsvScan<T> {
    reader: T,
    output: OperatorWriteBuffer
}

impl <T: Read> CsvScan<T> {
    fn start(mut self) {
        let mut rdr = Reader::from_reader(self.reader);
        for result in rdr.records() {
            let record = result.unwrap();
            let row: Vec<String> = record.iter()
                .map(|s| String::from(s))
                .collect();

            self.output.write_strings(row);
        }
    }
} 
