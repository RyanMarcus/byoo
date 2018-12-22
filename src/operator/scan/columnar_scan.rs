use operator_buffer::{OperatorWriteBuffer, OperatorReadBuffer};
use operator::ConstructableOperator;
use std::fs::File;
use std::io::{Seek, SeekFrom, BufRead, BufReader};
use byteorder::{ReadBytesExt, LittleEndian};
use data::{DataType, ReadByooDataExt};
use serde_json;

pub struct ColumnarScan<T> {
    reader: T,
    buffer: OperatorWriteBuffer,
    col_idx: usize
}

impl <T: BufRead + Seek> ColumnarScan<T> {
    pub fn new(reader: T, col_idx: usize,
               buffer: OperatorWriteBuffer) -> ColumnarScan<T> {

        return ColumnarScan {
            reader, col_idx, buffer
        };
    }

    pub fn start(mut self) {
        // read the format code
        let format_code = self.reader.read_u8().unwrap();

        assert_eq!(format_code, 1); // column order

        // read the number of columns
        let num_columns = self.reader.read_u16::<LittleEndian>().unwrap() as usize;
        assert!(self.col_idx < num_columns);

        // read the number of rows
        let num_rows = self.reader.read_u64::<LittleEndian>().unwrap();
        
        // next is the column data types
        let mut datatypes = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            datatypes.push(self.reader.read_u16::<LittleEndian>().unwrap());
        }

        // next is the column offsets
        let mut offsets = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            offsets.push(self.reader.read_u64::<LittleEndian>().unwrap());
        }


        let datatype = DataType::from_code(datatypes[self.col_idx]);
        let offset = offsets[self.col_idx];
        self.reader.seek(SeekFrom::Start(offset as u64)).unwrap();

        let mut snp_read = BufReader::new(snap::Reader::new(self.reader));
        for _ in 0..num_rows {
            let data = snp_read.read_data(&datatype).unwrap();
            self.buffer.write(vec![data]);
        }
        
    }
}

impl ConstructableOperator for ColumnarScan<BufReader<File>> {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    options: serde_json::Value) -> Self {
        assert!(input.is_empty());
        let out = output.unwrap();
        let f = file.unwrap();
        let col_idx = options["col"]
            .as_i64().unwrap() as usize;

        return ColumnarScan::new(BufReader::new(f), col_idx, out);
    }
}
