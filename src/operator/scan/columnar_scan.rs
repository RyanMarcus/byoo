use operator_buffer::{OperatorWriteBuffer, OperatorReadBuffer};
use operator::ConstructableOperator;
use std::fs::File;
use std::io::{Seek, SeekFrom, BufRead, BufReader};
use byteorder::{ReadBytesExt, LittleEndian};
use data::{DataType};
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
        self.reader.read_u16_into::<LittleEndian>(&mut datatypes).unwrap();

        // next is the column offsets
        let mut offsets = Vec::with_capacity(num_columns);
        self.reader.read_u64_into::<LittleEndian>(&mut offsets).unwrap();


        let datatype = DataType::from_code(datatypes[self.col_idx]);
        let offset = offsets[self.col_idx];
        self.reader.seek(SeekFrom::Current(offset as i64)).unwrap();

        for _ in 0..num_rows {
            let data = datatype.read_item(&mut self.reader).unwrap();
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
        let col_idx = options["column index"]
            .as_i64().unwrap() as usize;

        return ColumnarScan::new(BufReader::new(f), col_idx, out);
    }
}
