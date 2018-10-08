use byteorder::{LittleEndian, ReadBytesExt};
use data::DataType;
use operator_buffer::OperatorWriteBuffer;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};

struct ColumnarScan {
    filename: String,
    buffer: OperatorWriteBuffer,
    col_idx: usize,
}

impl ColumnarScan {
    pub fn new(filename: String, col_idx: usize, buffer: OperatorWriteBuffer) -> ColumnarScan {
        return ColumnarScan {
            filename,
            col_idx,
            buffer,
        };
    }

    pub fn start(mut self) {
        let f = File::open(self.filename).unwrap();
        let mut reader = BufReader::new(f);

        // read the format code
        let format_code = reader.read_u8().unwrap();

        assert_eq!(format_code, 1); // column order

        // read the number of columns
        let num_columns = reader.read_u16::<LittleEndian>().unwrap() as usize;
        assert!(self.col_idx < num_columns);

        // read the number of rows
        let num_rows = reader.read_u64::<LittleEndian>().unwrap();

        // next is the column data types
        let mut datatypes = Vec::with_capacity(num_columns);
        reader
            .read_u16_into::<LittleEndian>(&mut datatypes)
            .unwrap();

        // next is the column offsets
        let mut offsets = Vec::with_capacity(num_columns);
        reader.read_u64_into::<LittleEndian>(&mut offsets).unwrap();

        let datatype = DataType::from_code(datatypes[self.col_idx]);
        let offset = offsets[self.col_idx];
        reader.seek(SeekFrom::Current(offset as i64)).unwrap();

        for _ in 0..num_rows {
            let data = datatype.read_item(&mut reader).unwrap();
            self.buffer.write(vec![data]);
        }
    }
}
