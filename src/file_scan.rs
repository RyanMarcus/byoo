use operator_buffer::OperatorWriteBuffer;
use std::fs::File;
use std::io::{Seek, SeekFrom, BufReader};
use byteorder::{ReadBytesExt, LittleEndian};

struct FileScan {
    filename: String,
    buffer: OperatorWriteBuffer,
    from: usize,
    length: usize
}

impl FileScan {
    pub fn new(filename: String, from: usize, length: usize,
               buffer: OperatorWriteBuffer) -> FileScan {

        return FileScan {
            filename, from, length, buffer
        };
    }

    pub fn start(mut self) {
        let f = File::open(self.filename).unwrap();
        let mut reader = BufReader::new(f);
        reader.seek(SeekFrom::Start(self.from as u64 * 8)).unwrap();

        let mut data: Vec<u64> = Vec::with_capacity(128);
        
        for _ in 0..self.length {
            data.push(reader.read_u64::<LittleEndian>().unwrap());
            if data.len() == data.capacity() {
                self.buffer.write(&data);
                data.clear();
            }
        }

        self.buffer.write(&data);
    }
}
