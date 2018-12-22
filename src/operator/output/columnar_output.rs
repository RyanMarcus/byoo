use operator_buffer::{OperatorWriteBuffer, OperatorReadBuffer};
use spillable_store::WritableSpillableStore;
use std::io::{BufWriter, Write, Seek, SeekFrom};
use std::fs::File;
use byteorder::{WriteBytesExt, LittleEndian};
use operator::ConstructableOperator;
use serde_json;
use data::{WriteByooDataExt, ReadByooDataExt};
use tempfile::tempfile;
use std::io;


pub struct ColumnarOutput<T> {
    input: OperatorReadBuffer,
    int_bufs: Vec<WritableSpillableStore>,
    output: T
}

impl <T: Write + Seek> ColumnarOutput<T> {
    pub fn new(buf_size: usize, input: OperatorReadBuffer, output: T) -> ColumnarOutput<T> {
        let types = input.types().to_vec();

        let mut internal_bufs = Vec::with_capacity(types.len());

        for dt in types.iter() {
            internal_bufs.push(WritableSpillableStore::new(buf_size, vec![dt.clone()]));
        }
        
        return ColumnarOutput {
            int_bufs: internal_bufs,
            input, output
        };
    }

    pub fn start(mut self) {
        // first, push everything into a spillable buffer.
        iterate_buffer!(self.input, row, {
            for (v, mut sbuf) in row.into_iter().zip(self.int_bufs.iter_mut()) {
                sbuf.push_row(&[v.clone()]);
            }
        });

        // next, get the stats and spill readers
        let mut all_stats = Vec::new();
        let mut all_readers = Vec::new();

        for buf in self.int_bufs.iter_mut() {
            let (stats, reader) = buf.read();
            all_stats.push(stats);
            all_readers.push(reader);
        }
        
        self.output.write_u8(1).unwrap(); // columnar format
        
        // num columns
        self.output.write_u16::<LittleEndian>(all_stats.len() as u16).unwrap();

        // num rows
        let num_rows = all_stats[0].rows;
        self.output.write_u64::<LittleEndian>(num_rows as u64).unwrap();

        // write out column data types
        for dt in all_stats.iter() {
            self.output.write_u16::<LittleEndian>(dt.types[0].to_code()).unwrap();
        }

        // compute the header size (data before column offsets)
        let header_size = (1 + 2 + 8 + all_stats.len()*2) as u64;

        // write zeros for the column offsets for now
        for _ in all_stats.iter() {
            self.output.write_u64::<LittleEndian>(0).unwrap();
        }

        let mut column_sizes = vec![];

        // output the data
        for mut col_reader in all_readers {
            let mut f = tempfile().unwrap();

            // this extra scope ensures snp_wrt gets fully flushed
            {
                let mut snp_wrt = snap::Writer::new(f.try_clone().unwrap());
                iterate_buffer!(col_reader, idx, data, {
                    assert!(idx < num_rows);
                    snp_wrt.write_data(&data[0]).unwrap();
                });
            }
            
            f.seek(SeekFrom::Start(0)).unwrap();
            let num_bytes = io::copy(&mut f, &mut self.output).unwrap();
            column_sizes.push(num_bytes);
        }

        // go and fill in the column offest data
        self.output.seek(SeekFrom::Start(header_size)).unwrap();
        let mut accum = header_size + 8*all_stats.len() as u64;
        for offset in column_sizes {
            self.output.write_u64::<LittleEndian>(accum).unwrap();
            accum += offset;
        }
    }
}

impl ConstructableOperator for ColumnarOutput<BufWriter<File>> {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    _options: serde_json::Value) -> Self {

        assert!(output.is_none());
        let f = file.unwrap();

        let mut inp = input;
        let inp_v = inp.remove(0);
        assert!(inp.is_empty());

        return ColumnarOutput::new(4096, inp_v, BufWriter::new(f));
    }
    
}


#[cfg(test)]
mod tests {
    use operator::output::ColumnarOutput;
    use operator_buffer::{make_buffer_pair};
    use data::{Data,DataType, ReadByooDataExt};
    use byteorder::{ReadBytesExt, LittleEndian};
    use std::io::{Cursor, Seek, SeekFrom};

    
    #[test]
    fn writes_single_col() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(7)]);
        drop(w);

        let mut cursor = Cursor::new(Vec::new());

        {
            let co = ColumnarOutput::new(1024, r, &mut cursor);
            co.start();
        }

        cursor.seek(SeekFrom::Start(0)).unwrap();
        
        assert_eq!(cursor.read_u8().unwrap(), 1); // tag
        assert_eq!(cursor.read_u16::<LittleEndian>().unwrap(), 1); // cols
        assert_eq!(cursor.read_u64::<LittleEndian>().unwrap(), 3); // rows
        assert_eq!(cursor.read_u16::<LittleEndian>().unwrap(),
                   DataType::INTEGER.to_code()); // col code
    }

     
    #[test]
    fn writes_multi_col() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER,
                                                      DataType::TEXT]);

        
        w.write(vec![Data::Integer(5), Data::Text(String::from("string 1"))]);
        w.write(vec![Data::Integer(6), Data::Text(String::from("a longer string"))]);
        w.write(vec![Data::Integer(7), Data::Text(String::from("c"))]);
        w.write(vec![Data::Integer(-8), Data::Text(String::from("!!!"))]);
        drop(w);

        let mut cursor = Cursor::new(Vec::new());

        {
            let co = ColumnarOutput::new(1024, r, &mut cursor);
            co.start();
        }

        cursor.seek(SeekFrom::Start(0)).unwrap();
        
        assert_eq!(cursor.read_u8().unwrap(), 1); // tag
        assert_eq!(cursor.read_u16::<LittleEndian>().unwrap(), 2); // cols
        assert_eq!(cursor.read_u64::<LittleEndian>().unwrap(), 4); // rows
        assert_eq!(cursor.read_u16::<LittleEndian>().unwrap(),
                   DataType::INTEGER.to_code()); // col code
        assert_eq!(cursor.read_u16::<LittleEndian>().unwrap(),
                   DataType::TEXT.to_code()); // col code
    }
}
