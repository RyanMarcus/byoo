use byteorder::{LittleEndian, WriteBytesExt};
use operator_buffer::OperatorReadBuffer;
use spillable_store::WritableSpillableStore;
use std::io::Write;

pub struct ColumnarOutput<T> {
    input: OperatorReadBuffer,
    int_bufs: Vec<WritableSpillableStore>,
    output: T,
}

impl<T: Write> ColumnarOutput<T> {
    pub fn new(buf_size: usize, input: OperatorReadBuffer, output: T) -> ColumnarOutput<T> {
        let types = input.types().to_vec();

        let mut internal_bufs = Vec::with_capacity(types.len());

        for dt in types.iter() {
            internal_bufs.push(WritableSpillableStore::new(buf_size, vec![dt.clone()]));
        }

        return ColumnarOutput {
            int_bufs: internal_bufs,
            input,
            output,
        };
    }

    pub fn start(mut self) {
        // first, push everything into a spillable buffer.
        iterate_buffer!(self.input, row, {
            for (v, mut sbuf) in row.into_iter().zip(self.int_bufs.iter_mut()) {
                sbuf.push_row(vec![v.clone()]);
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
        self.output
            .write_u16::<LittleEndian>(all_stats.len() as u16)
            .unwrap();

        // num rows
        let num_rows = all_stats[0].rows;
        self.output
            .write_u64::<LittleEndian>(num_rows as u64)
            .unwrap();

        // write out column data types
        for dt in all_stats.iter() {
            self.output
                .write_u16::<LittleEndian>(dt.types[0].to_code())
                .unwrap();
        }

        // compute the column offsets
        let header_size = 1 + 2 + 8 + all_stats.len() * 2 + all_stats.len() * 8;
        let mut col_offset_counter = header_size;

        for stats in all_stats.iter() {
            let col_size = stats.col_sizes[0];
            self.output
                .write_u64::<LittleEndian>(col_offset_counter as u64)
                .unwrap();
            col_offset_counter += col_size;
        }

        // output the data
        for mut col_reader in all_readers {
            iterate_buffer!(col_reader, idx, data, {
                debug_assert!(idx < num_rows);
                let bytes = data[0].clone().into_bytes();
                self.output.write(&bytes).unwrap();
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{LittleEndian, ReadBytesExt};
    use data::{Data, DataType};
    use operator::output::ColumnarOutput;
    use operator_buffer::make_buffer_pair;
    use std::io::Cursor;

    #[test]
    fn writes_single_col() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(7)]);
        drop(w);

        let mut output_data = Vec::new();

        {
            let co = ColumnarOutput::new(1024, r, &mut output_data);
            co.start();
        }

        let mut cursor = Cursor::new(output_data);

        assert_eq!(cursor.read_u8().unwrap(), 1); // tag
        assert_eq!(cursor.read_u16::<LittleEndian>().unwrap(), 1); // cols
        assert_eq!(cursor.read_u64::<LittleEndian>().unwrap(), 3); // rows
        assert_eq!(
            cursor.read_u16::<LittleEndian>().unwrap(),
            DataType::INTEGER.to_code()
        ); // col code
        assert_eq!(cursor.read_u64::<LittleEndian>().unwrap(), 21); // col offset
        assert_eq!(
            DataType::INTEGER.read_item(&mut cursor).unwrap(),
            Data::Integer(5)
        );
        assert_eq!(
            DataType::INTEGER.read_item(&mut cursor).unwrap(),
            Data::Integer(6)
        );
        assert_eq!(
            DataType::INTEGER.read_item(&mut cursor).unwrap(),
            Data::Integer(7)
        );
    }

    #[test]
    fn writes_multi_col() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER, DataType::TEXT]);

        w.write(vec![Data::Integer(5), Data::Text(String::from("string 1"))]);
        w.write(vec![
            Data::Integer(6),
            Data::Text(String::from("a longer string")),
        ]);
        w.write(vec![Data::Integer(7), Data::Text(String::from("c"))]);
        w.write(vec![Data::Integer(-8), Data::Text(String::from("!!!"))]);
        drop(w);

        let mut output_data = Vec::new();

        {
            let co = ColumnarOutput::new(1024, r, &mut output_data);
            co.start();
        }

        let mut cursor = Cursor::new(output_data);

        assert_eq!(cursor.read_u8().unwrap(), 1); // tag
        assert_eq!(cursor.read_u16::<LittleEndian>().unwrap(), 2); // cols
        assert_eq!(cursor.read_u64::<LittleEndian>().unwrap(), 4); // rows
        assert_eq!(
            cursor.read_u16::<LittleEndian>().unwrap(),
            DataType::INTEGER.to_code()
        ); // col code
        assert_eq!(
            cursor.read_u16::<LittleEndian>().unwrap(),
            DataType::TEXT.to_code()
        ); // col code

        assert_eq!(cursor.read_u64::<LittleEndian>().unwrap(), 31); // col offset
        assert_eq!(cursor.read_u64::<LittleEndian>().unwrap(), 31 + 4 * 8); // col offset
        assert_eq!(
            DataType::INTEGER.read_item(&mut cursor).unwrap(),
            Data::Integer(5)
        );
        assert_eq!(
            DataType::INTEGER.read_item(&mut cursor).unwrap(),
            Data::Integer(6)
        );
        assert_eq!(
            DataType::INTEGER.read_item(&mut cursor).unwrap(),
            Data::Integer(7)
        );
        assert_eq!(
            DataType::INTEGER.read_item(&mut cursor).unwrap(),
            Data::Integer(-8)
        );

        let s1 = String::from("string 1");
        assert_eq!(
            DataType::TEXT.read_item(&mut cursor).unwrap(),
            Data::Text(s1)
        );

        let s2 = String::from("a longer string");
        assert_eq!(
            DataType::TEXT.read_item(&mut cursor).unwrap(),
            Data::Text(s2)
        );

        let s3 = String::from("c");
        assert_eq!(
            DataType::TEXT.read_item(&mut cursor).unwrap(),
            Data::Text(s3)
        );

        let s4 = String::from("!!!");
        assert_eq!(
            DataType::TEXT.read_item(&mut cursor).unwrap(),
            Data::Text(s4)
        );
    }
}
