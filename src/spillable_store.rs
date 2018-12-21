use data::{Data, DataType, WriteByooDataExt};
use std::mem;
use std::fs::File;
use std::io::{Write, BufWriter, BufReader, Seek, SeekFrom, ErrorKind};
use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer, make_buffer_pair};
use tempfile::tempfile;
use std::thread;
use std::thread::{JoinHandle};

pub struct WritableSpillableStore {
    data: Vec<Data>,
    types: Vec<DataType>,
    max_size: usize,
    backing_file: File,
    writer: BufWriter<File>,
    did_spill: bool,
    stats: SpillableStoreStats,
    jh: Option<JoinHandle<()>>
}

pub struct SpillableStoreStats {
    pub rows: usize,
    pub types: Vec<DataType>,
    pub col_sizes: Vec<usize>
}

struct ReadableSpillableStore {
    data: Vec<Data>,
    types: Vec<DataType>,
    reader: BufReader<File>,
    output: OperatorWriteBuffer
}

impl WritableSpillableStore {
    pub fn new(max_size: usize, types: Vec<DataType>) -> WritableSpillableStore {
        let f = tempfile().unwrap();
        let w = BufWriter::new(f.try_clone().unwrap());
        let types_copy = types.clone();
        let num_cols = types.len();
        return WritableSpillableStore {
            data: Vec::with_capacity(max_size / 4),
            types,
            max_size,
            backing_file: f,
            writer: w,
            did_spill: false,
            stats: SpillableStoreStats {
                rows: 0,
                types: types_copy,
                col_sizes: vec![0 ; num_cols]
            },
            jh: None
        };
    }

    pub fn push_row(&mut self, row: &[Data]) {
        // no writing while there is a reader out.
        assert_matches!(self.jh, None);
        
        self.stats.rows += 1;
        for (idx, d) in row.iter().enumerate() {
            self.stats.col_sizes[idx] += d.num_bytes();
        }
        
        if self.data.len() + row.len() < self.max_size {
            // it fits in memory
            self.data.extend_from_slice(row);
            return;
        }

        // it does not fit in memory -- we need to write out
        // the data buffer to the file and clear it out.
        self.did_spill = true;
        
        for d in self.data.iter() {
            self.writer.write_data(&d).unwrap();
        }
        self.data.clear();

        self.data.extend_from_slice(row);
    }

    #[cfg(test)]
    pub fn did_spill(&self) -> bool {
        return self.did_spill;
    }

    pub fn read(&mut self) -> (&SpillableStoreStats, OperatorReadBuffer) {
        self.writer.flush().unwrap();
        
        // Rust will let us make as many clones of an FD with .try_clone
        // as we want. As a result, multiple calls to read before one of the
        // ReadableSpillStores has finished will cause the FD to get seeked
        // around by multiple threads. So, we wait until the last read
        // is complete before we create a new one.
        let mut jh: Option<JoinHandle<()>> = None;
        mem::swap(&mut jh, &mut self.jh);
        
        if let Some(h) = jh {
            // previous reader exists, make sure it has finished.
            h.join().unwrap();
        }

        // At this point, we know there is no other reader running.
        let (r, w) = make_buffer_pair(5, 4096, self.types.clone());

        let reader = ReadableSpillableStore {
            data: self.data.clone(),
            types: self.types.clone(),
            reader: BufReader::new(self.backing_file.try_clone().unwrap()),
            output: w
        };

        self.jh = Some(thread::spawn(|| {
            reader.start();
        }));

        return (&self.stats, r);
    }

    pub fn into_read_buffer(mut self) -> (SpillableStoreStats, OperatorReadBuffer) {
        let (_, r) = self.read();
        return (self.stats, r);
    }
}

impl ReadableSpillableStore {
    fn start(mut self) {
        // seek to the start
        self.reader.seek(SeekFrom::Start(0)).unwrap();
        
        // first, read through the entire file.
        while self.read_row_from_file() {}

        // now, emit all of the remaining data in the buffer
        self.output.write_many(self.data);
        self.output.flush();
    }

    fn read_row_from_file(&mut self) -> bool {
        let mut row = Vec::with_capacity(self.types.len());
        for dt in self.types.iter() {
            match dt.read_item(&mut self.reader) {
                Ok(v) => { row.push(v); },
                Err(e) => {
                    if let ErrorKind::UnexpectedEof = e.kind() {
                        // if we hit an EOF, we shouldn't have any data in the row
                        assert!(row.is_empty());
                        return false;
                    } else {
                        panic!("Unexpected error when reading spill storage");
                    }
                }
            }
        }
        self.output.write(row);
        return true;
    }
}


#[cfg(test)]
mod tests {

    use data::{DataType,Data};
    use spillable_store::WritableSpillableStore;
    
    #[test]
    fn no_spill_test() {
        let dt = vec![DataType::INTEGER];
        let mut w = WritableSpillableStore::new(100, dt);
        w.push_row(&[Data::Integer(5)]);
        w.push_row(&[Data::Integer(6)]);
        w.push_row(&[Data::Integer(7)]);

        let (stats, mut r) = w.read();

        assert_eq!(stats.rows, 3);
        assert_eq!(stats.col_sizes[0], 3 * 8);
        
        let mut num_rows = 0;
        iterate_buffer!(r, idx, row, {
            match idx {
                0 => { assert_eq!(row[0], Data::Integer(5)); },
                1 => { assert_eq!(row[0], Data::Integer(6)); },
                2 => { assert_eq!(row[0], Data::Integer(7)); },
                _ => { panic!("too many values!"); }
            }
            num_rows += 1;
        });

        assert_eq!(num_rows, 3);
    }

    #[test]
    fn spill_test_singlecol() {
        let dt = vec![DataType::INTEGER];
        let mut w = WritableSpillableStore::new(5, dt);

        let num_rows: usize = 10;
        
        for i in 0..num_rows {
            w.push_row(&[Data::Integer(i as i64)]);
        }

        assert!(w.did_spill());
        
        let (stats, mut r) = w.read();

        assert_eq!(stats.rows, num_rows);
        assert_eq!(stats.col_sizes[0], num_rows * 8);
        
        let mut num_rows = 0;
        iterate_buffer!(r, idx, row, {
            assert_eq!(row[0], Data::Integer(idx));
            num_rows += 1;
        });
        
        assert_eq!(num_rows, num_rows);
    }

    #[test]
    fn spill_test_multicol() {
        let dt = vec![DataType::INTEGER, DataType::INTEGER, DataType::TEXT];
        let mut w = WritableSpillableStore::new(100, dt);

        for _ in 0..10000 {
            w.push_row(&[Data::Integer(5),
                         Data::Integer(6),
                         Data::Text(String::from("hello"))]);
            w.push_row(&[Data::Integer(-5),
                         Data::Integer(60),
                         Data::Text(String::from("world!"))]);

        }

        assert!(w.did_spill());

        let (stats, mut r) = w.read();

        assert_eq!(stats.rows, 20000);
        assert_eq!(stats.col_sizes[0], 20000 * 8);
        assert_eq!(stats.col_sizes[1], 20000 * 8);
        
        let mut num_rows = 0;
        iterate_buffer!(r, idx, row, {
            match idx % 2 {
                0 => {
                    assert_eq!(row[0], Data::Integer(5));
                    assert_eq!(row[1], Data::Integer(6));
                    if let Data::Text(s) = &row[2] {
                        assert_eq!(s, "hello");
                    } else {
                        panic!("Wrong data type");
                    }
                },
                1 => {
                    assert_eq!(row[0], Data::Integer(-5));
                    assert_eq!(row[1], Data::Integer(60));
                    if let Data::Text(s) = &row[2] {
                        assert_eq!(s, "world!");
                    } else {
                        panic!("Wrong data type");
                    }
                },
                _ => { panic!("invalid mod value"); }
            }
            num_rows += 1;
        });

        assert_eq!(num_rows, 20000);
    }

    #[test]
    fn spill_test_multicol_multiread() {
        let dt = vec![DataType::INTEGER, DataType::INTEGER, DataType::TEXT];
        let mut w = WritableSpillableStore::new(100, dt);

        for _ in 0..10000 {
            w.push_row(&[Data::Integer(5),
                         Data::Integer(6),
                         Data::Text(String::from("hello"))]);
            w.push_row(&[Data::Integer(-5),
                         Data::Integer(60),
                         Data::Text(String::from("world!"))]);

        }

        assert!(w.did_spill());
        for _ in 0..10 {
            let (stats, mut r) = w.read();

            assert_eq!(stats.rows, 20000);
            assert_eq!(stats.col_sizes[0], 20000 * 8);
            assert_eq!(stats.col_sizes[1], 20000 * 8);
            
            let mut num_rows = 0;
            iterate_buffer!(r, idx, row, {
                match idx % 2 {
                    0 => {
                        assert_eq!(row[0], Data::Integer(5));
                        assert_eq!(row[1], Data::Integer(6));
                        if let Data::Text(s) = &row[2] {
                            assert_eq!(s, "hello");
                        } else {
                            panic!("Wrong data type");
                        }
                    },
                    1 => {
                        assert_eq!(row[0], Data::Integer(-5));
                        assert_eq!(row[1], Data::Integer(60));
                        if let Data::Text(s) = &row[2] {
                            assert_eq!(s, "world!");
                        } else {
                            panic!("Wrong data type");
                        }
                    },
                    _ => { panic!("invalid mod value"); }
                }
                num_rows += 1;
            });

            assert_eq!(num_rows, 20000);
        }
    }
}
