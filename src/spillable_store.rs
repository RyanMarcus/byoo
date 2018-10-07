use data::{Data, DataType};
use std::mem;
use std::fs::File;
use std::io::{Write, BufWriter, Read, BufReader, Seek, SeekFrom, ErrorKind};
use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer, make_buffer_pair};
use tempfile::tempfile;
use std::thread;

pub struct WritableSpillableStore {
    data: Vec<Data>,
    types: Vec<DataType>,
    max_size: usize,
    backing_file: File,
    writer: BufWriter<File>,
    did_spill: bool,
    stats: SpillableStoreStats
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
        return WritableSpillableStore {
            data: Vec::with_capacity(max_size / 4),
            types: types.clone(),
            max_size: max_size,
            backing_file: f,
            writer: w,
            did_spill: false,
            stats: SpillableStoreStats {
                rows: 0,
                types: types.clone(),
                col_sizes: vec![0 ; types.len()]
            }
        };
    }

    pub fn push_row(&mut self, row: Vec<Data>) {
        self.stats.rows += 1;
        for (idx, d) in row.iter().enumerate() {
            self.stats.col_sizes[idx] += d.num_bytes();
        }
        
        if self.data.len() + row.len() < self.max_size {
            // it fits in memory
            self.data.extend(row);
            return;
        }

        // it does not fit in memory -- we need to write out
        // the data buffer to the file and replace it with a new one.
        self.did_spill = true;
        let mut buf = Vec::with_capacity(self.max_size);
        mem::swap(&mut buf, &mut self.data);
        
        for d in buf {
            self.writer.write(&d.into_bytes());
        }

        self.data.extend(row);
    }

    pub fn did_spill(&self) -> bool {
        return self.did_spill;
    }

    pub fn read(self) -> (SpillableStoreStats, OperatorReadBuffer) {
        let (r, w) = make_buffer_pair(5, 4096, self.types.clone());
        
        let reader = ReadableSpillableStore {
            data: self.data,
            types: self.types,
            reader: BufReader::new(self.backing_file),
            output: w
        };

        thread::spawn(|| {
            reader.start();
        });

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
                        assert!(row.len() == 0);
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
        w.push_row(vec![Data::Integer(5)]);
        w.push_row(vec![Data::Integer(6)]);
        w.push_row(vec![Data::Integer(7)]);

        let (stats, mut r) = w.read();

        assert_eq!(stats.rows, 3);
        assert_eq!(stats.col_sizes[0], 3 * 8);
        
        let mut num_rows = 0;
        iterate_buffer!(r, idx, row, {
            match idx {
                0 => { assert_matches!(row[0], Data::Integer(5)); },
                1 => { assert_matches!(row[0], Data::Integer(6)); },
                2 => { assert_matches!(row[0], Data::Integer(7)); },
                _ => { panic!("too many values!"); }
            }
            num_rows += 1;
        });

        assert_eq!(num_rows, 3);
    }

    #[test]
    fn spill_test() {
        let dt = vec![DataType::INTEGER];
        let mut w = WritableSpillableStore::new(100, dt);

        for _ in 0..10000 {
            w.push_row(vec![Data::Integer(5)]);
            w.push_row(vec![Data::Integer(6)]);
            w.push_row(vec![Data::Integer(7)]);
        }

        assert!(w.did_spill());
        
        let (stats, mut r) = w.read();

        assert_eq!(stats.rows, 30000);
        assert_eq!(stats.col_sizes[0], 30000 * 8);
        
        let mut num_rows = 0;
        iterate_buffer!(r, idx, row, {
            match idx % 3 {
                0 => { assert_matches!(row[0], Data::Integer(5)); },
                1 => { assert_matches!(row[0], Data::Integer(6)); },
                2 => { assert_matches!(row[0], Data::Integer(7)); },
                _ => { panic!("invalid mod value"); }
            }
            num_rows += 1;
        });

        assert_eq!(num_rows, 30000);
    }

    #[test]
    fn spill_test_multicol() {
        let dt = vec![DataType::INTEGER, DataType::INTEGER, DataType::TEXT];
        let mut w = WritableSpillableStore::new(100, dt);

        for _ in 0..10000 {
            w.push_row(vec![Data::Integer(5),
                            Data::Integer(6),
                            Data::Text(String::from("hello"))]);
            w.push_row(vec![Data::Integer(-5),
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
                    assert_matches!(row[0], Data::Integer(5));
                    assert_matches!(row[1], Data::Integer(6));
                    if let Data::Text(s) = &row[2] {
                        assert_eq!(s, "hello");
                    } else {
                        panic!("Wrong data type");
                    }
                },
                1 => {
                    assert_matches!(row[0], Data::Integer(-5));
                    assert_matches!(row[1], Data::Integer(60));
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
