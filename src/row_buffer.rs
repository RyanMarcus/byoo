use data::{Data, DataType};
use std::mem;

pub struct RowBuffer {
    types: Vec<DataType>,
    data: Vec<Data>,
    max_rows: usize,
    num_rows: usize
}

impl RowBuffer {
    pub fn new(types: Vec<DataType>, row_capacity: usize) -> RowBuffer {
        let capacity = types.len() * row_capacity;
        return RowBuffer {
            types,
            data: Vec::with_capacity(capacity),
            max_rows: row_capacity,
            num_rows: 0
        }
    }

    pub fn is_full(&self) -> bool {
        return self.num_rows == self.max_rows;
    }

    pub fn is_empty(&self) -> bool {
        return self.data.len() == 0;
    }

    pub fn num_rows(&self) -> usize {
        return self.num_rows;
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.num_rows = 0;
    }

    pub fn into_copy(&mut self) -> RowBuffer {
        let mut tmp = Vec::with_capacity(self.data.capacity());
        mem::swap(&mut self.data, &mut tmp);

        return RowBuffer {
            types: self.types.clone(),
            data: tmp,
            max_rows: self.max_rows,
            num_rows: self.num_rows
        };
    }

    pub fn raw_data(&self) -> &[Data] {
        return &self.data;
    }

    pub fn raw_data_mut(&mut self) -> &mut Vec<Data> {
        return &mut self.data;
    }

    pub fn recompute_row_count(&mut self) {
        self.num_rows = self.data.len() / self.types.len();
    }

    pub fn get_row(&self, row: usize) -> &[Data] {
        return &self.data[row*self.types.len()..(row+1)*self.types.len()];
    }

    #[cfg(any(test, debug_assertions))]
    fn write_value(&mut self, d: Data) {
        debug_assert!(!self.is_full());
        match self.types[self.data.len() % self.types.len()] {
            DataType::INTEGER => { debug_assert_matches!(d, Data::Integer(_)); },
            DataType::REAL => { debug_assert_matches!(d, Data::Real(_)); },
            DataType::TEXT => { debug_assert_matches!(d, Data::Text(_)); },
            DataType::BLOB => { debug_assert_matches!(d, Data::Blob(_)); }
        };
        
        self.data.push(d);
    }

    #[cfg(debug_assertions)]
    pub fn write_values(&mut self, data: Vec<Data>) {
        debug_assert!(data.len() == self.types.len());
        debug_assert!(!self.is_full());

        for d in data {
            self.write_value(d);
        }

        self.num_rows += 1;
    }

    #[cfg(not(debug_assertions))]
    pub fn write_values(&mut self, mut data: Vec<Data>) {
        self.data.append(&mut data);
        self.num_rows += 1;
    }

    pub fn copy_and_write_values(&mut self, data: &[Data]) {
        self.data.extend_from_slice(data);
        self.num_rows += 1;
    }
    
    pub fn iter(&self) -> RowBufferIterator {
        return RowBufferIterator::new(self);
    }

    pub fn to_vec(&self) -> Vec<Vec<Data>> {
        return self.data
            .chunks(self.types.len())
            .map(|c| c.to_vec())
            .collect();
    }
}


pub struct RowBufferIterator<'a> {
    rb: &'a RowBuffer,
    curr_row: usize
}

impl <'a> RowBufferIterator<'a> {
    fn new(rb: &RowBuffer) -> RowBufferIterator {
        return RowBufferIterator {
            rb,
            curr_row: 0
        }
    }
}

impl <'a> Iterator for RowBufferIterator<'a> {
    type Item = &'a [Data];

    fn next(&mut self) -> Option<&'a[Data]> {
        if self.curr_row >= self.rb.num_rows() {
            return None;
        }

        self.curr_row += 1;
        return Some(self.rb.get_row(self.curr_row - 1));
    }
}


#[cfg(test)]
#[cfg(test)]
mod tests {
    use row_buffer::RowBuffer;
    use data::{Data, DataType};

    #[test]
    fn fills() {
        let mut rb = RowBuffer::new(vec![DataType::INTEGER], 3);
        
        rb.write_values(vec![Data::Integer(5)]);
        assert!(!rb.is_full());

        rb.write_values(vec![Data::Integer(6)]);
        assert!(!rb.is_full());

        rb.write_values(vec![Data::Integer(7)]);
        assert!(rb.is_full());
    }

    #[test]
    fn fills_with_width() {
        let mut rb = RowBuffer::new(vec![DataType::INTEGER, DataType::REAL], 3);
        
        rb.write_values(vec![Data::Integer(5), Data::Real(5.5)]);
        assert!(!rb.is_full());

        rb.write_values(vec![Data::Integer(6), Data::Real(5.5)]);
        assert!(!rb.is_full());

        rb.write_values(vec![Data::Integer(7), Data::Real(5.5)]);
        assert!(rb.is_full());
    }

    #[test]
    fn fills_all_types() {
        let mut rb = RowBuffer::new(vec![DataType::INTEGER,
                                         DataType::REAL,
                                         DataType::TEXT,
                                         DataType::BLOB], 3);
        
        rb.write_values(vec![Data::Integer(5), Data::Real(5.5),
                             Data::Text(String::from("Hello!")),
                             Data::Blob(vec![5, 23, 95])]);
        assert!(!rb.is_full());

        rb.write_values(vec![Data::Integer(5), Data::Real(5.5),
                             Data::Text(String::from("Hello!")),
                             Data::Blob(vec![5, 23, 95])]);
        assert!(!rb.is_full());

        rb.write_values(vec![Data::Integer(5), Data::Real(5.5),
                             Data::Text(String::from("Hello!")),
                             Data::Blob(vec![5, 23, 95])]);
        assert!(rb.is_full());
    }

    #[test]
    fn test_iter() {
        let mut rb = RowBuffer::new(vec![DataType::INTEGER,
                                         DataType::REAL,
                                         DataType::TEXT,
                                         DataType::BLOB], 3);

        rb.write_values(vec![Data::Integer(5), Data::Real(5.5),
                             Data::Text(String::from("Hello!")),
                             Data::Blob(vec![89, 23, 95])]);
        assert!(!rb.is_full());

        rb.write_values(vec![Data::Integer(6), Data::Real(6.5),
                             Data::Text(String::from("World!")),
                             Data::Blob(vec![5, 27, 95])]);
        assert!(!rb.is_full());

        rb.write_values(vec![Data::Integer(7), Data::Real(7.5),
                             Data::Text(String::from("Testing!")),
                             Data::Blob(vec![5, 23, 96])]);
        assert!(rb.is_full());

        let mut num_iter = 0;
        for (idx, row) in rb.iter().enumerate() {
            if let Data::Integer(i) = row[0] {
                if idx == 0 { assert_eq!(i, 5); }
                if idx == 1 { assert_eq!(i, 6); }
                if idx == 2 { assert_eq!(i, 7); }
            } else {
                panic!("First type was not int!");
            }

            if let Data::Real(i) = row[1] {
                if idx == 0 { assert_eq!(i, 5.5); }
                if idx == 1 { assert_eq!(i, 6.5); }
                if idx == 2 { assert_eq!(i, 7.5); }
            } else {
                panic!("First type was not real!");
            }

            if let Data::Text(ref i) = row[2] {
                if idx == 0 { assert_eq!(i, "Hello!"); }
                if idx == 1 { assert_eq!(i, "World!"); }
                if idx == 2 { assert_eq!(i, "Testing!"); }
            } else {
                panic!("First type was not text!");
            }

            if let Data::Blob(ref i) = row[3] {
                if idx == 0 { assert_eq!(i, &vec![89 as u8, 23, 95]); }
                if idx == 1 { assert_eq!(i, &vec![5 as u8, 27, 95]); }
                if idx == 2 { assert_eq!(i, &vec![5 as u8, 23, 96]); }
            } else {
                panic!("First type was not blob!");
            }


            num_iter += 1;
        }

        assert_eq!(num_iter, 3);
    }
}
