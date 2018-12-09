use data::{Data, DataType};

pub struct RowBuffer {
    types: Vec<DataType>,
    data: Vec<Data>,
    max_rows: usize
}

impl RowBuffer {
    pub fn new(types: Vec<DataType>, row_capacity: usize) -> RowBuffer {
        let capacity = types.len() * row_capacity;
        return RowBuffer {
            types: types,
            data: Vec::with_capacity(capacity),
            max_rows: row_capacity
        }
    }

    pub fn is_full(&self) -> bool {
        return self.num_rows() == self.max_rows;
    }

    pub fn is_empty(&self) -> bool {
        return self.data.len() == 0;
    }

    fn num_rows(&self) -> usize {
        return self.data.len() / self.types.len();
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    fn get_row(&self, row: usize) -> &[Data] {
        return &self.data[row*self.types.len()..(row+1)*self.types.len()];
    }

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

    pub fn write_values(&mut self, data: Vec<Data>) {
        debug_assert!(data.len() == self.types.len());
        debug_assert!(!self.is_full());

        for d in data {
            self.write_value(d);
        }
    }

    pub fn iter(&self) -> RowBufferIterator {
        return RowBufferIterator::new(self);
    }
}


pub struct RowBufferIterator<'a> {
    rb: &'a RowBuffer,
    curr_row: usize
}

impl <'a> RowBufferIterator<'a> {
    fn new(rb: &RowBuffer) -> RowBufferIterator {
        return RowBufferIterator {
            rb: rb,
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
        
        rb.write_value(Data::Integer(5));
        assert!(!rb.is_full());

        rb.write_value(Data::Integer(6));
        assert!(!rb.is_full());

        rb.write_value(Data::Integer(7));
        assert!(rb.is_full());
    }

    #[test]
    fn fills_with_width() {
        let mut rb = RowBuffer::new(vec![DataType::INTEGER, DataType::REAL], 3);
        
        rb.write_value(Data::Integer(5));
        rb.write_value(Data::Real(5.5));
        assert!(!rb.is_full());

        rb.write_value(Data::Integer(6));
        rb.write_value(Data::Real(5.5));
        assert!(!rb.is_full());

        rb.write_value(Data::Integer(7));
        rb.write_value(Data::Real(5.5));
        assert!(rb.is_full());
    }

    #[test]
    fn fills_all_types() {
        let mut rb = RowBuffer::new(vec![DataType::INTEGER,
                                         DataType::REAL,
                                         DataType::TEXT,
                                         DataType::BLOB], 3);
        
        rb.write_value(Data::Integer(5));
        rb.write_value(Data::Real(5.5));
        rb.write_value(Data::Text(String::from("Hello!")));
        rb.write_value(Data::Blob(vec![5, 23, 95]));
        assert!(!rb.is_full());

        rb.write_value(Data::Integer(6));
        rb.write_value(Data::Real(5.5));
        rb.write_value(Data::Text(String::from("Hello!")));
        rb.write_value(Data::Blob(vec![5, 23, 95]));
        assert!(!rb.is_full());

        rb.write_value(Data::Integer(7));
        rb.write_value(Data::Real(5.5));
        rb.write_value(Data::Text(String::from("Hello!")));
        rb.write_value(Data::Blob(vec![5, 23, 95]));
        assert!(rb.is_full());
    }

    #[test]
    fn test_iter() {
        let mut rb = RowBuffer::new(vec![DataType::INTEGER,
                                         DataType::REAL,
                                         DataType::TEXT,
                                         DataType::BLOB], 3);
        
        rb.write_value(Data::Integer(5));
        rb.write_value(Data::Real(5.5));
        rb.write_value(Data::Text(String::from("Hello!")));
        rb.write_value(Data::Blob(vec![89, 23, 95]));
        assert!(!rb.is_full());

        rb.write_value(Data::Integer(6));
        rb.write_value(Data::Real(6.5));
        rb.write_value(Data::Text(String::from("World!")));
        rb.write_value(Data::Blob(vec![5, 27, 95]));
        assert!(!rb.is_full());

        rb.write_value(Data::Integer(7));
        rb.write_value(Data::Real(7.5));
        rb.write_value(Data::Text(String::from("Testing!")));
        rb.write_value(Data::Blob(vec![5, 23, 96]));
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
