use byteorder::{ReadBytesExt, LittleEndian};
use std::io::{BufRead, Error};

#[derive(Clone)]
pub enum DataType {
    INTEGER,
    REAL,
    TEXT,
    BLOB
}

#[derive(Debug, PartialEq)]
pub enum Data {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>)
}

impl DataType {
    pub fn from_code(code: u16) -> DataType {
        return match code {
            1 => DataType::INTEGER,
            2 => DataType::REAL,
            3 => DataType::TEXT,
            4 => DataType::BLOB,
            _ => { panic!("Unknown datatype code"); }
        };
    }
    
    pub fn read_item<T: BufRead>(&self, reader: &mut T) -> Result<Data, Error> {
        match self {
            &DataType::INTEGER => {
                return reader.read_i64::<LittleEndian>()
                    .map(|d| Data::Integer(d));
            },
            &DataType::REAL => {
                return reader.read_f64::<LittleEndian>()
                    .map(|d| Data::Real(d));
            },
            &DataType::TEXT => {
                let mut str_buf = Vec::new();
                if let Err(e) = reader.read_until(0, &mut str_buf) {
                    return Err(e);
                }

                // pop off the null
                str_buf.pop();

                return Ok(Data::Text(String::from_utf8(str_buf).unwrap()));
            },
            &DataType::BLOB => {
                let blob_length = reader.read_u64::<LittleEndian>()? as usize;
                    
                let mut data = Vec::with_capacity(blob_length);

                // TODO buffer this, don't read byte by byte
                for _ in 0..blob_length {
                    data.push(reader.read_u8()?);
                    
                }

                return Ok(Data::Blob(data));
            }
        };
    }
}
