use byteorder::{ByteOrder, ReadBytesExt, LittleEndian};
use std::io::{BufRead, Error, ErrorKind};

#[derive(Clone, Debug)]
pub enum DataType {
    INTEGER,
    REAL,
    TEXT,
    BLOB
}

#[derive(Debug, PartialEq, Clone)]
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

    pub fn to_code(&self) -> u16 {
        return match &self {
            DataType::INTEGER => 1,
            DataType::REAL => 2,
            DataType::TEXT => 3,
            DataType::BLOB => 4
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

                if str_buf.len() == 0 {
                    // it was empty, we hit an EOF.
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof, "Could not read string"));
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

    pub fn from_string(&self, data: String) -> Data {
        return match self {
            &DataType::INTEGER => Data::Integer(data.parse::<i64>().unwrap()),
            &DataType::REAL => Data::Real(data.parse::<f64>().unwrap()),
            &DataType::TEXT => Data::Text(data),
            &DataType::BLOB => Data::Blob(data.into_bytes())
        }
    }
}

impl Data {

    pub fn num_bytes(&self) -> usize {
        return match &self {
            Data::Integer(_) => 8,
            Data::Real(_) => 8,
            Data::Text(s) => s.as_bytes().len() + 1,
            Data::Blob(b) => 8 + b.len()
        };
    }
    
    pub fn into_bytes(self) -> Vec<u8> {
        match self {
            Data::Integer(i) => {
                let mut buf = [0; 8];
                LittleEndian::write_i64(&mut buf, i);
                return buf.to_vec();
            },

            Data::Real(f) => {
                let mut buf = [0; 8];
                LittleEndian::write_f64(&mut buf, f);
                return buf.to_vec();
            },

            Data::Text(s) => {
                let mut to_r = s.as_bytes().to_vec();
                to_r.push(0);
                return to_r;
            },

            Data::Blob(b) => {
                let mut buf = [0; 8];
                LittleEndian::write_u64(&mut buf, b.len() as u64);
                let mut to_r = buf.to_vec();
                to_r.extend(b);

                return to_r;
            }
        }
    }
}
