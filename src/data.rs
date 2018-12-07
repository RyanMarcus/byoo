use byteorder::{ByteOrder, ReadBytesExt, LittleEndian};
use std::io::{BufRead, Error, ErrorKind};
use base64;
use std::cmp::Ordering;
use std::{fmt, ops};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Hash)]
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

    pub fn from_string_code(code: &str) -> DataType {
        match code {
            "INTEGER" => DataType::INTEGER,
            "TEXT" => DataType::TEXT,
            "REAL" => DataType::REAL,
            "BLOB" => DataType::BLOB,
            _ => panic!("unknown datatype string")
        }
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

    pub fn into_string(self) -> String {
        match self {
            Data::Integer(i) => i.to_string(),
            Data::Real(f) => f.to_string(),
            Data::Text(t) => t,
            Data::Blob(b) => base64::encode(&b)
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            Data::Integer(i) => i.to_string(),
            Data::Real(f) => f.to_string(),
            Data::Text(t) => t.clone(),
            Data::Blob(b) => base64::encode(&b)
        }
    }
}

impl PartialOrd for Data {
    fn partial_cmp(&self, other: &Data) -> Option<Ordering> {
        match &self {
            Data::Integer(me) => {
                if let Data::Integer(other) = other {
                    return Some(me.cmp(other));
                } else {
                    return None;
                }
            },

            Data::Real(me) => {
                if let Data::Real(other) = other {
                    // define NAN as greater than all other values
                    if me.is_nan() {
                        if other.is_nan() {
                            return Some(Ordering::Equal);
                        } else {
                            return Some(Ordering::Greater);
                        }
                    }

                    if other.is_nan() {
                        return Some(Ordering::Less);
                    }

                    return Some(me.partial_cmp(other).unwrap());
                    
                } else {
                    return None;
                }
            },

            Data::Text(me) => {
                if let Data::Text(other) = other {
                    return Some(me.cmp(other));
                } else {
                    return None;
                }
            },

            Data::Blob(me) => {
                if let Data::Blob(other) = other {
                    return Some(me.cmp(other));
                } else {
                    return None;
                }
            }
        }
    }
}

impl Eq for Data { }

impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

impl Hash for Data {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Data::Integer(me) => me.hash(state),
            Data::Real(me) => self.clone().into_bytes().hash(state),
            Data::Text(me) => me.hash(state),
            Data::Blob(me) => me.hash(state)
        };
    }
}

impl ops::Add<Data> for Data {
    type Output = Data;

    fn add(self, rhs: Data) -> Data {
        match self {
            Data::Integer(me) => {
                if let Data::Integer(other) = rhs {
                    return Data::Integer(me + other);
                }
            },

            Data::Real(me) => {
                if let Data::Real(other) = rhs {
                    return Data::Real(me + other);
                }
            },

            _ => {}
        };

        panic!("Incompatible data types for sum operator");
    }
}

impl ops::Sub<Data> for Data {
    type Output = Data;

    fn sub(self, rhs: Data) -> Data {
        match self {
            Data::Integer(me) => {
                if let Data::Integer(other) = rhs {
                    return Data::Integer(me - other);
                }
            },

            Data::Real(me) => {
                if let Data::Real(other) = rhs {
                    return Data::Real(me - other);
                }
            },

            _ => {}
        };

        panic!("Incompatible data types for sub operator");
    }
}

impl ops::Div<usize> for Data {
    type Output = Data;

    fn div(self, rhs: usize) -> Data {
        match self {
            Data::Integer(me) => {
                return Data::Real(me as f64 / rhs as f64);
            },

            Data::Real(me) => {
                return Data::Real(me / rhs as f64);
            },

            _ => {}
        };

        panic!("Incompatible data types for div operator");
    }
}




pub fn rows_to_string(rows: &[Vec<Data>], sort: bool) -> String {
    let mut to_r = Vec::new();

    for row in rows {
        let mut buf = Vec::new();
        for col in row.iter() {
            buf.push(col.to_string() + " ");
        }
        to_r.push(buf.join(" "));
    }
    
    if sort {
        to_r.sort();
    }

    return to_r.join("\n");
}

