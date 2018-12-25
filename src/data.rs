// < begin copyright > 
// Copyright Ryan Marcus 2018
// 
// This file is part of byoo.
// 
// byoo is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// byoo is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with byoo.  If not, see <http://www.gnu.org/licenses/>.
// 
// < end copyright > 
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use std::io::{BufRead, Error, ErrorKind, Result};
use std::io;
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

#[derive(Debug, Clone)]
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

    pub fn from_string(&self, data: String) -> Option<Data> {
        return match *self {
            DataType::INTEGER => {
                data.parse::<i64>().ok()
                    .map(|i| Data::Integer(i))
            },
            DataType::REAL => {
                data.parse::<f64>().ok()
                    .map(|f| Data::Real(f))
            }
            DataType::TEXT => Some(Data::Text(data)),
            DataType::BLOB => Some(Data::Blob(data.into_bytes()))
        }
    }

    pub fn default_value(&self) -> Data {
        return match *self {
            DataType::INTEGER => Data::Integer(0),
            DataType::REAL => Data::Real(0.0),
            DataType::TEXT => Data::Text("".to_string()),
            DataType::BLOB => Data::Blob(vec![])
        }
    }
}

impl Data {

    pub fn as_f64(&self) -> f64 {
        return match &self {
            Data::Real(d) => *d,
            _ => panic!("as_f64() on non-real data item")
        };
    }

    pub fn as_i64(&self) -> i64 {
        return match &self {
            Data::Integer(d) => *d,
            _ => panic!("as_i64() on non-integer data item")
        };
    }

    pub fn num_bytes(&self) -> usize {
        return match &self {
            Data::Integer(_) => 8,
            Data::Real(_) => 8,
            Data::Text(s) => s.as_bytes().len() + 1,
            Data::Blob(b) => 8 + b.len()
        };
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

pub trait WriteByooDataExt: io::Write {
    fn write_data(&mut self, data: &Data) -> Result<()> {
        match *data {
            Data::Integer(i) => self.write_i64::<LittleEndian>(i),
            Data::Real(f) => self.write_f64::<LittleEndian>(f),
            Data::Text(ref s) => {
                self.write_all(s.as_bytes())?;
                self.write_all(&[0])
            },
                    
            Data::Blob(ref b) => {
                self.write_u64::<LittleEndian>(b.len() as u64)?;
                self.write_all(b)
            }
        }
    }
}
impl<W: io::Write + ?Sized> WriteByooDataExt for W {}

pub trait ReadByooDataExt: io::BufRead {
    fn read_data(&mut self, data_type: &DataType) -> Result<Data> {
        match data_type {
            DataType::INTEGER => {
                return self.read_i64::<LittleEndian>()
                    .map(Data::Integer);
            },
            DataType::REAL => {
                return self.read_f64::<LittleEndian>()
                    .map(Data::Real);
            },
            DataType::TEXT => {
                let mut str_buf = Vec::new();
                if let Err(e) = self.read_until(0, &mut str_buf) {
                    return Err(e);
                }

                if str_buf.is_empty() {
                    // it was empty, we hit an EOF.
                    return Err(Error::new(
                        ErrorKind::UnexpectedEof, "Could not read string"));
                }
                
                // pop off the null
                assert_eq!(str_buf.pop().unwrap(), 0);

                return Ok(Data::Text(String::from_utf8(str_buf).unwrap()));
            },
            DataType::BLOB => {
                let blob_length = self.read_u64::<LittleEndian>()? as usize;
                
                let mut data = Vec::with_capacity(blob_length);

                // TODO buffer this, don't read byte by byte
                for _ in 0..blob_length {
                    data.push(self.read_u8()?);
                    
                }

                return Ok(Data::Blob(data));
            }
        };
        
    }
}
impl<R: io::BufRead + ?Sized> ReadByooDataExt for R {}

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

impl PartialEq<Data> for Data {
    fn eq(&self, other: &Data) -> bool {
        let partial_cmp = self.partial_cmp(other);

        if partial_cmp.is_none() { return false; }
        if let Ordering::Equal = partial_cmp.unwrap() { return true; }
        return false;
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
            Data::Real(me) => (*me as i64).hash(state),
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
                match rhs {
                    Data::Integer(other) => return Data::Integer(me + other),
                    Data::Real(other) => return Data::Real(me as f64 + other),
                    _ => {}
                };
            },

            Data::Real(me) => {
                match rhs {
                    Data::Integer(other) => return Data::Real(me + (other as f64)),
                    Data::Real(other) => return Data::Real(me + other),
                    _ => {}
                };
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
                match rhs {
                    Data::Integer(other) => return Data::Integer(me - other),
                    Data::Real(other) => return Data::Real(me as f64 - other),
                    _ => {}
                };
            },

            Data::Real(me) => {
                match rhs {
                    Data::Integer(other) => return Data::Real(me - (other as f64)),
                    Data::Real(other) => return Data::Real(me - other),
                    _ => {}
                };
            },

            _ => {}
        };

        panic!("Incompatible data types for sub operator, got: {:?} - {:?}",
               self, rhs);
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

#[cfg(test)]
mod tests {
    use data::Data;

    #[test]
    fn add_data() {
        assert_eq!(Data::Integer(5) + Data::Integer(-2), Data::Integer(3));
        assert_eq!(Data::Real(5.0) + Data::Real(-2.0), Data::Real(3.0));
        assert_eq!(Data::Integer(5) + Data::Real(-2.0), Data::Real(3.0));
    }

    #[test]
    fn sub_data() {
        assert_eq!(Data::Integer(5) - Data::Integer(-2), Data::Integer(7));
        assert_eq!(Data::Integer(5) - Data::Real(-2.0), Data::Real(7.0));
        assert_eq!(Data::Real(5.0) - Data::Real(-2.0), Data::Real(7.0));
    }

    #[test]
    fn div_data() {
        assert_eq!(Data::Integer(5) / 2, Data::Real(2.5));
        assert_eq!(Data::Real(5.0) / 2, Data::Real(2.5));
    }
}

