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
extern crate byoo;
extern crate tempfile;

#[macro_use]
extern crate serde_json;

#[cfg(test)]
mod tests {
    use byoo;
    use tempfile::NamedTempFile;
    use std::fs;


    #[test]
    fn csv_to_columnar() {
        let file = NamedTempFile::new().unwrap();

        let create_json = json!(
            { "op": "columnar out",
               "options": { "file": file.path().to_string_lossy() },
               "input": [{
                   "op": "csv read",
                   "options": {
                       "file": "res/inputs/test1.csv",
                       "types": ["INTEGER", "INTEGER", "TEXT", "INTEGER", "REAL"]
                   }
               }]
            }
        );

        let root = byoo::compile(create_json.to_string());
        root.start().join().unwrap();

        // make sure something got written...
        assert!(fs::metadata(file.path()).unwrap().len() > 10);

        let read_json = json!(
            { "op": "union",
               "input": [
                   { "op": "columnar read",
                      "options": {
                          "file": file.path().to_string_lossy(),
                          "col": 0,
                          "type": "INTEGER"
                      }
                   },
                   { "op": "columnar read",
                      "options": {
                          "file": file.path().to_string_lossy(),
                          "col": 1,
                          "type": "INTEGER"
                      }
                   },
                   { "op": "columnar read",
                      "options": {
                          "file": file.path().to_string_lossy(),
                          "col": 2,
                          "type": "TEXT"
                      }
                   },
                   { "op": "columnar read",
                      "options": {
                          "file": file.path().to_string_lossy(),
                          "col": 3,
                          "type": "INTEGER"
                      }
                   },
                   { "op": "columnar read",
                      "options": {
                          "file": file.path().to_string_lossy(),
                          "col": 4,
                          "type": "REAL"
                      }
                   }
               ]
            }
        );

        let root2 = byoo::compile(read_json.to_string());
        let (rdr, jh) = root2.start_save();

        let data = rdr.into_vec();
        jh.join().unwrap();

        let root_check = byoo::compile(json!(
            {"op": "csv read",
             "options": {
                 "file": "res/inputs/test1.csv",
                 "types": ["INTEGER", "INTEGER", "TEXT", "INTEGER", "REAL"]
             }
            }).to_string());

        let (rdr2, jh2) = root_check.start_save();
        let data2 = rdr2.into_vec();
        jh2.join().unwrap();

        assert_eq!(data, data2);
    }
}
