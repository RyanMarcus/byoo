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


#[cfg(test)]
mod tests {
    use byoo;
    use byoo::Data;

    #[test]
    fn simple_plan() {
        
        let json = String::from(r#"
{
    "op": "project",
    "options": {
        "cols": [0, 1, 3]
    },

    "input": [
        { "op": "csv read",
          "options": {
              "file": "res/inputs/test1.csv",
              "types": ["INTEGER", "INTEGER", "TEXT", "INTEGER", "REAL"]
          }
        }
    ]
}
"#);

        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.into_vec();

        assert_eq!(data_vec.len(), 1000);

        // check the first and last rows
        let r_f = vec![Data::Integer(-7119), Data::Integer(8430), Data::Integer(8771)];
        let r_l = vec![Data::Integer(-4240), Data::Integer(2604), Data::Integer(-9236)];

        assert_eq!(data_vec[0], r_f);
        assert_eq!(data_vec[data_vec.len()-1], r_l);
    }

    #[test]
    fn simple_filter_plan() {
        
        let json = String::from(r#"
{
    "op": "project",
    "options": {
        "cols": [0, 1, 3]
    },
    "input": [{
        "op": "filter",
        "options": {
            "predicate": {"op": "lt", "col": 0, "val": 100}
        },
        
        "input": [
            { "op": "csv read",
              "options": {
                  "file": "res/inputs/test1.csv",
                  "types": ["INTEGER", "INTEGER", "TEXT", "INTEGER", "REAL"]
              }
            }
        ]
    }]
}

"#);

        let root = byoo::compile(json);
        let (read_buf, _) = root.start_save();

        let data_vec = read_buf.into_vec();

        assert_eq!(data_vec.len(), 495);

        // check the first and last rows
        let r_f = vec![Data::Integer(-7119), Data::Integer(8430), Data::Integer(8771)];
        let r_l = vec![Data::Integer(-4240), Data::Integer(2604), Data::Integer(-9236)];

        assert_eq!(data_vec[0], r_f);
        assert_eq!(data_vec[data_vec.len()-1], r_l);
    }
}
