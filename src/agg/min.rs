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
use agg::Aggregate;
use data::{Data, DataType};
use std::cmp::Ordering;

pub struct MinAggregate {
    row_idx: usize,
    curr_min: Option<Data>
}

impl MinAggregate {
    pub fn new(row_idx: usize) -> MinAggregate {
        return MinAggregate {
            row_idx,
            curr_min: None
        };
    }
}

impl Aggregate for MinAggregate {
    fn consume(&mut self, row: &[Data]) {
        let nxt = &row[self.row_idx];
        let curr = self.curr_min.take();
        self.curr_min = match curr {
            None => Some(nxt.clone()),
            Some(d) => {
                match d.partial_cmp(nxt).unwrap() {
                    Ordering::Equal => Some(d),
                    Ordering::Less => Some(d),
                    Ordering::Greater => Some(nxt.clone())
                }
            }
        };
    }

    fn produce(&mut self) -> Data {
        return self.curr_min.take().unwrap();
    }

    fn out_type(&self, in_type: &DataType) -> DataType {
        return in_type.clone();
    }
}

#[cfg(test)]
mod tests {
    use agg::{Aggregate};
    use agg::min::MinAggregate;
    use data::Data;
    
    #[test]
    fn simple_test() {
        let mut min_agg = MinAggregate::new(0);

        let data = vec![
            vec![Data::Integer(5), Data::Integer(-100)],
            vec![Data::Integer(-2), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
        ];

        for row in data.iter() {
            min_agg.consume(row);
        }

        assert_eq!(min_agg.produce(), Data::Integer(-2));
    }

    #[test]
    fn multi_test() {
        let mut min_agg = MinAggregate::new(0);

        let data = vec![
            vec![Data::Integer(5), Data::Integer(-100)],
            vec![Data::Integer(-200), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
            vec![Data::Integer(-10), Data::Integer(-100)],
            vec![Data::Integer(-30), Data::Integer(-100)],
            vec![Data::Integer(30), Data::Integer(-100)],
        ];

        
        min_agg.consume(&data[0]);
        min_agg.consume(&data[1]);
        min_agg.consume(&data[2]);
        assert_eq!(min_agg.produce(), Data::Integer(-200));

        min_agg.consume(&data[3]);
        min_agg.consume(&data[4]);
        min_agg.consume(&data[5]);
        assert_eq!(min_agg.produce(), Data::Integer(-30));

    }
}
