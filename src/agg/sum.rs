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
use data::{DataType, Data};

pub struct SumAggregate {
    row_idx: usize,
    curr_sum: Option<Data>
}

impl SumAggregate {
    pub fn new(row_idx: usize) -> SumAggregate {
        return SumAggregate {
            row_idx,
            curr_sum: None
        };
    }
}

impl Aggregate for SumAggregate {
    fn consume(&mut self, row: &[Data]) {
        let nxt = &row[self.row_idx];
        let curr = self.curr_sum.take();
        self.curr_sum = match curr {
            None => Some(nxt.clone()),
            Some(d) => Some(d + nxt.clone())
        };
    }

    fn produce(&mut self) -> Data {
        return self.curr_sum.take().unwrap();
    }

    fn out_type(&self, in_type: &DataType) -> DataType {
        return in_type.clone();
    }
}

#[cfg(test)]
mod tests {
    use agg::{Aggregate};
    use data::Data;
    use agg::sum::SumAggregate;
    
    #[test]
    fn simple_test() {
        let mut sum_agg = SumAggregate::new(0);

        let data = vec![
            vec![Data::Integer(5), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
        ];

        for row in data.iter() {
            sum_agg.consume(row);
        }

        assert_eq!(sum_agg.produce(), Data::Integer(15));
    }

    #[test]
    fn simple_float_test() {
        let mut sum_agg = SumAggregate::new(0);

        let data = vec![
            vec![Data::Real(5.5), Data::Real(-100.0)],
            vec![Data::Real(10.0), Data::Real(-100.0)],
        ];

        for row in data.iter() {
            sum_agg.consume(row);
        }

        assert_eq!(sum_agg.produce(), Data::Real(15.5));
    }

    #[test]
    fn multi_test() {
        let mut sum_agg = SumAggregate::new(0);

        let data = vec![
            vec![Data::Integer(500), Data::Integer(-100)],
            vec![Data::Integer(-200), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
            vec![Data::Integer(-10), Data::Integer(100)],
            vec![Data::Integer(30), Data::Integer(-100)],
            vec![Data::Integer(-30), Data::Integer(-100)],
        ];

        
        sum_agg.consume(&data[0]);
        sum_agg.consume(&data[1]);
        sum_agg.consume(&data[2]);
        assert_eq!(sum_agg.produce(), Data::Integer(310));

        sum_agg.consume(&data[3]);
        sum_agg.consume(&data[4]);
        sum_agg.consume(&data[5]);
        assert_eq!(sum_agg.produce(), Data::Integer(-10));

    }
}
