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

pub struct AverageAggregate {
    row_idx: usize,
    curr_avg: Option<Data>,
    curr_count: usize
}

impl AverageAggregate {
    pub fn new(row_idx: usize) -> AverageAggregate {
        return AverageAggregate {
            row_idx,
            curr_avg: None,
            curr_count: 0
        };
    }
}

impl Aggregate for AverageAggregate {
    fn consume(&mut self, row: &[Data]) {
        let nxt = &row[self.row_idx];
        let curr = self.curr_avg.take();
        self.curr_count += 1;
        self.curr_avg = match curr {
            None => Some(nxt.clone()),
            Some(ref d) => Some(d.clone() + (nxt.clone() - d.clone())
                                / self.curr_count)
        };
    }

    fn produce(&mut self) -> Data {
        self.curr_count = 0;
        return self.curr_avg.take().unwrap_or(Data::Real(0.0));
    }

    fn out_type(&self, _in_type: &DataType) -> DataType {
        return DataType::REAL;
    }
}

#[cfg(test)]
mod tests {
    use agg::{Aggregate};
    use agg::average::AverageAggregate;
    use data::Data;
    
    #[test]
    fn simple_test() {
        let mut avg_agg = AverageAggregate::new(0);

        let data = vec![
            vec![Data::Integer(5), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
        ];

        for row in data.iter() {
            avg_agg.consume(row);
        }

        assert_eq!(avg_agg.produce(), Data::Real(15.0/2.0));
    }

    #[test]
    fn simple_float_test() {
        let mut avg_agg = AverageAggregate::new(0);

        let data = vec![
            vec![Data::Real(5.5), Data::Real(-100.0)],
            vec![Data::Real(10.0), Data::Real(-100.0)],
        ];

        for row in data.iter() {
            avg_agg.consume(row);
        }

        assert_eq!(avg_agg.produce(), Data::Real(15.5 / 2.0));
    }

    #[test]
    fn multi_test() {
        let mut avg_agg = AverageAggregate::new(0);

        let data = vec![
            vec![Data::Integer(500), Data::Integer(-100)],
            vec![Data::Integer(-200), Data::Integer(-100)],
            vec![Data::Integer(12), Data::Integer(-100)],
            vec![Data::Integer(-10), Data::Integer(100)],
            vec![Data::Integer(28), Data::Integer(-100)],
            vec![Data::Integer(-30), Data::Integer(-100)],
        ];

        
        avg_agg.consume(&data[0]);
        avg_agg.consume(&data[1]);
        avg_agg.consume(&data[2]);
        assert_eq!(avg_agg.produce(), Data::Real(312.0 / 3.0));

        avg_agg.consume(&data[3]);
        avg_agg.consume(&data[4]);
        avg_agg.consume(&data[5]);
        assert_eq!(avg_agg.produce(), Data::Real(-12.0 / 3.0));

    }
}
