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

pub struct CountAggregate {
    curr_count: usize
}

impl CountAggregate {
    pub fn new(_row_idx: usize) -> CountAggregate {
        return CountAggregate {
            curr_count: 0
        };
    }
}

impl Aggregate for CountAggregate {
    fn consume(&mut self, _row: &[Data]) {
        self.curr_count += 1;
    }

    fn produce(&mut self) -> Data {
        let to_r = self.curr_count;
        self.curr_count = 0;
        return Data::Integer(to_r as i64);
    }

    fn out_type(&self, _in_type: &DataType) -> DataType {
        return DataType::INTEGER;
    }
}

#[cfg(test)]
mod tests {
    use agg::{Aggregate};
    use agg::count::CountAggregate;
    use data::Data;
    
    #[test]
    fn simple_test() {
        let mut count_agg = CountAggregate::new(0);

        let data = vec![
            vec![Data::Integer(5), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
        ];

        for row in data.iter() {
            count_agg.consume(row);
        }

        assert_eq!(count_agg.produce(), Data::Integer(2));
    }

    #[test]
    fn multi_test() {
        let mut count_agg = CountAggregate::new(0);

        let data = vec![
            vec![Data::Integer(500), Data::Integer(-100)],
            vec![Data::Integer(-200), Data::Integer(-100)],
            vec![Data::Integer(10), Data::Integer(-100)],
            vec![Data::Integer(-10), Data::Integer(100)],
            vec![Data::Integer(30), Data::Integer(-100)],
            vec![Data::Integer(-30), Data::Integer(-100)],
        ];

        
        count_agg.consume(&data[0]);
        count_agg.consume(&data[1]);
        count_agg.consume(&data[2]);
        assert_eq!(count_agg.produce(), Data::Integer(3));

        count_agg.consume(&data[3]);
        count_agg.consume(&data[4]);
        count_agg.consume(&data[5]);
        assert_eq!(count_agg.produce(), Data::Integer(3));

    }
}
