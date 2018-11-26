use data::Data;

trait Aggregate {
    fn new(rowIdx: usize) -> Self;
    fn consume(&mut self, row: &[Data]);
    fn produce(&mut self) -> Data;
}

mod min;
pub use self::min::MinAggregate;

mod max;
pub use self::max::MaxAggregate;

mod count;
pub use self::count::CountAggregate;
