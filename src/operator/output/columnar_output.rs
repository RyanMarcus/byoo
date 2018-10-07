use operator_buffer::OperatorReadBuffer;
use std::io::Write;

struct ColumnarOutput<T> {
    input: OperatorReadBuffer,
    output: T
}

impl <T: Write> ColumnarOutput<T> {
    pub fn new(input: OperatorReadBuffer, output: T) -> ColumnarOutput<T> {
        return ColumnarOutput {
            input, output
        };
    }

    pub fn start(mut self) {
        iterate_buffer!(self.input, row, {

        });
    }
}
