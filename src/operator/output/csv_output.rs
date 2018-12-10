use operator_buffer::{OperatorWriteBuffer, OperatorReadBuffer};
use std::io::{Write, BufWriter};
use std::fs::File;
use csv::Writer;
use operator::ConstructableOperator;
use serde_json;

pub struct CsvOutput<T> {
    input: OperatorReadBuffer,
    output: T,
    headers: Vec<String>
}

impl <T: Write> CsvOutput<T> {
    pub fn new(input: OperatorReadBuffer, output: T, headers: Vec<String>) -> CsvOutput<T> {
        return CsvOutput {
            input, output, headers
        };
    }
    
    pub fn start(mut self) {
        let mut csv = Writer::from_writer(self.output);

        if self.headers.len() != 0 {
            csv.write_record(self.headers).unwrap();
        }
        
        iterate_buffer!(self.input, row, {
            let row_strs: Vec<String> = row.into_iter().cloned()
                .map(|d| d.into_string()).collect();
            csv.write_record(row_strs).unwrap();
        });
    }
}

impl ConstructableOperator for CsvOutput<BufWriter<File>> {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    _options: serde_json::Value) -> Self {

        assert!(output.is_none());
        let f = file.unwrap();

        let mut inp = input;
        let inp_v = inp.remove(0);
        assert!(inp.is_empty());

        return CsvOutput::new(inp_v, BufWriter::new(f), vec![]);
    }
    
}

#[cfg(test)]
mod tests {
    use operator::output::CsvOutput;
    use operator_buffer::{make_buffer_pair};
    use data::{Data,DataType};

    
    #[test]
    fn writes_single_col() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(7)]);
        drop(w);

        let mut output_data = Vec::new();

        {
            let co = CsvOutput::new(r, &mut output_data, vec![]);
            co.start();
        }

        let str_data = String::from_utf8(output_data).unwrap();

        assert_eq!(str_data, "5\n6\n7\n");
    }

        #[test]
    fn writes_multi_col() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER,
                                                      DataType::TEXT]);

        w.write(vec![Data::Integer(5), Data::Text(String::from("hello"))]);
        w.write(vec![Data::Integer(6), Data::Text(String::from("world!"))]);
        w.write(vec![Data::Integer(7), Data::Text(String::from("test,test"))]);
        drop(w);

        let mut output_data = Vec::new();

        {
            let co = CsvOutput::new(r, &mut output_data, vec![String::from("int"),
                                                              String::from("text")]);
            co.start();
        }

        let str_data = String::from_utf8(output_data).unwrap();

        assert_eq!(str_data, "int,text\n5,hello\n6,world!\n7,\"test,test\"\n");
    }
}
