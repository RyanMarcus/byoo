use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::VecDeque;
use row_buffer::{RowBuffer};
use data::{Data, DataType};


pub struct OperatorReadBuffer {
    buffers: VecDeque<RowBuffer>,
    send: Sender<RowBuffer>,
    recv: Receiver<RowBuffer>,
    types: Vec<DataType>
}

pub struct OperatorWriteBuffer {
    buffers: VecDeque<RowBuffer>,
    send: Sender<RowBuffer>,
    recv: Receiver<RowBuffer>,
    types: Vec<DataType>
}

pub struct PeekableOperatorReadBuffer {
    read_buf: OperatorReadBuffer,
    dq: VecDeque<Vec<Data>>
}

macro_rules! iterate_buffer {
    ($op_buf:expr, $row_var:ident, $loop_body: block) => {
        loop {
            {
                let next_rb = match ($op_buf).data() {
                    Some(rb) => rb,
                    None => { break; }
                };

                for $row_var in next_rb.iter() {
                    $loop_body
                }
            }
            ($op_buf).progress();
        }
    };
    ($op_buf:expr, $idx_var:ident, $row_var:ident, $loop_body: block) => {
        let mut count = 0;
        loop {
            {
                let next_rb = match ($op_buf).data() {
                    Some(rb) => rb,
                    None => { break; }
                };
                for $row_var in next_rb.iter() {
                    let $idx_var = count;
                    $loop_body;
                    count += 1;
                }
            }
            ($op_buf).progress();
        }
    };
}


impl OperatorReadBuffer {
    fn new(send: Sender<RowBuffer>, recv: Receiver<RowBuffer>, types: Vec<DataType>)
           -> OperatorReadBuffer {        
        return OperatorReadBuffer {
            buffers: VecDeque::new(),
            send,
            recv,
            types
                
        };
    }

    pub fn data(&mut self) -> Option<&mut RowBuffer> {
        if self.buffers.is_empty() {
            match self.recv.recv() {
                Ok(r) => { self.buffers.push_back(r); }
                Err(_) => { return None; }
            };
        }

        return Some(self.buffers.front_mut().unwrap());
    }

    pub fn progress(&mut self) {
        if let Some(mut buffer_to_return) = self.buffers.pop_front() {
            buffer_to_return.clear();
            self.send.send(buffer_to_return);
        }
    }

    pub fn types(&self) -> &[DataType] {
        return &self.types;
    }

    pub fn to_vec(mut self) -> Vec<Vec<Data>> {
        let mut to_r = Vec::new();
        iterate_buffer!(self, row, {
            to_r.push(row.to_vec());
        });

        return to_r;
    }
}


impl PeekableOperatorReadBuffer {
    pub fn new(read_buf: OperatorReadBuffer) -> PeekableOperatorReadBuffer {
        let mut to_r = PeekableOperatorReadBuffer {
            read_buf,
            dq: VecDeque::new()
        };

        to_r.load_next_block();

        return to_r;
    }

    fn load_next_block(&mut self) {
        if let Some(rb) = self.read_buf.data() {
            for row in rb.iter() {
                self.dq.push_back(row.to_vec());
            }
        }

        if !self.dq.is_empty() {
            self.read_buf.progress();
        }
    }

    pub fn peek(&self) -> Option<&Vec<Data>> {
        if self.dq.is_empty() {
            return None;
        }

        return Some(&self.dq[0]);
    }

    pub fn pop(&mut self) -> Option<Vec<Data>> {
        let to_r = self.dq.pop_front();

        if self.dq.is_empty() {
            self.load_next_block();
        }

        return to_r;
    }
}

impl OperatorWriteBuffer {
    fn new(num_buffers: usize, buffer_size: usize,
           types: Vec<DataType>,
           send: Sender<RowBuffer>, recv: Receiver<RowBuffer>)
           -> OperatorWriteBuffer {

        let mut buffers = VecDeque::new();

        for _ in 0..num_buffers {
            buffers.push_back(RowBuffer::new(types.clone(), buffer_size));
        }

        
        return OperatorWriteBuffer {
            buffers: buffers,
            send: send,
            recv: recv,
            types: types
        };
    }

    fn ensure_buffer(&mut self) {
        if self.buffers.is_empty() {
            // we must wait until we get a buffer back!
            self.buffers.push_back(self.recv.recv().unwrap());
        }   
    }

    fn send_buffer(&mut self) {
        if let Some(buf) = self.buffers.pop_front() {
            if buf.is_empty() {
                // buffer was empty, no need to send it
                self.buffers.push_back(buf);
                return;
            }
            self.send.send(buf).unwrap();
        }
    }

    fn have_full_front(&self) -> bool {
        if let Some(b) = self.buffers.front() {
            return b.is_full();
        } else {
            return false;
        }
    }
    
    pub fn write(&mut self, row: Vec<Data>) {
        self.ensure_buffer();

        if self.have_full_front() {
            self.send_buffer();
            self.ensure_buffer();
        }

        self.buffers.front_mut().unwrap().write_values(row);
    }

    pub fn write_many(&mut self, rows: Vec<Data>) {
        // ensure this is a valid number of data points
        assert!(rows.len() % self.types.len() == 0);

        // TODO candidate for optimization, since this will do multiple
        // copies
        for row in rows.chunks(self.types.len()) {
            self.write(row.to_vec());
        }
    }

    pub fn write_strings(&mut self, row: Vec<String>) {
        assert_eq!(row.len(), self.types.len(),
                   "Was expecting {} columns in operator but write_strings got {}",
                   self.types.len(), row.len());
        
        let data: Vec<Data> = row.into_iter().enumerate().map(|(idx, field)| {
            let dt = &self.types[idx];
            return dt.from_string(field);
        }).collect();

        self.write(data);
    }

    pub fn flush(&mut self) {
        self.send_buffer();
    }

}

impl Drop for OperatorWriteBuffer {
    fn drop(&mut self) {
        self.flush();
    }
}

pub fn make_buffer_pair(num_buffers: usize, buffer_size: usize,
                        types: Vec<DataType>)
                        -> (OperatorReadBuffer, OperatorWriteBuffer) {
    let (s_r2w, r_r2w) = channel();
    let (s_w2r, r_w2r) = channel();

    let read = OperatorReadBuffer::new(s_r2w, r_w2r, types.clone());
    let write = OperatorWriteBuffer::new(num_buffers, buffer_size,
                                         types,
                                         s_w2r, r_r2w);

    return (read, write);
}

#[cfg(test)]
mod tests {
    use operator_buffer::make_buffer_pair;
    use std::thread;
    use data::{Data, DataType};

    #[test]
    fn can_construct() {
        let (_, _) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
    }

    #[test]
    fn can_use_iter() {
        let (mut r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(-100)]);
        w.flush();
        drop(w);

        iterate_buffer!(r, idx, row, {
            match idx {
                0 => { assert_eq!(row[0], Data::Integer(5)); }
                1 => { assert_eq!(row[0], Data::Integer(6)); }
                2 => { assert_eq!(row[0], Data::Integer(-100)); }
                _ => { panic!("Too many values!"); }
            }
        });
    }

    #[test]
    fn can_send_and_recv_multibuf() {
        let (mut r, mut w) = make_buffer_pair(5, 3,
                                              vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(-100)]);
        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(7)]);
        w.write(vec![Data::Integer(-100)]);
        w.flush();

        drop(w);

        iterate_buffer!(r, idx, row, {
            match idx {
                0 => { assert_eq!(row[0], Data::Integer(5)); }
                1 => { assert_eq!(row[0], Data::Integer(6)); }
                2 => { assert_eq!(row[0], Data::Integer(-100)); }
                3 => { assert_eq!(row[0], Data::Integer(5)); }
                4 => { assert_eq!(row[0], Data::Integer(7)); }
                5 => { assert_eq!(row[0], Data::Integer(-100)); }
                _ => { panic!("Too many values!"); }
            }
        });
    }

    
    #[test]
    fn thread_test() {
        let num_sends = 100000;
        let (mut r, mut w) = make_buffer_pair(5, 10,
                                              vec![DataType::INTEGER,
                                                   DataType::INTEGER,
                                                   DataType::INTEGER]);

        // spawn a writer
        let writer_handler = thread::spawn(move || {
            for idx in 0..num_sends {
                let data = vec![Data::Integer(idx),
                                Data::Integer(idx + 1),
                                Data::Integer(idx + 2)];
                w.write(data);
            }

            w.flush();
        });

        let read_handler = thread::spawn(move || {
            let mut data = Vec::new();

            iterate_buffer!(r, row, {
                for d in row {
                    if let Data::Integer(i) = row[0] {
                        data.push(i);
                    } else {
                        panic!("Invalid datatype from writer!");
                    } 
                }
            });
            data
        });


        writer_handler.join().unwrap();
        let data = read_handler.join().unwrap();
        assert_eq!(data.len(), (num_sends*3) as usize);
        
    }
     

}
