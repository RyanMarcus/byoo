use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::VecDeque;
use row_buffer::{RowBuffer};
use data::{Data, DataType};


pub struct OperatorReadBuffer {
    buffers: VecDeque<RowBuffer>,
    send: Sender<RowBuffer>,
    recv: Receiver<RowBuffer>
}

pub struct OperatorWriteBuffer {
    buffers: VecDeque<RowBuffer>,
    send: Sender<RowBuffer>,
    recv: Receiver<RowBuffer>
}

impl OperatorReadBuffer {
    fn new(send: Sender<RowBuffer>, recv: Receiver<RowBuffer>)
           -> OperatorReadBuffer {        
        return OperatorReadBuffer {
            buffers: VecDeque::new(),
            send: send,
            recv: recv
        };
    }

    fn data(&mut self) -> Option<&mut RowBuffer> {
        if self.buffers.is_empty() {
            match self.recv.recv() {
                Ok(r) => { self.buffers.push_back(r); }
                Err(_) => { return None; }
            };
        }

        return Some(self.buffers.front_mut().unwrap());
    }

    fn next(&mut self) {
        if let Some(mut buffer_to_return) = self.buffers.pop_front() {
            buffer_to_return.clear();
            self.send.send(buffer_to_return);
        }
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
            recv: recv
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

    pub fn flush(&mut self) {
        self.send_buffer();
    }

}

fn make_buffer_pair(num_buffers: usize, buffer_size: usize,
                    types: Vec<DataType>)
                    -> (OperatorReadBuffer, OperatorWriteBuffer) {
    let (s_r2w, r_r2w) = channel();
    let (s_w2r, r_w2r) = channel();

    let read = OperatorReadBuffer::new(s_r2w, r_w2r);
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
    fn can_send_and_recv() {
        let (mut r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(-100)]);
        w.flush();

        let read_data = r.data().unwrap();
        assert_eq!(read_data.pop_row()[0], Data::Integer(5));
        assert_eq!(read_data.pop_row()[0], Data::Integer(6));
        assert_eq!(read_data.pop_row()[0], Data::Integer(-100));
    }

    #[test]
    fn can_send_and_recv_multibuf() {
        let (mut r, mut w) = make_buffer_pair(5, 3,
                                              vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(-100)]);
        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(-100)]);
        w.flush();

        drop(w);

        {
            let read_data = r.data().unwrap();
            assert_eq!(read_data.pop_row()[0], Data::Integer(5));
            assert_eq!(read_data.pop_row()[0], Data::Integer(6));
            assert_eq!(read_data.pop_row()[0], Data::Integer(-100));
            assert!(read_data.is_empty());
        }

        r.next();

        {
            let read_data2 = r.data().unwrap();
            assert_eq!(read_data2.pop_row()[0], Data::Integer(5));
            assert_eq!(read_data2.pop_row()[0], Data::Integer(6));
            assert_eq!(read_data2.pop_row()[0], Data::Integer(-100));
        }
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
            loop {
                if let Some(rb) = r.data() {
                    while !rb.is_empty() {
                        for d in rb.pop_row() {
                            if let Data::Integer(i) = d {
                                data.push(i);
                            } else {
                                panic!("Invalid datatype from writer!");
                            }
                        }
                    }
                } else {
                    break;
                }
                
                r.next();
            }

            data
        });


        writer_handler.join().unwrap();
        let data = read_handler.join().unwrap();
        assert_eq!(data.len(), (num_sends*3) as usize);
        
    }

}
