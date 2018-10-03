use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::VecDeque;

type Data = u64;

pub struct OperatorReadBuffer {
    buffers: VecDeque<Vec<Data>>,
    send: Sender<Vec<Data>>,
    recv: Receiver<Vec<Data>>
}

pub struct OperatorWriteBuffer {
    buffers: VecDeque<Vec<Data>>,
    send: Sender<Vec<Data>>,
    recv: Receiver<Vec<Data>>
}

impl OperatorReadBuffer {
    fn new(send: Sender<Vec<Data>>, recv: Receiver<Vec<Data>>)
           -> OperatorReadBuffer {        
        return OperatorReadBuffer {
            buffers: VecDeque::new(),
            send: send,
            recv: recv
        };
    }

    fn data(&mut self) -> Option<&[Data]> {
        if self.buffers.is_empty() {
            match self.recv.recv() {
                Ok(r) => { self.buffers.push_back(r); }
                Err(_) => { return None; }
            };
        }

        return Some(self.buffers.front().unwrap());
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
           send: Sender<Vec<Data>>, recv: Receiver<Vec<Data>>)
           -> OperatorWriteBuffer {

        let mut buffers = VecDeque::new();

        for _ in 0..num_buffers {
            buffers.push_back(Vec::with_capacity(buffer_size));
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

    fn get_remaining_capacity(&self) -> usize {
        if let Some(f) = self.buffers.front() {
            return f.capacity() - f.len();
        }

        return 0;
    }
    
    pub fn write(&mut self, data: &[Data]) {
        self.ensure_buffer();

        let current_capacity = self.get_remaining_capacity();
        if current_capacity < data.len() {
            self.send_buffer();
            self.ensure_buffer();
        }

        self.buffers.front_mut().unwrap().extend_from_slice(data);
    }

    pub fn flush(&mut self) {
        self.send_buffer();
    }

}

fn make_buffer_pair(num_buffers: usize, buffer_size: usize)
                    -> (OperatorReadBuffer, OperatorWriteBuffer) {
    let (s_r2w, r_r2w) = channel();
    let (s_w2r, r_w2r) = channel();

    let read = OperatorReadBuffer::new(s_r2w, r_w2r);
    let write = OperatorWriteBuffer::new(num_buffers, buffer_size,
                                         s_w2r, r_r2w);

    return (read, write);
}

#[cfg(test)]
mod tests {
    use operator_buffer::make_buffer_pair;
    use std::thread;

    #[test]
    fn can_construct() {
        let (_, _) = make_buffer_pair(5, 10);
    }

    #[test]
    fn can_send_and_recv() {
        let (mut r, mut w) = make_buffer_pair(5, 10);

        let data = &vec![5, 6, 7, 8];
        w.write(data);
        w.flush();

        let read_data = r.data().unwrap();
        assert_eq!(read_data, &data[..]);
    }

    #[test]
    fn can_send_and_recv_multibuf() {
        let (mut r, mut w) = make_buffer_pair(5, 9);

        let data1 = &vec![5, 6, 7, 8];
        let data2 = &vec![1, 3, 4, 9];
        w.write(data1);
        w.write(data2);
        w.write(data1);
        w.write(data2);
        w.flush();

        drop(w);

        let mut data = Vec::new();
        loop {
            if let Some(chunk) = r.data() {
                data.extend_from_slice(chunk);
            } else {
                break;
            }
            
            r.next();
        }

        
        assert_eq!(data, [5,6,7,8,1,3,4,9,
                          5,6,7,8,1,3,4,9]);
    }

    #[test]
    fn thread_test() {
        let num_sends = 100000;
        let (mut r, mut w) = make_buffer_pair(5, 2);

        // spawn a writer
        let writer_handler = thread::spawn(move || {
            for idx in 0..num_sends {
                let data = vec![idx, idx + 1, idx + 2];
                w.write(&data);
            }

            w.flush();
        });

        let read_handler = thread::spawn(move || {
            let mut data = Vec::new();
            loop {
                if let Some(chunk) = r.data() {
                    data.extend_from_slice(chunk);
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
