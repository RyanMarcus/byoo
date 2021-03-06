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
use predicate::Predicate;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::collections::VecDeque;
use row_buffer::{RowBuffer};
use data::{Data, DataType};
use std::io::{Write, Error};


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
    types: Vec<DataType>,
    filters: Vec<Predicate>,
    projection: Option<Vec<usize>>
}

pub struct PeekableOperatorReadBuffer {
    read_buf: OperatorReadBuffer,
    curr_idx: usize,
    dq: VecDeque<RowBuffer>
}

#[macro_export]
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
        drop($op_buf);
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
        drop($op_buf);
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
            // if there is an error, it must be a SendError,
            // meaning that the sending operator has finished and
            // doesn't want the buffer back.
            if let Err(e) = self.send.send(buffer_to_return) { drop(e); }
        }
    }

    pub fn types(&self) -> &[DataType] {
        return &self.types;
    }

    pub fn into_vec(mut self) -> Vec<Vec<Data>> {
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
            curr_idx: 0,
            dq: VecDeque::new()
        };

        to_r.load_next_block();

        return to_r;
    }

    fn load_next_block(&mut self) {
        if let Some(rb) = self.read_buf.data() {
            self.dq.push_back(rb.into_copy());
        }

        if !self.dq.is_empty() {
            self.read_buf.progress();
        }

    }

    pub fn peek(&self) -> Option<&[Data]> {
        if self.dq.is_empty() {
            return None;
        }

        return Some(self.dq[0].get_row(self.curr_idx));
    }


    pub fn pop(&mut self) -> Option<Vec<Data>> {
        if self.dq.len() == 0 { return None; }
        
        let to_r = {
            let row = self.dq[0].get_row(self.curr_idx);
            Some(row.to_vec())
        };
        self.curr_idx += 1;

        if self.curr_idx >= self.dq[0].num_rows() {
            // we've hit the end of a rowbuffer.
            self.curr_idx = 0;
            self.dq.pop_front();
            
            if self.dq.is_empty() {
                self.load_next_block();
            }
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
            buffers, send, recv, types,
            filters: vec![],
            projection: None
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

    fn prepare_for_write(&mut self) {
         self.ensure_buffer();

        if self.have_full_front() {
            self.send_buffer();
            self.ensure_buffer();
        }
    }

    pub fn add_filter(&mut self, filter: Predicate) {
        self.filters.push(filter);
    }

    pub fn set_projection(&mut self, cols: Vec<usize>) {
        self.projection = Some(cols)
    }

    pub fn write_single_col(&mut self, row: Data) {
        debug_assert_eq!(self.types.len(), 1);
        self.prepare_for_write();
        {
            let acc = |idx: usize| {
                debug_assert!(idx == 0);
                return &row
            };
            
            if !self.filters.iter().all(|p| p.eval_with_accessor(&acc)) {
                return;
            }
        }

        self.buffers.front_mut().unwrap()
            .write_single_col(row);
    }
    
    pub fn write(&mut self, row: Vec<Data>) {
        if !self.filters.iter().all(|p| p.eval(&row)) {
            // don't write the row.
            return;
        }

        self.prepare_for_write();
        if let Some(ref cols) = self.projection {
            let projected_row = cols.iter()
                .map(|&col_idx| row[col_idx].clone())
                .collect();
            
            self.buffers.front_mut().unwrap()
                .write_values(projected_row);
        } else {
            self.buffers.front_mut().unwrap()
                .write_values(row);
        }
    }

    pub fn copy_and_write(&mut self, row: &[Data]) {
        if !self.filters.iter().all(|p| p.eval(&row)) {
            // don't write the row.
            return;
        }

        self.prepare_for_write();
        if let Some(ref cols) = self.projection {
            let projected_row = cols.iter()
                .map(|&col_idx| row[col_idx].clone())
                .collect();
            
            self.buffers.front_mut().unwrap()
                .write_values(projected_row);
        } else {
            self.buffers.front_mut().unwrap()
                .copy_and_write_values(row);
        }
    }

    pub fn copy_and_write_from(&mut self, num_rows: usize, bufs: &[&[Data]]) {
        // first, make sure we have an empty buffer
        self.send_buffer();
        self.ensure_buffer();

        
        let ub = self.buffers.front_mut().unwrap();
        {
            let target_vec = ub.raw_data_mut();
            debug_assert!(target_vec.is_empty());
            
            for row_idx in 0..num_rows {
                let acc_func = |idx: usize| { &bufs[idx][row_idx] };

                if !self.filters.iter().all(|p| p.eval_with_accessor(&acc_func)) {
                    // row is filtered out
                    continue;
                }

                if let Some(ref cols) = self.projection {
                    for &col in cols.iter() {
                        target_vec.push(bufs[col][row_idx].clone());
                    }
                } else {
                    for buf in bufs {
                        target_vec.push(buf[row_idx].clone());
                    }
                }
            }
        }
        ub.recompute_row_count();
    }


    pub fn write_many(&mut self, rows: Vec<Data>) {
        // ensure this is a valid number of data points
        assert!(rows.len() % self.types.len() == 0);

        // TODO candidate for optimization, since this will do multiple
        // copies
        for row in rows.chunks(self.types.len()) {
            self.copy_and_write(row);
        }
        drop(rows);
    }

    pub fn write_strings(&mut self, mut row: Vec<String>) {
        if let Some(ref cols) = self.projection {
            row = cols.iter().map(|&col_idx| row[col_idx].clone()).collect()
        }
        
        let data: Vec<Data> = row.into_iter().enumerate().map(|(idx, field)| {
            let dt = &self.types[idx];
            return dt.from_string(field)
                .unwrap_or_else(|| dt.default_value());
        }).collect();

        if !self.filters.iter().all(|p| p.eval(&data)) {
            // don't write the row.
            return;
        }

        self.prepare_for_write();
        self.buffers.front_mut().unwrap()
            .write_values(data);
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
    use predicate::Predicate;
    use operator_buffer::{make_buffer_pair, PeekableOperatorReadBuffer};
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

        let mut seen_rows = 0;
        iterate_buffer!(r, idx, row, {
            seen_rows += 1;
            match idx {
                0 => { assert_eq!(row[0], Data::Integer(5)); }
                1 => { assert_eq!(row[0], Data::Integer(6)); }
                2 => { assert_eq!(row[0], Data::Integer(-100)); }
                _ => { panic!("Too many values!"); }
            }
        });

        assert_eq!(seen_rows, 3);
    }

    #[test]
    fn can_use_peek() {
        let (r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(-100)]);
        w.flush();
        drop(w);

        let mut prdr = PeekableOperatorReadBuffer::new(r);
        
        let mut seen_rows = 0;

        while prdr.peek().is_some() {
            let row = prdr.pop().unwrap();
            match seen_rows {
                0 => { assert_eq!(row[0], Data::Integer(5)); }
                1 => { assert_eq!(row[0], Data::Integer(6)); }
                2 => { assert_eq!(row[0], Data::Integer(-100)); }
                _ => { panic!("Too many values!"); }
            }
            seen_rows += 1;
        }
        
        assert_eq!(seen_rows, 3);
    }

    #[test]
    fn can_filter() {
        let (mut r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);

        let json = json!({ "op": "eq", "col": 0, "val": 6 });
        let p = Predicate::from_json(&json);
        w.add_filter(p);

        w.write(vec![Data::Integer(5)]);
        w.write(vec![Data::Integer(6)]);
        w.write(vec![Data::Integer(-100)]);
        w.flush();
        drop(w);

        let mut seen_rows = 0;
        iterate_buffer!(r, idx, row, {
            seen_rows += 1;
            match idx {
                0 => { assert_eq!(row[0], Data::Integer(6)); }
                _ => { panic!("Too many values!"); }
            }
        });

        assert_eq!(seen_rows, 1);
    }

    #[test]
    fn can_project() {
        let (mut r, mut w) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        w.set_projection(vec![1]);

        w.write(vec![Data::Integer(-50),  Data::Integer(5)]);
        w.write(vec![Data::Integer(-90), Data::Integer(6)]);
        w.write(vec![Data::Integer(1000), Data::Integer(-100)]);
        w.flush();
        drop(w);

        let mut seen_rows = 0;
        iterate_buffer!(r, idx, row, {
            seen_rows += 1;
            match idx {
                0 => { assert_eq!(row[0], Data::Integer(5)); }
                1 => { assert_eq!(row[0], Data::Integer(6)); }
                2 => { assert_eq!(row[0], Data::Integer(-100)); }
                _ => { panic!("Too many values!"); }
            }
        });

        assert_eq!(seen_rows, 3);
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
                for _ in row {
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
