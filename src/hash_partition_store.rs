use operator_buffer::OperatorReadBuffer;
use spillable_store::WritableSpillableStore;
use std::collections::vec_deque::VecDeque;
use std::hash::{Hash, Hasher};
use fnv::{FnvHasher};
use std::cmp;


const MAX_FILES:usize = 32;


pub struct ReadableHashPartitionStore {
    data: VecDeque<OperatorReadBuffer>,
    num_partitions: usize
}

impl ReadableHashPartitionStore {
    pub fn new(max_size: usize, mut data: OperatorReadBuffer,
               relv_cols: &[usize]) -> ReadableHashPartitionStore {
        let mut wss = WritableSpillableStore::new(max_size, data.types().to_vec());
        let mut count = 0;
        
        iterate_buffer!(data, row, {
            wss.push_row(row);
            count += 1;
        });
                        
        // don't create more than MAX_FILES files ever, in case we run out of
        // allocatable FDs
        let num_partitions = cmp::min(MAX_FILES, ((count / max_size) as usize)+1);

        
        return ReadableHashPartitionStore::with_partitions(
            num_partitions,
            4096,
            wss.into_read_buffer().1,
            relv_cols);
    }

    pub fn with_partitions(num_partitions: usize,
                       buf_size: usize,
                       mut data: OperatorReadBuffer, relv_cols: &[usize])
                       -> ReadableHashPartitionStore {

        let mut bufs = Vec::with_capacity(num_partitions);

        for _ in 0..num_partitions {
            bufs.push(WritableSpillableStore::new(buf_size, data.types().to_vec()));
        }

        iterate_buffer!(data, row, {
            let mut hasher = FnvHasher::default();
            for &col_idx in relv_cols.iter() {
                row[col_idx].hash(&mut hasher);
            }
            
            let mut hash_value = (hasher.finish() % num_partitions as u64)
                as usize;

            bufs[hash_value].push_row(row);
        });

        let mut vdq = VecDeque::new();

        for wss in bufs {
            vdq.push_back(wss.into_read_buffer().1);
        }

        return ReadableHashPartitionStore { data: vdq,
                                            num_partitions };
    }

    pub fn next_buf(&mut self) -> Option<OperatorReadBuffer> { self.data.pop_front() }
    pub fn num_partitions(&self) -> usize { self.num_partitions }
}


#[cfg(test)]
mod test {
    use data::{Data, DataType};
    use hash_partition_store::{ReadableHashPartitionStore, MAX_FILES};
    use spillable_store::WritableSpillableStore;
    use fnv::{FnvHasher};
    use std::hash::{Hasher, Hash};

    
    #[test]
    fn simple_rhps_test() {
        let mut wss = WritableSpillableStore::new(50000, vec![DataType::INTEGER]);

        for i in 0..10000 {
            wss.push_row(&vec![Data::Integer(i*6)]);
            wss.push_row(&vec![Data::Integer(i*5)]);
            wss.push_row(&vec![Data::Integer(i*-100)]);
        }

        let mut rhps = ReadableHashPartitionStore::new(
            100, wss.into_read_buffer().1, &[0]);

        // 100 is too small a size, so we should hit the cap of 32
        // partitions. 
        assert_eq!(rhps.num_partitions(), MAX_FILES);

        let mut row_count = 0;
        for hv in 0..MAX_FILES {
            let nxt = rhps.next_buf();
            if nxt.is_none() {
                panic!("Not enough buffers!");
            }

            let mut buf = nxt.unwrap();
            iterate_buffer!(buf, row, {
                let mut hasher = FnvHasher::default();
                row[0].hash(&mut hasher);
                let mut hash_value = (hasher.finish() % 32)
                    as usize;

                assert_eq!(hash_value, hv);
                row_count += 1;
            });
        }

        assert!(rhps.next_buf().is_none());
        assert_eq!(row_count, 3*10000);
    }
}
