use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use operator::ConstructableOperator;
use data::{Data};
use serde_json;
//use fnv::{FnvHashMap};
use hashbrown::HashMap;
use hash_partition_store::ReadableHashPartitionStore;
use std::fs::File;
use std::hash::{Hash, Hasher};



const HASHTABLE_SIZE_LIMIT: usize = 65536*4; // 2^16

// TODO both of these Option<>'s can be removed with the
// addition of substruct that handles the do_join
pub struct HashJoin {
    left: Option<OperatorReadBuffer>,
    right: Option<OperatorReadBuffer>,
    out: OperatorWriteBuffer,
    left_cols: Vec<usize>,
    right_cols: Vec<usize>
}

enum RefOrCopy<'a> {
    Ref(&'a [Data]),
    Copy(Vec<Data>)
}

struct HashJoinKey<'a> {
    relv_cols: &'a [usize],
    data: RefOrCopy<'a>
}

impl <'a> Hash for HashJoinKey<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ref_val = self.get_ref();
        
        for &idx in self.relv_cols {
            ref_val[idx].hash(state);
        }
    }
}

impl <'a> PartialEq for HashJoinKey<'a> {
    fn eq(&self, other: &HashJoinKey) -> bool {
        debug_assert!(self.relv_cols.len() == other.relv_cols.len());
        let ref1 = self.get_ref();
        let ref2 = other.get_ref();
        
        for (&idx1, &idx2) in self.relv_cols.iter().zip(other.relv_cols) {
            if ref1[idx1] != ref2[idx2] { return false; }
        }
        return true;
    }
}

impl <'a> Eq for HashJoinKey<'a> { }

impl <'a> HashJoinKey<'a> {
    fn new_by_ref(relv_cols: &'a[usize], data: &'a[Data]) -> HashJoinKey<'a> {
        return HashJoinKey {
            relv_cols,
            data: RefOrCopy::Ref(data)
        };
    }

    fn new_by_val(relv_cols: &'a[usize], data: &[Data]) -> HashJoinKey<'a> {
        return HashJoinKey {
            relv_cols,
            data: RefOrCopy::Copy(data.to_vec())
        };
    }

    fn get_ref(&self) -> &[Data] {
        match self.data {
            RefOrCopy::Ref(d) => d,
            RefOrCopy::Copy(ref d) => d
        }
    }
}

impl HashJoin {
    pub fn new(left: OperatorReadBuffer, right: OperatorReadBuffer,
               out: OperatorWriteBuffer,
               left_cols: Vec<usize>,
               right_cols: Vec<usize>)
               -> HashJoin {

        assert!(left_cols.len() == right_cols.len());
        
        return HashJoin {
            left: Some(left), right: Some(right),
            out, left_cols, right_cols
        };
    }

 
    pub fn start(mut self) {
        // first, see how many different hash partitions we need to split
        // the left relation into
        let left = self.left.take().unwrap();
        let right = self.right.take().unwrap();

        let mut left_hash_store = ReadableHashPartitionStore::new(
            HASHTABLE_SIZE_LIMIT, left, &self.left_cols);

        if left_hash_store.num_partitions() == 1 {
            // the whole left-side dataset fits in memory. We only
            // have to iterate over the right-side dataset once.
            self.do_join(left_hash_store.next_buf().unwrap(), right);
            return;
        }
        println!("Num partitions: {}", left_hash_store.num_partitions());
        // otherwise, we have more than one partition on the left side.
        // we'll need to split the right side into an equal number of partitions
        let mut right_hash_store = ReadableHashPartitionStore::with_partitions(
            left_hash_store.num_partitions(),
            HASHTABLE_SIZE_LIMIT, right, &self.right_cols);

        assert_eq!(left_hash_store.num_partitions(),
                   right_hash_store.num_partitions());
        
        for _ in 0..left_hash_store.num_partitions() {
            println!("Joining...");
            let mut sub_left = left_hash_store.next_buf().unwrap();
            let mut sub_right = right_hash_store.next_buf().unwrap();
            self.do_join(sub_left, sub_right);
        }

        assert!(left_hash_store.next_buf().is_none());
        assert!(right_hash_store.next_buf().is_none());
        
    }

    fn do_join(&mut self,
               mut left: OperatorReadBuffer, mut right: OperatorReadBuffer) {
        let mut ht: HashMap<HashJoinKey, Vec<Vec<Data>>> = HashMap::default();

        // first, load the left side into a hash table.
        iterate_buffer!(left, row, {
            let key = HashJoinKey::new_by_val(&self.left_cols, row);
            ht.entry(key)
                .or_insert_with(Vec::new)
                .push(row.to_vec());
        });

        let mut out_row = Vec::new();
        iterate_buffer!(right, row, {
            let key2 = HashJoinKey::new_by_ref(&self.right_cols, row);
            if let Some(matches) = ht.get(&key2) {
                // all these rows match.
                for matching_row in matches.iter() {
                    out_row.clear();
                    out_row.extend_from_slice(matching_row);
                    out_row.extend_from_slice(row);
                    self.out.copy_and_write(&out_row);
                }
            }
        });
    }

 }

impl ConstructableOperator for HashJoin {
    fn from_buffers(output: Option<OperatorWriteBuffer>,
                    mut input: Vec<OperatorReadBuffer>,
                    file: Option<File>,
                    options: serde_json::Value) -> Self {
        
        assert!(file.is_none());
        let o = output.unwrap();

        assert_eq!(input.len(), 2);
        let lb = input.remove(0);
        let rb = input.remove(0);

        assert!(options["left_cols"].is_array(),
                "hash join operator missing left_cols array!");

        assert!(options["right_cols"].is_array(),
                "hash join operator missing right_cols array!");


        let left_cols = options["left_cols"].as_array().unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap() as usize)
            .collect();

        let right_cols = options["right_cols"].as_array().unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap() as usize)
            .collect();

        
        return HashJoin::new(lb, rb, o, left_cols, right_cols);
    }
}


#[cfg(test)]
mod tests {
    use operator::join::HashJoin;
    use operator_buffer::{make_buffer_pair};
    use data::{Data, DataType};

    #[test]
    fn equijoin() {
        let (r1, mut w1) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        let (r2, mut w2) = make_buffer_pair(5, 10, vec![DataType::INTEGER]);
        let (mut r, w) = make_buffer_pair(5, 10, vec![DataType::INTEGER,
                                                      DataType::INTEGER]);

        w1.write(vec![Data::Integer(5)]);
        w1.write(vec![Data::Integer(6)]);
        w1.write(vec![Data::Integer(7)]);
        drop(w1);
        
        w2.write(vec![Data::Integer(5)]);
        w2.write(vec![Data::Integer(5)]);
        w2.write(vec![Data::Integer(5)]);
        w2.write(vec![Data::Integer(8)]);
        drop(w2);

        let j = HashJoin::new(r1, r2, w, vec![0], vec![0]);
        j.start();

        let mut rc = 0;
        iterate_buffer!(r, idx, row, {
            rc += 1;
            match idx {
                0 => { assert_eq!(row.to_vec(), vec![Data::Integer(5), Data::Integer(5)]); },
                1 => { assert_eq!(row.to_vec(), vec![Data::Integer(5), Data::Integer(5)]); },
                2 => { assert_eq!(row.to_vec(), vec![Data::Integer(5), Data::Integer(5)]); },
                _ => { panic!("too many rows"); }
            };
        });

        assert_eq!(rc, 3);
    }
}

