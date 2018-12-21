use operator_buffer::OperatorReadBuffer;
use spillable_store::WritableSpillableStore;
use std::collections::vec_deque::VecDeque;
use std::hash::{Hash, Hasher};
use fnv::{FnvHashMap, FnvHasher};
use std::cmp;


const MAX_FILES:usize = 32;

type Node = usize;
struct HashTree {
    nodes: usize,
    children: FnvHashMap<usize, (usize, usize)>,
    sizes: Vec<usize>
}


impl HashTree {    
    fn new() -> HashTree {
        return HashTree {
            nodes: 1, children: FnvHashMap::default(), sizes: vec![0]
        };
    }

    fn root(&self) -> Node { 0 }

    fn left_child(&self, node: Node) -> Node { self.children[&node].0 }
    fn right_child(&self, node: Node) -> Node { self.children[&node].1 }
    fn is_leaf(&self, node: Node) -> bool { !self.children.contains_key(&node) }
    
    fn size(&self, node: Node) -> usize { self.sizes[node] }
    fn inc_size(&mut self, node: Node) { self.sizes[node] += 1; }

    fn add_children(&mut self, parent: Node, children_size: usize) {
        self.sizes.push(children_size);
        self.sizes.push(children_size);
        
        let lc = self.nodes;
        let rc = self.nodes + 1;
        self.nodes += 2;
        debug_assert_eq!(self.sizes.len(), self.nodes);
        
        self.children.insert(parent, (lc, rc));

    }

    fn count_leaves_under(&self, node: Node) -> usize {
        if self.is_leaf(node) {
            return 1;
        }

        let lsum = self.count_leaves_under(self.left_child(node));
        let rsum = self.count_leaves_under(self.right_child(node));
        return lsum + rsum;
    }
}

#[cfg(test)]
mod ht_test {
    use hash_partition_store::HashTree;
    
    #[test]
    fn simple_ht_test() {
        let mut tree = HashTree::new();
        let root = tree.root();
        assert!(tree.is_leaf(root));
        assert_eq!(tree.count_leaves_under(root), 1);

        tree.inc_size(root);
        assert_eq!(tree.size(root), 1);


        tree.add_children(root, 5);
        assert_eq!(tree.size(tree.left_child(root)), 5);
        assert_eq!(tree.count_leaves_under(root), 2);

        let lc = tree.left_child(root);
        tree.add_children(lc, 10);
        assert_eq!(tree.count_leaves_under(root), 3);
    }
}


pub struct ReadableHashPartitionStore {
    data: VecDeque<OperatorReadBuffer>,
    num_partitions: usize
}

impl ReadableHashPartitionStore {
    pub fn new(max_size: usize, mut data: OperatorReadBuffer,
               relv_cols: &[usize]) -> ReadableHashPartitionStore {
        let mut wss = WritableSpillableStore::new(max_size, data.types().to_vec());

        let mut tree = HashTree::new();
        
        iterate_buffer!(data, row, {
            wss.push_row(row);

            let mut hasher = FnvHasher::default();
            for &col_idx in relv_cols {
                row[col_idx].hash(&mut hasher);
            }
            
            let mut hash_value = hasher.finish();
            let mut hd_ptr = tree.root();
            
            loop {
                if tree.is_leaf(hd_ptr) {
                    let curr_size = tree.size(hd_ptr);
                    if curr_size >= max_size {
                        // we need to split up this node.
                        // we will assume that this split will cause all the data
                        // already represented at this node to be split 0.65 / 0.65
                        // (greater than 0.5 to be pessimistic)
                        tree.add_children(hd_ptr, (curr_size as f64 * 0.65) as usize);
                    } else {
                        tree.inc_size(hd_ptr);
                    }

                    break;
                }

                // otherwise, we are at an internal node
                let nxt_bit = hash_value % 2;
                hash_value /= 2;
                hd_ptr = if nxt_bit == 0 {
                    tree.left_child(hd_ptr)
                } else {
                    tree.right_child(hd_ptr)
                }
            }
        });

        // don't create more than MAX_FILES files ever, in case we run out of
        // allocatable FDs
        let num_partitions = cmp::min(MAX_FILES, tree.count_leaves_under(tree.root()));

        
        return ReadableHashPartitionStore::with_partitions(
            num_partitions,
            max_size,
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
