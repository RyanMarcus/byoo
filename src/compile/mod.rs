use data::{DataType};
use serde_json;
use std::collections::{VecDeque};
use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer, make_buffer_pair};
use operator::{ConstructableOperator, Filter, Project, Sort, ColumnUnion};
use operator::output::{CsvOutput, ColumnarOutput};
use operator::scan::{CsvScan, ColumnarScan};
use operator::join::{LoopJoin, MergeJoin, HashJoin};
use operator::groupby::{SortedGroupBy, HashedGroupBy};
use agg;
use std::fs::File;
use std::fmt;
use std::thread;
use std::thread::JoinHandle;


enum Operator {
    Union, Project, Filter, LoopJoin, MergeJoin, HashJoin,
    Sort, ColumnarRead, CSVRead, CSVOut, ColumnarOut,
    SortedGroupBy, HashedGroupBy
}

impl Operator {
    fn from_opcode(opcode: &str) -> Operator {
        return match opcode {
            "union" => Operator::Union,
            "project" => Operator::Project,
            "filter" => Operator::Filter,
            "loop join" => Operator::LoopJoin,
            "merge join" => Operator::MergeJoin,
            "hash join" => Operator::HashJoin,
            "sort" => Operator::Sort,
            "columnar read" => Operator::ColumnarRead,
            "csv read" => Operator::CSVRead,
            "csv out" => Operator::CSVOut,
            "columnar out" => Operator::ColumnarOut,
            "sorted group by" => Operator::SortedGroupBy,
            "hashed group by" => Operator::HashedGroupBy,
            _ => panic!("invalid opcode")
        };
    }

    fn requires_input_file(&self) -> bool {
        match self {
            Operator::ColumnarRead => true,
            Operator::CSVRead => true,
            _ => false
        }
    }

    fn requires_output_file(&self) -> bool {
        match self {
            Operator::CSVOut => true,
            Operator::ColumnarOut => true,
            _ => false
        }
    }


}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Operator::Union => write!(f, "union"),
            Operator::Project => write!(f, "project"),
            Operator::Filter => write!(f, "filter"),
            Operator::LoopJoin => write!(f, "loop join"),
            Operator::MergeJoin => write!(f, "merge join"),
            Operator::HashJoin => write!(f, "hash join"),
            Operator::Sort => write!(f, "sort"),
            Operator::ColumnarRead => write!(f, "columnar read"),
            Operator::CSVRead => write!(f, "csv read"),
            Operator::CSVOut => write!(f, "csv out"),
            Operator::ColumnarOut => write!(f, "columnar out"),
            Operator::SortedGroupBy => write!(f, "sorted group by"),
            Operator::HashedGroupBy => write!(f, "hashed group by")
        }
    }
}

fn inputs_per_op(op: &str) -> ChildCount {
    return match op {
        "union" => ChildCount::Any,
        "project" => ChildCount::Specific(1),
        "filter" => ChildCount::Specific(1),
        "loop join" => ChildCount::Specific(2),
        "merge join" => ChildCount::Specific(2),
        "hash join" => ChildCount::Specific(2),
        "sort" => ChildCount::Specific(1),
        "columnar read" => ChildCount::None,
        "csv read" => ChildCount::None,
        "csv out" => ChildCount::Specific(1),
        "columnar out" => ChildCount::Specific(1),
        "sorted group by" => ChildCount::Specific(1),
        "hashed group by" => ChildCount::Specific(1),
        _ => panic!("unknown op code")
    };
}

enum InType {
    Unknown,
    None,
    Known(Vec<Vec<DataType>>)
}

#[derive(Debug)]
pub enum OutType {
    Unknown,
    None,
    Known(Vec<DataType>)
}

enum ChildCount {
    None,
    Any,
    Specific(usize)
}

pub struct OperatorNode {
    id: usize,
    opcode: Operator,
    in_types: InType,
    out_type: OutType,
    children: Vec<OperatorNode>,
    options: serde_json::Value
}

fn get_operator_out_type(opcode: &Operator,
                         options: &serde_json::Value,
                         in_types: &[Vec<DataType>]) -> OutType {
    match opcode {
        Operator::LoopJoin
            | Operator::MergeJoin
            | Operator::HashJoin
            | Operator::Union => {
            // flattens all the types
            return OutType::Known(
                in_types.iter()
                    .flat_map(|x| x)
                    .cloned()
                    .collect());
        },
        Operator::Project => {
            return OutType::Known(
                options["cols"].as_array().unwrap()
                    .iter()
                    .map(|v| v.as_i64().unwrap() as usize)
                    .map(|idx| in_types[0][idx].clone())
                    .collect());
        },
        Operator::Sort | Operator::Filter => {
            return OutType::Known(in_types[0].clone());
        },
        Operator::ColumnarRead | Operator::CSVRead => {
            return OutType::Known(
                options["types"].as_array().unwrap()
                    .iter()
                    .map(|v| DataType::from_string_code(v.as_str().unwrap()))
                    .collect());
        },
        Operator::ColumnarOut | Operator::CSVOut => return OutType::None,
        Operator::SortedGroupBy | Operator::HashedGroupBy => {
            let mut input_types = in_types[0].clone();

            for agg_type in options["aggregates"].as_array().unwrap() {
                let op = agg_type["op"].as_str().unwrap();
                let col_idx = agg_type["col"].as_i64().unwrap() as usize;

                let agg = agg::new(op, col_idx);
                input_types.push(agg.out_type(&in_types[0][col_idx]));
            }
            
            return OutType::Known(input_types);
        }
    };
}

macro_rules! spawn_op {
    ($x: ident, $p1: expr, $p2: expr, $p3: expr, $p4: expr) => {{
        let op = $x::from_buffers($p1, $p2, $p3, $p4);
        thread::spawn(move || {
            op.start();
        })
    }}
}

impl OperatorNode {
    fn new(id: usize, opcode: &str,
           options: serde_json::Value) -> OperatorNode {

        return OperatorNode {
            opcode: Operator::from_opcode(opcode),
            options, id,
            in_types: InType::Unknown,
            out_type: OutType::Unknown,
            children: Vec::new()
        };
    }

    fn add_child(&mut self, child: OperatorNode) {
        self.children.push(child);
    }

    fn get_in_types_from_children(&mut self) -> Vec<Vec<DataType>> {
        let mut in_types = Vec::new();

        for child in self.children.iter() {
            match &child.out_type {
                OutType::Unknown => panic!("child output type unknown"),
                OutType::None => panic!("child output type is none"),
                OutType::Known(ref t) => in_types.push(t.clone())
            };
        }

        return in_types;
    }


    fn derive_types(&mut self) {
        if let OutType::Known(_) = self.out_type {
            // we have already derived our type
            return;
        }
        
        // otherwise, I have to derive my output type
        for child in self.children.iter_mut() {
            child.derive_types();
        }
        
        let my_in = self.get_in_types_from_children();
        self.out_type = get_operator_out_type(&self.opcode,
                                              &self.options,
                                              &my_in);
        self.in_types = if my_in.is_empty() {
            InType::None
        } else {
            InType::Known(my_in)
        };
    }

    pub fn start(self) -> JoinHandle<()> {
        return self.run(None);
    }

    pub fn start_save(self) -> (OperatorReadBuffer, JoinHandle<()>) {
        let (r, w) = match self.out_type {
            OutType::None =>
                panic!("Root operator has no output, but buffered output requested"),
            OutType::Known(ref v) => make_buffer_pair(5, 4096, v.clone()),
            OutType::Unknown => panic!("unknown output types in start_save")
        };


        return (r, self.run(Some(w)));
    }
    
    fn run(self, output: Option<OperatorWriteBuffer>) -> JoinHandle<()> {

        // check to see if we need an input or output file
        let f = if self.opcode.requires_input_file() {
            let path = self.options["file"].as_str().unwrap();
            Some(File::open(path).unwrap())
        } else if self.opcode.requires_output_file() {
            let path = self.options["file"].as_str().unwrap();
            Some(File::create(path).unwrap())
        } else {
            None
        };

        // construct the input buffer(s) for this operator
        let mut read_bufs = Vec::new();
        let mut write_bufs = Vec::new();
        match self.in_types {
            InType::Known(v) => {
                for dts in v.iter() {
                    let (r, w) = make_buffer_pair(5, 4096, dts.clone());
                    read_bufs.push(r);
                    write_bufs.push(w);
                }
            },
            InType::None => {
                // nothing to do.
            },
            InType::Unknown => {
                panic!("Unknown input types in run for operator {}", self.opcode);
            }
        };

        let jh = match self.opcode {
            Operator::CSVOut => spawn_op!(CsvOutput, output, read_bufs, f, self.options),
            Operator::CSVRead => spawn_op!(CsvScan, output, read_bufs, f, self.options),
            Operator::ColumnarOut => spawn_op!(ColumnarOutput, output, read_bufs, f, self.options),
            Operator::ColumnarRead => spawn_op!(ColumnarScan, output, read_bufs, f, self.options),
            Operator::Filter => spawn_op!(Filter, output, read_bufs, f, self.options),
            Operator::LoopJoin => spawn_op!(LoopJoin, output, read_bufs, f, self.options),
            Operator::MergeJoin => spawn_op!(MergeJoin, output, read_bufs, f, self.options),
            Operator::HashJoin => spawn_op!(HashJoin, output, read_bufs, f, self.options),
            Operator::Project => spawn_op!(Project, output, read_bufs, f, self.options),
            Operator::Sort => spawn_op!(Sort, output, read_bufs, f, self.options),
            Operator::Union => spawn_op!(ColumnUnion, output, read_bufs, f, self.options),
            Operator::SortedGroupBy => spawn_op!(SortedGroupBy, output, read_bufs, f, self.options),
            Operator::HashedGroupBy => spawn_op!(HashedGroupBy, output, read_bufs, f, self.options)
        };

        //  next, we have to start the children.
        for (op, wb) in self.children.into_iter().zip(write_bufs) {
            op.run(Some(wb));
        }
        
        return jh;
    }
}

pub fn compile(json: String) -> OperatorNode {
    let parsed = serde_json::from_str(json.as_str())
        .unwrap();
    drop(json);

    return create_op_tree(&parsed, 0).1;
}

fn create_op_tree(root: &serde_json::Value, nxt_id: usize)
                  -> (usize, OperatorNode) {
    let opcode = root["op"].as_str().unwrap();

    let mut to_r = OperatorNode::new(nxt_id,
                                     opcode,
                                     root["options"].clone());

    // next, build all the children.
    let children = match root["input"].as_array() {
        Some(a) => a.clone(),
        None => Vec::new()
    };
    
    match inputs_per_op(opcode) {
        ChildCount::None => assert!(children.is_empty()),
        ChildCount::Any => assert!(!children.is_empty()),
        ChildCount::Specific(i) => assert_eq!(
            children.len(), i,
            "opcode {} had {} inputs but should have had {}",
            opcode, children.len(), i
        )
    };

    let mut num_added = 1;
    for v in children {
        let (nc, c) = create_op_tree(&v, nxt_id + num_added);
        to_r.add_child(c);
        num_added += nc;
    }

    to_r.derive_types();

    return (num_added, to_r);
}

fn label_for_node(node: &OperatorNode) -> String {
    let mut to_r = String::new();

    to_r.push_str(format!("<b>{}</b> ({})<br/>", node.opcode.to_string(),
                          node.id).as_str());

    if let Some(opts) = node.options.as_object() {
        for (k, v) in opts.iter() {
            if k == "types" {
                continue;
            }
            
            to_r.push_str(
                format!("{}: {}<br/>", k, v).as_str());
        }
    }

    match node.out_type {
        OutType::None => {},
        OutType::Known(ref v) => {
            to_r.push_str(format!("<br/>outputs {:?}", v).as_str());
        },
        OutType::Unknown => panic!("unknown output types in label generation")
    };

    return to_r;
}

pub fn tree_to_gv(root: &OperatorNode) -> String {
    let mut labels = String::new();
    let mut edges = String::new();
    let mut stack = VecDeque::new();
    stack.push_back(root);

    
    while let Some(node) = stack.pop_front() {
        labels.push_str(format!("op{} [label=<{}>, shape=box];\n", node.id,
                                label_for_node(node)).as_str());

        for child in node.children.iter() {
            edges.push_str(format!("op{} -> op{};\n", child.id, node.id).as_str());
            stack.push_back(child);
        }
    }
    
    return format!("digraph G {{\nrankdir=BT;\n{}\n\n{}\n}}\n", labels, edges);
}

#[cfg(test)]
mod tests {

    use compile::{tree_to_gv, create_op_tree};
    use serde_json;
    
    #[test]
    fn plan_gv_test() {
        let json: serde_json::Value = serde_json::from_str("
{
    \"op\": \"columnar out\",
    \"options\": {
        \"file\": \"tests/outputs/test.dat\"
    },
    \"input\": [
        { \"op\": \"project\",
          \"options\": {
              \"cols\": [0, 1, 3]
          },

          \"input\": [
              { \"op\": \"csv read\",
                \"options\": {
                    \"file\": \"tests/inputs/test.csv\",
                    \"types\": [\"INTEGER\", \"INTEGER\", \"TEXT\", \"INTEGER\"]
                }
              }
          ]
        }
    ]
}
").unwrap();

        let (count, root) = create_op_tree(&json, 0);
        assert_eq!(count, 3);
        
        let gv = tree_to_gv(&root);

        assert!(gv.contains("columnar out"));
        assert!(gv.contains("project"));
        assert!(gv.contains("csv read"));
    }
}
