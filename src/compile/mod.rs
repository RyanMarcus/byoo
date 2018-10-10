use data::{Data, DataType};
use serde_json;
use std::collections::{VecDeque};
use operator_buffer::{OperatorReadBuffer, OperatorWriteBuffer};
use operator::{ColumnUnion};

enum InType {
    Unknown,
    None,
    Known(Vec<Vec<DataType>>)
}

#[derive(Debug)]
enum OutType {
    Unknown,
    None,
    Known(Vec<DataType>)
}

enum ChildCount {
    None,
    Any,
    Specific(usize)
}

struct OperatorNode {
    id: usize,
    opcode: String,
    in_types: InType,
    out_type: OutType,
    children: Vec<OperatorNode>,
    options: serde_json::Value
}

fn get_operator_out_type(opcode: &String,
                         options: &serde_json::Value,
                         in_types: &[Vec<DataType>]) -> OutType {
    match opcode.as_str() {
        "loop join" |
        "merge join" |
        "union" => {
            // flattens all the types
            return OutType::Known(
                in_types.iter()
                    .flat_map(|x| x)
                    .cloned()
                    .collect());
        },
        "project" => {
            return OutType::Known(
                options["keep_cols"].as_array().unwrap()
                    .iter()
                    .map(|v| v.as_i64().unwrap() as usize)
                    .map(|idx| in_types[0][idx].clone())
                    .collect());
        },
        "sort" |
        "filter" => {
            return OutType::Known(in_types[0].clone());
        },
        "columnar read" |
        "csv read" => {
            return OutType::Known(
                options["types"].as_array().unwrap()
                    .iter()
                    .map(|v| DataType::from_string_code(v.as_str().unwrap()))
                    .collect());
        },
        "columnar out" |
        "csv out" => return OutType::None,
        _ => panic!("{} is an invalid opcode for internal node", opcode)
    };
}

impl OperatorNode {
    fn new(id: usize, opcode: String,
           options: serde_json::Value) -> OperatorNode {

        return OperatorNode {
            opcode, options, id,
            in_types: InType::Unknown,
            out_type: OutType::Unknown,
            children: Vec::new()
        };
    }

    pub fn set_out_type(&mut self, otype: OutType) {
        self.out_type = otype;
    }

    pub fn add_child(&mut self, child: OperatorNode) {
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


    pub fn derive_types(&mut self) {
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
    
    pub fn run(self, output: Option<OperatorWriteBuffer>,
               inputs: Option<Vec<OperatorReadBuffer>>) {
        match self.opcode.as_str() {
            "union" => {

            },

            _ => panic!("unknown opcode")
        };
    }
}

fn is_valid_opcode(op: &str) -> bool {
    return match op {
        "union" => true,
        "project" => true,
        "filter" => true,
        "loop join" => true,
        "merge join" => true,
        "sort" => true,
        "columnar read" => true,
        "csv read" => true,
        "csv out" => true,
        "columnar out" => true,
        _ => false
    };
}

fn inputs_per_op(op: &str) -> ChildCount {
    return match op {
        "union" => ChildCount::Any,
        "project" => ChildCount::Specific(1),
        "filter" => ChildCount::Specific(1),
        "loop join" => ChildCount::Specific(2),
        "merge join" => ChildCount::Specific(2),
        "sort" => ChildCount::Specific(1),
        "columnar read" => ChildCount::None,
        "csv read" => ChildCount::None,
        "csv out" => ChildCount::Specific(1),
        "columnar out" => ChildCount::Specific(1),
        _ => panic!("unknown op code")
    };
}

fn create_op_tree(mut root: serde_json::Value, nxt_id: usize)
                  -> (usize, OperatorNode) {
    let opcode = root["op"].as_str().unwrap();

    if !is_valid_opcode(opcode) {
        panic!("invalid opcode in input: {}", opcode);
    }

    let mut to_r = OperatorNode::new(nxt_id,
                                     String::from(opcode),
                                     root["options"].clone());

    // next, build all the children.
    let children = match root["input"].as_array() {
        Some(a) => a.clone(),
        None => Vec::new()
    };
    
    match inputs_per_op(opcode) {
        ChildCount::None => assert_eq!(children.len(), 0),
        ChildCount::Any => assert!(children.len() > 0),
        ChildCount::Specific(i) => assert_eq!(
            children.len(), i,
            "opcode {} had {} inputs but should have had {}",
            opcode, children.len(), i
        )
    };

    let mut num_added = 1;
    for v in children {
        let (nc, c) = create_op_tree(v.clone(), nxt_id + num_added);
        to_r.add_child(c);
        num_added += nc;
    }

    to_r.derive_types();

    return (num_added, to_r);
}

fn label_for_node(node: &OperatorNode) -> String {
    let mut to_r = String::new();

    to_r.push_str(format!("<b>{}</b> ({})<br/>", node.opcode,
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

fn tree_to_gv(root: &OperatorNode) -> String {
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

    use compile::{OperatorNode, tree_to_gv, create_op_tree};
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
              \"keep_cols\": [0, 1, 3]
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

        let (count, root) = create_op_tree(json, 0);
        assert_eq!(count, 3);
        
        let gv = tree_to_gv(&root);

        assert!(gv.contains("columnar out"));
        assert!(gv.contains("project"));
        assert!(gv.contains("csv read"));
    }

}
