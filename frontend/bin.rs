extern crate byoo;

use std::env;
use std::fs::File;
use std::io::prelude::*;

fn usage_and_quit() -> ! {
    println!("Usage: byoo FILE");
    panic!("Invalid usage");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let file_name = if args.len() == 1 {
        eprintln!("No file passed, using default value of plan.json");
        "plan.json"
    } else if args.len() > 2 {
        usage_and_quit();
    } else {
        args[1].as_str()
    };

    let mut file = File::open(file_name).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    
    let root = byoo::compile(contents);
    let jh = root.start();
    jh.join().unwrap();
}

                          
