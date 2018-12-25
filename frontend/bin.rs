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

#[macro_use]
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
    let (b, jh) = root.start_possibly_save();

    if let Some(mut buf) = b {
        iterate_buffer!(buf, row, {
            println!("{:?}", row);
        });
    } else {
        println!("Root operator returned no data.");
    }
    
    jh.join().unwrap();
}

                          
