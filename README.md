# byoo

**This project should be regarded as "alpha" quality. Documentation is sparse.**

This is *byoo*, the **b**ring **y**our **o**wn **o**ptimizer execution engine. byoo (pronounced "bio") is a simple, entirely modular, high performance relational database execution engine designed for optimizer research purposes.

byoo uses a pipelined concurrent "push" data model (as opposed to a "pull"/Volcano model), compresses intermediary results, supports row-store and columnar operations, and can directly read CSV files as 1st class citizens. Written in Rust, byoo is free from many sources of undefined behavior and memory issues that plague C/C++ codebases, which (1) makes it easier to work with and (2) should give you more confidence experimental results produced.

Goals:

* Initially, enough complexity and operators to support the Join Order Benchmark (JOB) and TPC-H. 
* Fast, multithreaded performance faster than, or at least comparable to, PostgreSQL.
* Serverless, any process, embeddable, like SQLite.
* A "do-what-I-say" engine that obeys user-specified join orderings, join operators, aggregation operators, index selection, etc.
* Precisely controllable memory usage and allocation.
* Easy & convenient interface for researchers.

Non-goals:

* Transactions, high-performance inserts. byoo is designed for OLAP workloads.
* A full-SQL interface. Such an interface would certainly require an optimizer. byoo could be used as an execution engine in such a database, but does not seek to be one itself.
* Multi-user support. byoo supports only a single writer at a time.

## Execution plans
You can run execution plans with byoo using the binary built by Cargo (e.g. `cargo build --release`). You can then use byoo to execute a plan like so:

```bash
target/release/byoo my_plan.json # or cargo run --release -- my_plan.json
```

The referenced JSON file should be a tree of operators. Currently, the format is quite verbose, but easy to generate programatically. For example, to compute some aggregates using a hash `group by` operator:

```json
{"op": "project",
 "options": { "cols": [0, 3, 4, 5] }, 
 "input": [
     {
         "op": "hashed group by",
         "options": {
             "col": 0,
             "aggregates": [
                 {"op": "min", "col": 1},
                 {"op": "max", "col": 1},
                 {"op": "count", "col": 1}
             ]
         },
         "input": [
             { "op": "csv read",
               "options": {
                   "file": "res/inputs/agg_test.csv",
                   "types": ["INTEGER", "INTEGER", "REAL"]
               }
             }]
     }
 ]
}
```

More examples of plans can be found in `tests/`. You can find a listing of all currently supported operators in `src/compile/mod/rs`.

## Tests and benchmarks

To run the tests, clone the repository and execute `make` in the `res` folder (this extracts and builds the input files needed for testing). Then, testing is as simple as `cargo test`. For speed reasons, some tests only run in release mode, so run `cargo test --release` to get a few extras.

There are benchmarks as well. Currently, the Cargo benchmarking tools only work with nightly Rust (although byoo works perfectly fine with stable rust). To run them, execute `cargo bench`.
