extern crate core;

use core::panicking::panic;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use clap::{Arg, Command};
use shapiro::models::datalog::Rule;
use shapiro::models::index::{BTreeIndex, HashMapIndex, ImmutableVectorIndex, IndexedHashMapIndex, SpineIndex, ValueRowId, VecIndex};
use shapiro::models::reasoner::{BottomUpEvaluator, Dynamic, Materializer};
use shapiro::reasoning::reasoners::chibi::ChibiDatalog;
use shapiro::reasoning::reasoners::simple::SimpleDatalog;
use crate::Reasoners::{Chibi, SimpleBTree, SimpleHashMap, SimpleImmutableVector, SimpleSpine, SimpleVec};

fn read_file(filename: &str) -> Result<impl Iterator<Item=String>, &'static str> {
    return if let Ok(file) = File::open(filename) {
        let buffer = BufReader::new(file);

        Ok(buffer.lines().filter_map(|line| line.ok()))
    } else {
        Err("fail to open file")
    };
}

pub fn load3ple<'a>(
    filename: &str,
) -> Result<impl Iterator<Item=(String, String, String)> + 'a, &'static str> {
    match read_file(filename) {
        Ok(file) => Ok(file.map(move |line| {
            let mut split_line = line.split(' ');
            let digit_one: String = split_line.next().unwrap().to_string();
            let digit_two: String = split_line.next().unwrap().to_string();
            let digit_three: String = split_line.next().unwrap().to_string();
            (digit_one.clone(), digit_two.clone(), digit_three.clone())
        })),
        Err(msg) => Err(msg),
    }
}

pub enum Reasoners {
    Chibi,
    SimpleHashMap,
    SimpleBTree,
    SimpleVec,
    SimpleImmutableVector,
    SimpleSpine
}

fn main() {
    let matches = Command::new("shapiro-bencher")
        .version("0.6.0")
        .about("Benches the time taken to reason over .nt files")
        .arg(
            Arg::new("DATA_PATH")
                .help("Sets the data file path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("PROGRAM")
                .help("Sets the program file path")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("PARALLEL")
                .help("Sets whether the reasoner should run single-threaded or in parallel")
                .required(true)
                .index(3),
        )
        .arg(
            Arg::new("REASONER")
                .help("Sets the reasoner to be used, chibi or simple")
                .required(true)
                .index(4),
        )
        .arg(
            Arg::new("INTERN")
                .help("Sets whether strings should be interned")
                .required(true)
                .index(5)
        )
        .arg(
            Arg::new("BATCH")
                .help("Sets the batch size, from 0-1.0")
                .required(false)
                .index(6),
        )
        .get_matches();

    let data_path: String = matches.value_of("DATA_PATH").unwrap().to_string();
    let program_path: String = matches.value_of("PROGRAM_PATH").unwrap().to_string();
    let parallel: bool = matches.value_of("PARALLEL").unwrap().parse().unwrap();
    let reasoner: Reasoners = match matches.value_of("REASONER").unwrap() {
        "chibi" => Chibi,
        "simple-hashmap" => SimpleHashMap,
        "simple-btree" => SimpleBTree,
        "simple-vec" => SimpleVec,
        "simple-immutable-vector" => SimpleImmutableVector,
        "simple-spine" => SimpleSpine,
        other => panic!("unknown reasoner variant: {}", other),
    };
    let intern: bool = matches.value_of("INTERN").unwrap().parse().unwrap();
    let batch_size: f64 = matches
        .value_of("BATCH_SIZE")
        .unwrap()
        .parse::<f64>()
        .unwrap();

    let evaluator: Materializer = match reasoner {
        Chibi => ChibiDatalog::new(parallel, intern),
        SimpleHashMap => SimpleDatalog::new(parallel, intern),
        SimpleBTree => {}
        SimpleVec => {}
        SimpleImmutableVector => {}
        SimpleSpine => {}
    }

    infer_reasoner.materialize(&program);

    println!("starting bench");
    let mut now = Instant::now();

    println!("reasoning time - simple: {} ms", now.elapsed().as_millis());
}
