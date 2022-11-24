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

    let program = vec![
        Rule::from("A(?y, rdf:type, ?x) <- [T(?a, rdfs:domain, ?x), A(?y, ?a, ?z)]"),
        Rule::from("A(?z, rdf:type, ?x) <- [T(?a, rdfs:range, ?x), A(?y, ?a, ?z)]"),
        Rule::from("T(?x, rdfs:subPropertyOf, ?z) <- [T(?x, rdfs:subPropertyOf, ?y), T(?y, rdfs:subPropertyOf, ?z)]"),
        Rule::from("T(?x, rdfs:subClassOf, ?z) <- [T(?x, rdfs:subClassOf, ?y), T(?y, rdfs:subClassOf, ?z)]"),
        Rule::from("A(?z, rdf:type, ?y) <- [T(?x, rdfs:subClassOf, ?y), A(?z, rdf:type, ?x)]"),
        Rule::from("A(?x, ?b, ?y) <- [T(?a, rdfs:subPropertyOf, ?b), A(?x, ?a, ?y)]"),
    ];

    const ABOX_LOCATION: &str = "./data/real_abox.nt";
    const TBOX_LOCATION: &str = "./data/real_tbox.nt";

    let abox = load3ple(&ABOX_LOCATION).unwrap();
    let tbox = load3ple(&TBOX_LOCATION).unwrap();

    //let mut simple_reasoner: SimpleDatalog<IndexedHashMapIndex> = SimpleDatalog::default();
    //let mut simple_reasoner: SimpleDatalog<SpineIndex> = SimpleDatalog::default();
    //let mut simple_reasoner: SimpleDatalog<BTreeIndex> = SimpleDatalog::default();
    //let mut simple_reasoner: SimpleDatalog<VecIndex> = SimpleDatalog::default();
    //let mut simple_reasoner: SimpleDatalog<ImmutableVectorIndex> = SimpleDatalog::default();
    let mut simple_reasoner: SimpleDatalog<HashMapIndex> = SimpleDatalog::default();
    let mut infer_reasoner: ChibiDatalog = ChibiDatalog::default();
    infer_reasoner.materialize(&program);

    tbox.for_each(|row| {
        let mut predicate = row.1.clone();
        if predicate.clone().contains("type") {
            predicate = "rdf:type".to_string()
        } else if predicate.clone().contains("domain") {
            predicate = "rdfs:domain".to_string()
        } else if predicate.clone().contains("range") {
            predicate = "rdfs:range".to_string()
        } else if predicate.clone().contains("subPropertyOf") {
            predicate = "rdfs:subPropertyOf".to_string()
        } else if predicate.clone().contains("subClassOf") {
            predicate = "rdfs:subClassOf".to_string()
        }

        let s = row.0;
        let p = predicate;
        let o = row.2;

        simple_reasoner.insert(
            "T",
            vec![
                Box::new(s.clone()),
                Box::new(p.clone()),
                Box::new(o.clone()),
            ]);
        infer_reasoner.insert(
            "T",
            vec![
                Box::new(s),
                Box::new(p),
                Box::new(o),
            ]);
    });

    abox.for_each(|row| {
        let mut predicate = row.1.clone();
        if predicate.clone().contains("type") {
            predicate = "rdf:type".to_string()
        } else if predicate.clone().contains("domain") {
            predicate = "rdfs:domain".to_string()
        } else if predicate.clone().contains("range") {
            predicate = "rdfs:range".to_string()
        } else if predicate.clone().contains("subPropertyOf") {
            predicate = "rdfs:subPropertyOf".to_string()
        } else if predicate.clone().contains("subClassOf") {
            predicate = "rdfs:subClassOf".to_string()
        }

        let s = row.0;
        let p = predicate;
        let o = row.2;

        simple_reasoner.insert(
            "A",
            vec![
                Box::new(s.clone()),
                Box::new(p.clone()),
                Box::new(o.clone()),
            ]);
        infer_reasoner.insert(
            "A",
            vec![
                Box::new(s),
                Box::new(p),
                Box::new(o),
            ]);
    });

    println!("starting bench");
    let mut now = Instant::now();
    let simple_triples = simple_reasoner.evaluate_program_bottom_up(program.clone());
    println!("reasoning time - simple: {} ms", now.elapsed().as_millis());
    println!("triples - simple: {}", simple_triples.view("A").len());

    now = Instant::now();
    let infer_triples = infer_reasoner.evaluate_program_bottom_up(program.clone());
    println!("reasoning time - infer: {} ms", now.elapsed().as_millis());
    println!("triples - infer: {}", infer_triples.view("A").len());
}
