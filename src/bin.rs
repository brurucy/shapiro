use std::collections::{BTreeMap, BTreeSet};
use shapiro::implementations::datalog_positive_relalg::SimpleDatalog;
use shapiro::models::datalog::{BottomUpEvaluator, Rule, TypedValue};
use shapiro::ChibiDatalog;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use ahash::AHashMap;
use im::{Vector, HashMap};
use shapiro::data_structures::hashmap::IndexedHashMap;
use shapiro::data_structures::spine::Spine;
use shapiro::models::index::ValueRowId;
use shapiro::models::relational_algebra::Row;

fn read_file(filename: &str) -> Result<impl Iterator<Item = String>, &'static str> {
    return if let Ok(file) = File::open(filename) {
        if let buffer = BufReader::new(file) {
            Ok(buffer.lines().filter_map(|line| line.ok()))
        } else {
            Err("fail to make buffer")
        }
    } else {
        Err("fail to open file")
    };
}

pub fn load3ple<'a>(
    filename: &str,
) -> Result<impl Iterator<Item = (String, String, String)> + 'a, &'static str> {
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

fn main() {
    let program = vec![
        Rule::from("T(?y, rdf:type, ?x) <- [T(?a, rdfs:domain, ?x), T(?y, ?a, ?z)]"),
        Rule::from("T(?z, rdf:type, ?x) <- [T(?a, rdfs:range, ?x), T(?y, ?a, ?z)]"),
        Rule::from("T(?x, rdfs:subPropertyOf, ?z) <- [T(?x, rdfs:subPropertyOf, ?y), T(?y, rdfs:subPropertyOf, ?z)]"),
        Rule::from("T(?x, rdfs:subClassOf, ?z) <- [T(?x, rdfs:subClassOf, ?y), T(?y, rdfs:subClassOf, ?z)]"),
        Rule::from("T(?z, rdf:type, ?y) <- [T(?x, rdfs:subClassOf, ?y), T(?z, rdf:type, ?x)]"),
        Rule::from("T(?x, ?b, ?y) <- [T(?a, rdfs:subPropertyOf, ?b), T(?x, ?a, ?y)]"),
    ];

    const ABOX_LOCATION: &str = "./data/real_abox.nt";
    const TBOX_LOCATION: &str = "./data/real_tbox.nt";

    let abox = load3ple(&ABOX_LOCATION).unwrap();
    let tbox = load3ple(&TBOX_LOCATION).unwrap();

    //let mut simple_reasoner: SimpleDatalog<IndexedHashMap<TypedValue, Vec<usize>>> = SimpleDatalog::default();
    //let mut simple_reasoner: SimpleDatalog<Spine<ValueRowId>> = SimpleDatalog::default();
    //let mut simple_reasoner: SimpleDatalog<BTreeSet<ValueRowId>> = SimpleDatalog::default();
    //let mut simple_reasoner: SimpleDatalog<Vec<ValueRowId>> = SimpleDatalog::default();
    //let mut simple_reasoner: SimpleDatalog<Vector<ValueRowId>> = SimpleDatalog::default();
    //let mut simple_reasoner: SimpleDatalog<HashMap<TypedValue, Vec<usize>, ahash::RandomState>> = SimpleDatalog::default();
    let mut infer_reasoner: ChibiDatalog = ChibiDatalog::default();

    abox.chain(tbox).for_each(|row| {
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
        infer_reasoner
            .insert("T", vec![
                Box::new(s),
                Box::new(p),
                Box::new(o)
            ])
    });

    println!("starting bench");
    let mut now = Instant::now();
    let simple_triples = simple_reasoner.evaluate_program_bottom_up(program.clone());
    println!("reasoning time - simple: {} ms", now.elapsed().as_millis());
    println!(
        "triples - simple: {}",
        simple_triples.database.get("T").unwrap().ward.len()
    );

    now = Instant::now();
    let infer_triples = infer_reasoner.evaluate_program_bottom_up(program.clone());
    println!("reasoning time - infer: {} ms", now.elapsed().as_millis());
    println!(
        "triples - infer: {}",
        infer_triples.database.get("T").unwrap().ward.len()
    );
}
