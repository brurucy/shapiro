use shapiro::implementations::datalog_positive_relalg::SimpleDatalog;
use shapiro::models::datalog::{BottomUpEvaluator, Rule};
use shapiro::ChibiDatalog;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use lasso::{Key, Rodeo};

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

pub fn load3enc<'a>(
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
    let mut grand_ole_pry = Rodeo::default();
    let rdf_type = grand_ole_pry.get_or_intern("rdf:type");
    let rdfs_domain = grand_ole_pry.get_or_intern("rdfs:domain");
    let rdfs_range = grand_ole_pry.get_or_intern("rdfs:range");
    let rdf_spo  = grand_ole_pry.get_or_intern("rdfs:subPropertyOf");
    let rdf_sco  = grand_ole_pry.get_or_intern("rdfs:subClassOf");

    let program = vec![
        Rule::from(& format!("T(?y, {}, ?x) <- [T(?a, {}, ?x), T(?y, ?a, ?z)]", rdf_type.into_usize(), rdfs_domain.into_usize())),
        Rule::from(&format!("T(?z, {}, ?x) <- [T(?a, {}, ?x), T(?y, ?a, ?z)]", rdf_type.into_usize(), rdfs_range.into_usize())),
        Rule::from(&format!("T(?x, {}, ?z) <- [T(?x, {}, ?y), T(?y, {}, ?z)]", rdf_spo.into_usize(), rdf_spo.into_usize(), rdf_spo.into_usize())),
        Rule::from("T(?x, rdfs:subClassOf, ?z) <- [T(?x, rdfs:subClassOf, ?y), T(?y, rdfs:subClassOf, ?z)]"),
        Rule::from("T(?z, rdf:type, ?y) <- [T(?x, rdfs:subClassOf, ?y), T(?z, rdf:type, ?x)]"),
        Rule::from("T(?x, ?b, ?y) <- [T(?a, rdfs:subPropertyOf, ?b), T(?x, ?a, ?y)]"),
    ];

    //const ABOX_LOCATION: &str = "./data/tiny_abox.nt";
    //const TBOX_LOCATION: &str = "./data/tiny_tbox.nt";
    const ABOX_LOCATION: &str = "./data/real_abox.nt";
    const TBOX_LOCATION: &str = "./data/real_tbox.nt";

    let abox = load3enc(&ABOX_LOCATION).unwrap();
    let tbox = load3enc(&TBOX_LOCATION).unwrap();

    let mut lazy_simple_reasoner: SimpleDatalog = Default::default();
    let mut infer_reasoner: ChibiDatalog = Default::default();

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

        let s = grand_ole_pry.get_or_intern(row.clone().0).into_usize() as u32;
        let p = grand_ole_pry.get_or_intern(predicate).into_usize() as u32;
        let o = grand_ole_pry.get_or_intern(row.clone().1).into_usize() as u32;

        lazy_simple_reasoner.fact_store.insert(
            "T",
            vec![
                Box::new(s),
                Box::new(p),
                Box::new(o),
            ],
        );
        infer_reasoner.fact_store.insert(
            "T",
            vec![Box::new(s), Box::new(p), Box::new(o)],
        )
    });

    println!("starting bench");
    let mut now = Instant::now();
    let simple_triples = lazy_simple_reasoner.evaluate_program_bottom_up(program.clone());
    println!(
        "reasoning time - lazy simple: {} ms",
        now.elapsed().as_millis()
    );
    println!(
        "triples - simple: {}",
        simple_triples.database.get("T").unwrap().ward.len()
    );

    now = Instant::now();
    let infer_triples = infer_reasoner.evaluate_program_bottom_up(program.clone());
    println!("reasoning time - infer: {} ms", now.elapsed().as_millis());
    println!(
        "triples - infer: {}",
        infer_triples.database.get("T").unwrap().ward
            .len()
    );

}
