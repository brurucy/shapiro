extern crate core;

use crate::Reasoners::{
    Chibi, ChibiIndexed, Differential, DifferentialIndexed, RelationalBTree, RelationalHashMap,
    RelationalImmutableVector, RelationalSpine, RelationalVec,
};
use clap::{Arg, Command};
use colored::*;
use phf::phf_map;
use shapiro::models::datalog::{SugaredAtom, SugaredRule, Term, Ty, TypedValue};
use shapiro::models::index::{
    BTreeIndex, HashMapIndex, ImmutableVectorIndex, SpineIndex, VecIndex,
};
use shapiro::models::reasoner::{Diff, Materializer, UntypedRow};
use shapiro::reasoning::algorithms::constant_specialization::specialize_to_constants;
use shapiro::reasoning::reasoners::chibi::ChibiDatalog;
use shapiro::reasoning::reasoners::differential::DifferentialDatalog;
use shapiro::reasoning::reasoners::relational::RelationalDatalog;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader};

static OWL: phf::Map<&'static str, &'static str> = phf_map! {
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" =>"rdf:type",
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>" =>"rdf:rest",
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>" =>"rdf:first",
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>" =>"rdf:nil",
    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>" =>"rdf:Property",
    "<http://www.w3.org/2000/01/rdf-schema#subClassOf>" =>"rdfs:subClassOf",
    "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>" =>"rdfs:subPropertyOf",
    "<http://www.w3.org/2000/01/rdf-schema#domain>" =>"rdfs:domain",
    "<http://www.w3.org/2000/01/rdf-schema#range>" =>"rdfs:range",
    "<http://www.w3.org/2000/01/rdf-schema#comment>" =>"rdfs:comment",
    "<http://www.w3.org/2000/01/rdf-schema#label>" =>"rdfs:label",
    "<http://www.w3.org/2000/01/rdf-schema#Literal>" =>"rdfs:Literal",
    "<http://www.w3.org/2002/07/owl#TransitiveProperty>" =>"owl:TransitiveProperty",
    "<http://www.w3.org/2002/07/owl#inverseOf>" =>"owl:inverseOf",
    "<http://www.w3.org/2002/07/owl#Thing>" =>"owl:Thing",
    "<http://www.w3.org/2002/07/owl#maxQualifiedCardinality>" =>"owl:maxQualifiedCardinality",
    "<http://www.w3.org/2002/07/owl#someValuesFrom>" =>"owl:someValuesFrom",
    "<http://www.w3.org/2002/07/owl#equivalentClass>" =>"owl:equivalentClass",
    "<http://www.w3.org/2002/07/owl#intersectionOf>" =>"owl:intersectionOf",
    "<http://www.w3.org/2002/07/owl#members>" =>"owl:members",
    "<http://www.w3.org/2002/07/owl#equivalentProperty>" =>"owl:equivalentProperty",
    "<http://www.w3.org/2002/07/owl#onProperty>" =>"owl:onProperty",
    "<http://www.w3.org/2002/07/owl#propertyChainAxiom>" =>"owl:propertyChainAxiom",
    "<http://www.w3.org/2002/07/owl#disjointWith>" =>"owl:disjointWith",
    "<http://www.w3.org/2002/07/owl#propertyDisjointWith>" =>"owl:propertyDisjointWith",
    "<http://www.w3.org/2002/07/owl#unionOf>" =>"owl:unionOf",
    "<http://www.w3.org/2002/07/owl#hasKey>" =>"owl:hasKey",
    "<http://www.w3.org/2002/07/owl#allValuesFrom>" =>"owl:allValuesFrom",
    "<http://www.w3.org/2002/07/owl#complementOf>" =>"owl:complementOf",
    "<http://www.w3.org/2002/07/owl#onClass>" =>"owl:onClass",
    "<http://www.w3.org/2002/07/owl#distinctMembers>" =>"owl:distinctMembers",
    "<http://www.w3.org/2002/07/owl#FunctionalProperty>" =>"owl:FunctionalProperty",
    "<http://www.w3.org/2002/07/owl#NamedIndividual>" =>"owl:NamedIndividual",
    "<http://www.w3.org/2002/07/owl#ObjectProperty>" =>"owl:ObjectProperty",
    "<http://www.w3.org/2002/07/owl#Class>" =>"owl:Class",
    "<http://www.w3.org/2002/07/owl#AllDisjointClasses>" =>"owl:AllDisjointClasses",
    "<http://www.w3.org/2002/07/owl#Restriction>" =>"owl:Restriction",
    "<http://www.w3.org/2002/07/owl#DatatypeProperty>" =>"owl:DatatypeProperty",
    "<http://www.w3.org/2002/07/owl#Ontology>" =>"owl:Ontology",
    "<http://www.w3.org/2002/07/owl#AsymmetricProperty>" =>"owl:AsymmetricProperty",
    "<http://www.w3.org/2002/07/owl#SymmetricProperty>" =>"owl:SymmetricProperty",
    "<http://www.w3.org/2002/07/owl#IrreflexiveProperty>" =>"owl:IrreflexiveProperty",
    "<http://www.w3.org/2002/07/owl#AllDifferent>" =>"owl:AllDifferent",
    "<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>" =>"owl:InverseFunctionalProperty",
    "<http://www.w3.org/2002/07/owl#sameAs>" =>"owl:sameAs",
    "<http://www.w3.org/2002/07/owl#hasValue>" =>"owl:hasValue",
    "<http://www.w3.org/2002/07/owl#Nothing>" =>"owl:Nothing",
    "<http://www.w3.org/2002/07/owl#oneOf>" =>"owl:oneOf",
};

trait SugaredAtomParser {
    fn parse_line(&self, line: &str) -> SugaredAtom;
}

pub struct NTripleParser;

impl SugaredAtomParser for NTripleParser {
    fn parse_line(&self, line: &str) -> SugaredAtom {
        let mut split_line = line.split(' ');

        let digit_one: String = split_line.next().unwrap().to_string();
        let mut digit_two: String = split_line.next().unwrap().to_string();
        if let Some(alias) = OWL.get(&digit_two) {
            digit_two = alias.to_string();
        }
        let mut digit_three: String = split_line.next().unwrap().to_string();
        if let Some(alias) = OWL.get(&digit_three) {
            digit_three = alias.to_string()
        }

        let terms = vec![
            Term::Constant(TypedValue::Str(digit_one)),
            Term::Constant(TypedValue::Str(digit_two)),
            Term::Constant(TypedValue::Str(digit_three)),
        ];

        return SugaredAtom {
            terms,
            symbol: "T".to_string(),
            positive: true,
        };
    }
}

pub struct SpaceSepParser;

impl SugaredAtomParser for SpaceSepParser {
    fn parse_line(&self, line: &str) -> SugaredAtom {
        let raw_terms: Vec<&str> = line.split(' ').collect();

        let symbol = raw_terms[raw_terms.len() - 1];

        let terms = raw_terms[..raw_terms.len() - 1]
            .into_iter()
            .map(|term| Term::Constant(term.to_typed_value()))
            .collect();

        return SugaredAtom {
            terms,
            symbol: symbol.to_string(),
            positive: true,
        };
    }
}

struct Parser {
    atom_parser: Box<dyn SugaredAtomParser>,
}

fn read_file(filename: &str) -> Result<impl Iterator<Item = String>, &'static str> {
    return if let Ok(file) = File::open(filename) {
        let buffer = BufReader::new(file);

        Ok(buffer.lines().filter_map(|line| line.ok()))
    } else {
        Err("fail to open file")
    };
}

impl Parser {
    fn read_fact_file(&self, filename: &str) -> impl Iterator<Item = SugaredAtom> + '_ {
        match read_file(filename) {
            Ok(file) => {
                return file
                    .into_iter()
                    .map(|line| self.atom_parser.parse_line(&line));
            }
            Err(e) => {
                panic!("{}", e)
            }
        }
    }
    fn read_datalog_file(&self, filename: &str) -> impl Iterator<Item = SugaredRule> + '_ {
        match read_file(filename) {
            Ok(file) => {
                return file
                    .into_iter()
                    .map(|line| SugaredRule::from(line.as_str()))
            }
            Err(e) => {
                panic!("{}", e)
            }
        }
    }
}

pub enum Reasoners {
    Chibi,
    ChibiIndexed,
    Differential,
    DifferentialIndexed,
    RelationalHashMap,
    RelationalBTree,
    RelationalVec,
    RelationalImmutableVector,
    RelationalSpine,
}

impl Display for Reasoners {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Chibi => write!(f, "chibi"),
            ChibiIndexed => write!(f, "chibi"),
            Differential => write!(f, "differential"),
            DifferentialIndexed => write!(f, "differential-tabled"),
            RelationalHashMap => write!(f, "relational-hashmap"),
            RelationalBTree => write!(f, "relational-btree"),
            RelationalVec => write!(f, "relational-vec"),
            RelationalImmutableVector => write!(f, "relational-immutable-vector"),
            RelationalSpine => write!(f, "relational-spine"),
        }
    }
}

fn main() {
    let matches = Command::new("shapiro-bencher")
        .version("0.7.0")
        .about(
            "Benches the time taken to reason over relational space-separated facts or .nt files",
        )
        .arg(
            Arg::new("DATA_PATH")
                .help("Sets the data file path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("PROGRAM_PATH")
                .help("Sets the program file path")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("REASONER")
                .help("Sets the reasoner to be used, chibi, relational or differential")
                .required(true)
                .index(3),
        )
        .arg(
            Arg::new("PARALLEL")
                .help("Sets whether the reasoner should run single-threaded or in parallel")
                .required(true)
                .index(4),
        )
        .arg(
            Arg::new("INTERN")
                .help("Sets whether the reasoner should intern string values")
                .required(true)
                .index(5),
        )
        .arg(
            Arg::new("BATCH_SIZE")
                .help("Sets the batch size, from 0-1.0")
                .required(true)
                .index(6),
        )
        .arg(
            Arg::new("PARSER")
                .help("Sets the parser, nt or free")
                .required(true)
                .index(7),
        )
        .arg(
            Arg::new("SPECIALIZE")
                .help("Specializes the constants in the program unto their own relations")
                .required(true)
                .index(8),
        )
        .get_matches();

    let data_path: String = matches.value_of("DATA_PATH").unwrap().to_string();
    let program_path: String = matches.value_of("PROGRAM_PATH").unwrap().to_string();
    let parallel: bool = matches.value_of("PARALLEL").unwrap().parse().unwrap();
    let intern: bool = matches.value_of("INTERN").unwrap().parse().unwrap();
    let specialize: bool = matches.value_of("SPECIALIZE").unwrap().parse().unwrap();
    let reasoner: Reasoners = match matches.value_of("REASONER").unwrap() {
        "chibi" => Chibi,
        "chibi-indexed" => ChibiIndexed,
        "differential" => Differential,
        "differential-indexed" => DifferentialIndexed,
        "relational-hashmap" => RelationalHashMap,
        "relational-btree" => RelationalBTree,
        "relational-vec" => RelationalVec,
        "relational-immutable-vector" => RelationalImmutableVector,
        "relational-spine" => RelationalSpine,
        other => panic!("unknown reasoner variant: {}", other),
    };
    let batch_size: f64 = matches
        .value_of("BATCH_SIZE")
        .unwrap()
        .parse::<f64>()
        .unwrap();
    let line_parser: Box<dyn SugaredAtomParser> = match matches.value_of("PARSER").unwrap() {
        "nt" => Box::new(NTripleParser),
        _ => Box::new(SpaceSepParser),
    };
    let parser = Parser {
        atom_parser: line_parser,
    };

    let mut evaluator: Box<dyn Materializer> = match reasoner {
        Chibi => Box::new(ChibiDatalog::new(parallel, intern, false)),
        ChibiIndexed => Box::new(ChibiDatalog::new(parallel, intern, true)),
        Differential => Box::new(DifferentialDatalog::new(parallel, false)),
        DifferentialIndexed => Box::new(DifferentialDatalog::new(parallel, true)),
        RelationalHashMap => Box::new(RelationalDatalog::<HashMapIndex>::new(parallel, intern)),
        RelationalBTree => Box::new(RelationalDatalog::<BTreeIndex>::new(parallel, intern)),
        RelationalVec => Box::new(RelationalDatalog::<VecIndex>::new(parallel, intern)),
        RelationalImmutableVector => Box::new(RelationalDatalog::<ImmutableVectorIndex>::new(
            parallel, intern,
        )),
        RelationalSpine => Box::new(RelationalDatalog::<SpineIndex>::new(parallel, intern)),
    };

    let facts: Vec<SugaredAtom> = parser.read_fact_file(&data_path).collect();
    let cutoff: usize = (facts.len() as f64 * batch_size) as usize;

    let batch_size: usize = {
        if cutoff == 0 {
            facts.len()
        } else {
            cutoff
        }
    };

    println!(
        "data: {}\nprogram: {}\nparallel: {}\nintern: {}\nreasoner: {}\nbatch_size: {}",
        data_path, program_path, parallel, intern, reasoner, batch_size
    );

    let mut sugared_program = parser.read_datalog_file(&program_path).collect();
    if specialize {
        sugared_program = specialize_to_constants(&sugared_program);
    }

    let mut initial_materialization: Vec<Diff> = vec![];
    let mut positive_update: Vec<Diff> = vec![];
    let mut negative_update: Vec<Diff> = vec![];

    facts.iter().enumerate().for_each(|(idx, atom)| {
        let sym = atom.symbol.as_str();
        let terms: UntypedRow = atom
            .terms
            .iter()
            .map(|term| match term {
                Term::Constant(inner) => return inner.clone().into(),
                _ => unreachable!(),
            })
            .collect();

        if idx < batch_size {
            initial_materialization.push((true, (sym, terms)))
        } else {
            positive_update.push((true, (sym, terms)));

            let negative_terms: UntypedRow = atom
                .terms
                .iter()
                .map(|term| match term {
                    Term::Constant(inner) => return inner.clone().into(),
                    _ => unreachable!(),
                })
                .collect();

            negative_update.push((false, (sym, negative_terms)));
        }
    });

    evaluator.materialize(&sugared_program);
    println!("{}", "Initial materialization".purple());
    evaluator.update(initial_materialization);
    println!("triples: {}", evaluator.triple_count());

    println!("{}", "Positive Update".purple());
    evaluator.update(positive_update);
    println!("triples: {}", evaluator.triple_count());

    println!("{}", "Negative Update".purple());
    evaluator.update(negative_update);
    println!("triples: {}", evaluator.triple_count());
}
