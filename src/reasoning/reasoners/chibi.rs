use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use crate::misc::helpers::{
    idempotent_intern, idempotent_program_strong_intern, idempotent_program_weak_intern, ty_to_row,
};
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, SugaredProgram, SugaredRule};
use crate::models::instance::{Database, HashSetDatabase};
use crate::models::reasoner::{
    BottomUpEvaluator, Diff, Dynamic, DynamicTyped, EvaluationResult, Materializer, Queryable,
    RelationDropper, UntypedRow,
};
use crate::models::relational_algebra::Row;
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::delta_rule_rewrite::{DELTA_PREFIX, deltaify_idb, make_sne_programs, make_update_sne_programs};
use crate::reasoning::algorithms::evaluation::{
    ImmediateConsequenceOperator, IncrementalEvaluation,
};
use crate::reasoning::algorithms::rewriting::evaluate_rule;
use colored::Colorize;
use lasso::{Key, Spur};
use rayon::prelude::*;
use std::time::Instant;
use phf::phf_map;

static OWL_INV: phf::Map<&'static str, &'static str> = phf_map! {
    "rdf:type" => "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
    "rdf:rest" => "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>",
    "rdf:first" => "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>",
    "rdf:nil" => "<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>",
    "rdf:Property" => "<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>",
    "rdfs:subClassOf" => "<http://www.w3.org/2000/01/rdf-schema#subClassOf>",
    "rdfs:subPropertyOf" => "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>",
    "rdfs:domain" => "<http://www.w3.org/2000/01/rdf-schema#domain>",
    "rdfs:range" => "<http://www.w3.org/2000/01/rdf-schema#range>",
    "rdfs:comment" => "<http://www.w3.org/2000/01/rdf-schema#comment>",
    "rdfs:label" => "<http://www.w3.org/2000/01/rdf-schema#label>",
    "rdfs:Literal" => "<http://www.w3.org/2000/01/rdf-schema#Literal>",
    "owl:TransitiveProperty" => "<http://www.w3.org/2002/07/owl#TransitiveProperty>",
    "owl:inverseOf" => "<http://www.w3.org/2002/07/owl#inverseOf>",
    "owl:Thing" => "<http://www.w3.org/2002/07/owl#Thing>",
    "owl:maxQualifiedCardinality" => "<http://www.w3.org/2002/07/owl#maxQualifiedCardinality>",
    "owl:someValuesFrom" => "<http://www.w3.org/2002/07/owl#someValuesFrom>",
    "owl:equivalentClass" => "<http://www.w3.org/2002/07/owl#equivalentClass>",
    "owl:intersectionOf" => "<http://www.w3.org/2002/07/owl#intersectionOf>",
    "owl:members" => "<http://www.w3.org/2002/07/owl#members>",
    "owl:equivalentProperty" => "<http://www.w3.org/2002/07/owl#equivalentProperty>",
    "owl:onProperty" => "<http://www.w3.org/2002/07/owl#onProperty>",
    "owl:propertyChainAxiom" => "<http://www.w3.org/2002/07/owl#propertyChainAxiom>",
    "owl:disjointWith" => "<http://www.w3.org/2002/07/owl#disjointWith>",
    "owl:propertyDisjointWith" => "<http://www.w3.org/2002/07/owl#propertyDisjointWith>",
    "owl:unionOf" => "<http://www.w3.org/2002/07/owl#unionOf>",
    "owl:hasKey" => "<http://www.w3.org/2002/07/owl#hasKey>",
    "owl:allValuesFrom" => "<http://www.w3.org/2002/07/owl#allValuesFrom>",
    "owl:complementOf" => "<http://www.w3.org/2002/07/owl#complementOf>",
    "owl:onClass" => "<http://www.w3.org/2002/07/owl#onClass>",
    "owl:distinctMembers" => "<http://www.w3.org/2002/07/owl#distinctMembers>",
    "owl:FunctionalProperty" => "<http://www.w3.org/2002/07/owl#FunctionalProperty>",
    "owl:NamedIndividual" => "<http://www.w3.org/2002/07/owl#NamedIndividual>",
    "owl:ObjectProperty" => "<http://www.w3.org/2002/07/owl#ObjectProperty>",
    "owl:Class" => "<http://www.w3.org/2002/07/owl#Class>",
    "owl:AllDisjointClasses" => "<http://www.w3.org/2002/07/owl#AllDisjointClasses>",
    "owl:Restriction" => "<http://www.w3.org/2002/07/owl#Restriction>",
    "owl:DatatypeProperty" => "<http://www.w3.org/2002/07/owl#DatatypeProperty>",
    "owl:Ontology" => "<http://www.w3.org/2002/07/owl#Ontology>",
    "owl:AsymmetricProperty" => "<http://www.w3.org/2002/07/owl#AsymmetricProperty>",
    "owl:SymmetricProperty" => "<http://www.w3.org/2002/07/owl#SymmetricProperty>",
    "owl:IrreflexiveProperty" => "<http://www.w3.org/2002/07/owl#IrreflexiveProperty>",
    "owl:AllDifferent" => "<http://www.w3.org/2002/07/owl#AllDifferent>",
    "owl:InverseFunctionalProperty" => "<http://www.w3.org/2002/07/owl#InverseFunctionalProperty>",
    "owl:sameAs" => "<http://www.w3.org/2002/07/owl#sameAs>",
    "owl:hasValue" => "<http://www.w3.org/2002/07/owl#hasValue>",
    "owl:Nothing" => "<http://www.w3.org/2002/07/owl#Nothing>",
    "owl:oneOf" => "<http://www.w3.org/2002/07/owl#oneOf>",
};

pub fn evaluate_rules_sequentially(
    program: &Program,
    instance: &HashSetDatabase,
    index: bool,
) -> HashSetDatabase {
    let mut out: HashSetDatabase = Default::default();

    program.iter().for_each(|rule| {
        if let Some(eval) = evaluate_rule(&instance, &rule, index) {
            eval.into_iter()
                .for_each(|row| out.insert_at(rule.head.relation_id.get(), row))
        }
    });

    return out;
}

pub fn evaluate_rules_in_parallel(
    program: &Program,
    instance: &HashSetDatabase,
    index: bool,
) -> HashSetDatabase {
    let mut out: HashSetDatabase = Default::default();

    program
        .par_iter()
        .filter_map(|rule| {
            if let Some(eval) = evaluate_rule(instance, &rule, index) {
                return Some((rule.head.relation_id.get(), eval));
            }

            return None;
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|(relation_id, eval)| {
            eval.into_iter()
                .for_each(|row| out.insert_at(relation_id, row))
        });

    return out;
}

pub struct Rewriting {
    pub nonrecursive_program: Program,
    pub recursive_program: Program,
    pub deltaifying_program: Program,
    pub index: bool,
}

impl Rewriting {
    fn new(
        nonrecursive_program: &Program,
        recursive_program: &Program,
        deltaifying_program: &Program,
        index: bool,
    ) -> Self {
        return Rewriting {
            nonrecursive_program: nonrecursive_program.clone(),
            recursive_program: recursive_program.clone(),
            deltaifying_program: deltaifying_program.clone(),
            index,
        };
    }
}

impl ImmediateConsequenceOperator<HashSetDatabase> for Rewriting {
    fn deltaify_idb(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return deltaify_idb_by_renaming(&self.deltaifying_program, fact_store);
    }

    fn nonrecursive_program(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return evaluate_rules_sequentially(&self.nonrecursive_program, fact_store, self.index);
    }

    fn recursive_program(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return evaluate_rules_sequentially(&self.recursive_program, fact_store, self.index);
    }
}

pub struct ParallelRewriting {
    pub nonrecursive_program: Program,
    pub recursive_program: Program,
    pub deltaifying_program: Program,
    pub index: bool,
}

impl ParallelRewriting {
    fn new(
        nonrecursive_program: &Program,
        recursive_program: &Program,
        deltaifying_program: &Program,
        index: bool,
    ) -> Self {
        return ParallelRewriting {
            nonrecursive_program: nonrecursive_program.clone(),
            recursive_program: recursive_program.clone(),
            deltaifying_program: deltaifying_program.clone(),
            index,
        };
    }
}

pub fn deltaify_idb_by_renaming(
    deltaify_idb_program: &Program,
    fact_store: &HashSetDatabase,
) -> HashSetDatabase {
    let mut out = fact_store.clone();
    deltaify_idb_program.iter().for_each(|rule| {
        if let Some(relation) = fact_store.storage.get(&(rule.body[0].relation_id.get())) {
            out.storage.insert(rule.head.relation_id.get(), relation.clone());
        }
    });

    return out;
}

impl ImmediateConsequenceOperator<HashSetDatabase> for ParallelRewriting {
    fn deltaify_idb(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return deltaify_idb_by_renaming(&self.deltaifying_program, fact_store);
    }

    fn nonrecursive_program(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return evaluate_rules_in_parallel(&self.nonrecursive_program, fact_store, self.index);
    }

    fn recursive_program(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return evaluate_rules_in_parallel(&self.recursive_program, fact_store, self.index);
    }
}

pub struct ChibiDatalog {
    pub fact_store: HashSetDatabase,
    pub(crate) interner: Interner,
    parallel: bool,
    intern: bool,
    index: bool,
    program: Program,
    sugared_program: SugaredProgram,
}

impl Default for ChibiDatalog {
    fn default() -> Self {
        ChibiDatalog {
            fact_store: Default::default(),
            interner: Default::default(),
            parallel: true,
            intern: true,
            index: true,
            program: vec![],
            sugared_program: vec![],
        }
    }
}

impl ChibiDatalog {
    pub fn new(parallel: bool, intern: bool, index: bool) -> Self {
        return Self {
            parallel,
            intern,
            index,
            ..Default::default()
        };
    }
    fn new_evaluation(
        &self,
        immediate_consequence_operator: Box<dyn ImmediateConsequenceOperator<HashSetDatabase>>,
    ) -> IncrementalEvaluation<HashSetDatabase> {
        return IncrementalEvaluation::new(immediate_consequence_operator);
    }
    fn update_materialization(&mut self) {
        let evaluation = self.evaluate_program_bottom_up(&self.sugared_program.clone());

        evaluation.into_iter().for_each(|(symbol, relation)| {
            relation.into_iter().for_each(|row| {
                self.insert_typed(&symbol, row);
            });
        });
    }
}

impl Dynamic for ChibiDatalog {
    fn insert(&mut self, table: &str, row: UntypedRow) {
        self.insert_typed(table, ty_to_row(&row))
    }

    fn delete(&mut self, table: &str, row: &UntypedRow) {
        self.delete_typed(table, &ty_to_row(row))
    }
}

impl DynamicTyped for ChibiDatalog {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let (relation_id, typed_row) =
            idempotent_intern(&mut self.interner, self.intern, table, row);

        self.fact_store.insert_at(relation_id, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: &Row) {
        let (relation_id, typed_row) =
            idempotent_intern(&mut self.interner, self.intern, table, row.clone());

        self.fact_store.delete_at(relation_id, &typed_row)
    }
}

impl BottomUpEvaluator for ChibiDatalog {
    fn evaluate_program_bottom_up(&mut self, program: &Vec<SugaredRule>) -> EvaluationResult {
        let deltaifier = deltaify_idb(program);
        let (nonrecursive, recursive) = make_update_sne_programs(program);
        let programs: Vec<_> = [nonrecursive, recursive, deltaifier]
            .into_iter()
            .map(|sugared_program| {
                return idempotent_program_strong_intern(
                    &mut self.interner,
                    self.intern,
                    &sugared_program,
                );
            })
            .collect();

        let im_op = Box::new(ParallelRewriting::new(
            &programs[0],
            &programs[1],
            &programs[2],
            self.index,
        ));
        let mut evaluation = self.new_evaluation(im_op);
        if !self.parallel {
            evaluation.immediate_consequence_operator = Box::new(Rewriting::new(
                &programs[0],
                &programs[1],
                &programs[2],
                self.index,
            ));
        }

        let now = Instant::now();
        evaluation.semi_naive(&self.fact_store);
        println!(
            "{} {}",
            "inference time:".green(),
            now.elapsed().as_millis().to_string().green()
        );

        return evaluation.output.storage.into_iter().fold(
            Default::default(),
            |mut acc: EvaluationResult, (relation_id, row_set)| {
                let spur = Spur::try_from_usize(relation_id as usize - 1).unwrap();
                let sym = self.interner.rodeo.resolve(&spur);

                acc.insert(sym.to_string(), row_set);
                acc
            },
        );
    }
}

impl Materializer for ChibiDatalog {
    fn materialize(&mut self, program: &SugaredProgram) {
        idempotent_program_weak_intern(&mut self.interner, self.intern, program)
            .into_iter()
            .for_each(|sugared_rule| self.sugared_program.push(sugared_rule));

        self.program = self
            .sugared_program
            .iter()
            .map(|sugared_rule| self.interner.intern_rule_weak(&sugared_rule))
            .collect();

        self.update_materialization()
    }

    fn update(&mut self, changes: Vec<Diff>) {
        let mut additions: Vec<(&str, Row)> = vec![];
        let mut retractions: Vec<(&str, Row)> = vec![];

        changes.iter().for_each(|(sign, (sym, value))| {
            let typed_row: Row = ty_to_row(value);

            if *sign {
                additions.push((sym, typed_row));
            } else {
                retractions.push((sym, typed_row));
            }
        });

        if retractions.len() > 0 {
            delete_rederive(self, &self.sugared_program.clone(), retractions)
        }

        if additions.len() > 0 {
            additions.iter().for_each(|(sym, row)| {
                self.insert_typed(&format!("{}{}", DELTA_PREFIX, sym), row.clone());
            });

            self.update_materialization();

            additions.into_iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row.clone());
                self.delete_typed(&format!("{}{}", DELTA_PREFIX, sym), &row);
            });
        }
    }

    fn triple_count(&self) -> usize {
        return self
            .fact_store
            .storage
            .iter()
            .map(|(_sym, rel)| return rel.len())
            .sum();
    }

    fn dump(&self) {
        let mut file = OpenOptions::new().append(true).create(true).open("mat.nt").unwrap();
        let mut writer = BufWriter::new(file);

        self
            .fact_store
            .storage
            .iter()
            .for_each(|(_relation_id, relation)| {
                relation
                    .iter()
                    .for_each(|row| {
                        let interner = &self.interner;

                        let row0: u32 = row[0].clone().try_into().unwrap();
                        let row1: u32 = row[1].clone().try_into().unwrap();
                        let row2: u32 = row[2].clone().try_into().unwrap();
                        let mut subject = interner.rodeo.resolve(&Spur::try_from_usize(row0 as usize - 1).unwrap());
                        let mut predicate = interner.rodeo.resolve(&Spur::try_from_usize(row1 as usize - 1).unwrap());
                        let mut object = interner.rodeo.resolve(&Spur::try_from_usize(row2 as usize - 1).unwrap());

                        if let Some(alias) = OWL_INV.get(&subject) {
                            subject = alias;
                        }
                        if let Some(alias) = OWL_INV.get(&predicate) {
                            predicate = alias;
                        }
                        if let Some(alias) = OWL_INV.get(&object) {
                            object = alias;
                        }

                        let spo = format!("{} {} {} .", subject, predicate, object);
                        writeln!(writer, "{}", spo).unwrap();
                    })
            });
    }
}

impl RelationDropper for ChibiDatalog {
    fn drop_relation(&mut self, table: &str) {
        let sym = self.interner.rodeo.get_or_intern(table);

        self.fact_store.storage.remove(&sym.into_inner().get());
    }
}

impl Queryable for ChibiDatalog {
    fn contains_row(&self, table: &str, row: &UntypedRow) -> bool {
        if let Some(relation_id) = self.interner.rodeo.get(table) {
            let mut typed_row = ty_to_row(row);
            if self.intern {
                if let Some(existing_typed_row) = self.interner.try_intern_row(&typed_row) {
                    typed_row = existing_typed_row
                } else {
                    return false;
                }
            }
            return self
                .fact_store
                .storage
                .get(&relation_id.into_inner().get())
                .unwrap()
                .contains(&typed_row);
        }

        return false;
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{SugaredRule, TypedValue};
    use crate::models::reasoner::{BottomUpEvaluator, Dynamic, Materializer, Queryable};
    use crate::models::relational_algebra::Row;
    use crate::reasoning::reasoners::chibi::ChibiDatalog;
    use indexmap::IndexSet;

    #[test]
    fn test_chibi_operations() {
        let mut reasoner: ChibiDatalog = ChibiDatalog::new(false, false, true);

        assert!(!reasoner.contains_row("edge", &vec![Box::new("a"), Box::new("b")]));
        assert!(!reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("c")]));
        assert!(!reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("d")]));

        assert_eq!(reasoner.triple_count(), 0);

        reasoner.insert("edge", vec![Box::new("a"), Box::new("b")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("c")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("d")]);

        assert_eq!(reasoner.triple_count(), 3);

        assert!(reasoner.contains_row("edge", &vec![Box::new("a"), Box::new("b")]));
        assert!(reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("c")]));
        assert!(reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("d")]));

        reasoner.delete("edge", &vec![Box::new("a"), Box::new("b")]);
        reasoner.delete("edge", &vec![Box::new("b"), Box::new("c")]);
        reasoner.delete("edge", &vec![Box::new("b"), Box::new("d")]);

        assert_eq!(reasoner.triple_count(), 0);

        assert!(!reasoner.contains_row("edge", &vec![Box::new("a"), Box::new("b")]));
        assert!(!reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("c")]));
        assert!(!reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("d")]));
    }

    #[test]
    fn test_chibi_datalog() {
        let mut reasoner: ChibiDatalog = ChibiDatalog::new(false, false, true);
        reasoner.insert("edge", vec![Box::new("a"), Box::new("b")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("c")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("d")]);

        let query = vec![
            SugaredRule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
            SugaredRule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
        ];

        let new_tuples = reasoner
            .evaluate_program_bottom_up(&query)
            .get("reachable")
            .unwrap()
            .clone();

        let mut expected_new_tuples: IndexSet<Row> = Default::default();

        vec![
            // Rule 1 output
            Box::new([
                TypedValue::Str("a".to_string()),
                TypedValue::Str("b".to_string()),
            ]),
            Box::new([
                TypedValue::Str("b".to_string()),
                TypedValue::Str("c".to_string()),
            ]),
            Box::new([
                TypedValue::Str("b".to_string()),
                TypedValue::Str("d".to_string()),
            ]),
            // Rule 2 output
            Box::new([
                TypedValue::Str("a".to_string()),
                TypedValue::Str("c".to_string()),
            ]),
            Box::new([
                TypedValue::Str("a".to_string()),
                TypedValue::Str("d".to_string()),
            ]),
        ]
        .into_iter()
        .for_each(|row| {
            expected_new_tuples.insert(row);
        });

        assert_eq!(expected_new_tuples, new_tuples)
    }
}
