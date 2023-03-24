use crate::misc::helpers::{
    idempotent_intern, idempotent_program_strong_intern, idempotent_program_weak_intern, ty_to_row,
};
use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, Rule, SugaredProgram, SugaredRule, Ty};
use crate::models::instance::{Database, HashSetDatabase};
use crate::models::reasoner::{
    BottomUpEvaluator, Diff, Dynamic, DynamicTyped, EvaluationResult, Materializer, Queryable,
    RelationDropper,
};
use crate::models::relational_algebra::Row;
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::evaluation::{
    Evaluation, IncrementalEvaluation, InstanceEvaluator, Set,
};
use crate::reasoning::algorithms::rewriting::evaluate_rule;
use lasso::{Key, Spur};
use rayon::prelude::*;
use std::time::Instant;
use crate::reasoning::algorithms::delta_rule_rewrite::{DELTA_PREFIX, deltaify_idb, make_sne_programs};

pub struct Rewriting {
    pub program: Program,
    pub sugared_program: SugaredProgram,
}

impl Rewriting {
    fn new(program: &Program, sugared_program: &SugaredProgram) -> Self {
        return Rewriting {
            program: program.clone(),
            sugared_program: sort_program(sugared_program),
        };
    }
}

impl InstanceEvaluator<HashSetDatabase> for Rewriting {
    fn evaluate(&self, instance: HashSetDatabase) -> HashSetDatabase {
        let mut out: HashSetDatabase = Default::default();

        self.program
            .iter()
            .enumerate()
            .for_each(|(rule_idx, rule)| {
                let now = Instant::now();
                if let Some(eval) = evaluate_rule(&instance, &rule) {
                    //println!("{} : {}", self.sugared_program[rule_idx], now.elapsed().as_micros());
                    eval.into_iter()
                        .for_each(|row| out.insert_at(rule.head.relation_id.get(), row))
                }
            });

        return out;
    }
}

pub struct ParallelRewriting {
    pub program: Vec<Rule>,
}

impl ParallelRewriting {
    fn new(program: &Vec<Rule>) -> Self {
        return ParallelRewriting {
            program: program.clone(),
        };
    }
}

impl InstanceEvaluator<HashSetDatabase> for ParallelRewriting {
    fn evaluate(&self, instance: HashSetDatabase) -> HashSetDatabase {
        let mut out: HashSetDatabase = Default::default();

        self.program
            .par_iter()
            .filter_map(|rule| {
                if let Some(eval) = evaluate_rule(&instance, &rule) {
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
}

pub struct ChibiDatalog {
    // Extensional database
    pub edb: HashSetDatabase,
    // Intensional database
    pub idb: HashSetDatabase,
    pub(crate) interner: Interner,
    parallel: bool,
    intern: bool,
    program: Program,
    sugared_program: SugaredProgram,
}

impl Default for ChibiDatalog {
    fn default() -> Self {
        ChibiDatalog {
            edb: Default::default(),
            idb: Default::default(),
            interner: Default::default(),
            parallel: true,
            intern: true,
            program: vec![],
            sugared_program: vec![],
        }
    }
}

impl ChibiDatalog {
    pub fn new(parallel: bool, intern: bool) -> Self {
        return Self {
            parallel,
            intern,
            ..Default::default()
        };
    }
    fn new_evaluation(&self, [delta, nonrecursive, recursive]: [Box<ParallelRewriting>; 3]) -> IncrementalEvaluation<HashSetDatabase> {
        return IncrementalEvaluation::new(delta, nonrecursive, recursive)
    }
    fn update_materialization(&mut self) {
        let deltaifier = deltaify_idb(&self.sugared_program);
        let (nonrecursive, recursive) = make_sne_programs(&self.sugared_program);

        let programs = [deltaifier, nonrecursive, recursive];
        let instance_evaluators = programs.map(|sugared_program| {
            let savory_program = idempotent_program_strong_intern(&mut self.interner, self.intern, &sugared_program);
            return Box::new(ParallelRewriting::new(&savory_program))
        });

        let mut evaluation = self.new_evaluation(instance_evaluators);

        evaluation.semi_naive(&self.edb);

        evaluation
            .output
            .storage
            .into_iter()
            .for_each(|(relation_id, relation)| {
                relation.into_iter().for_each(|row| {
                    self.idb.insert_at(relation_id, row);
                });
            });
    }
}

impl Dynamic for ChibiDatalog {
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        self.insert_typed(table, ty_to_row(&row))
    }

    fn delete(&mut self, table: &str, row: &Vec<Box<dyn Ty>>) {
        self.delete_typed(table, &ty_to_row(row))
    }
}

impl DynamicTyped for ChibiDatalog {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let (relation_id, typed_row) =
            idempotent_intern(&mut self.interner, self.intern, table, row);

        self.edb.insert_at(relation_id, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: &Row) {
        let (relation_id, typed_row) =
            idempotent_intern(&mut self.interner, self.intern, table, row.clone());

        self.edb.delete_at(relation_id, &typed_row)
    }
}

impl BottomUpEvaluator for ChibiDatalog {
    fn evaluate_program_bottom_up(&mut self, program: &Vec<SugaredRule>) -> EvaluationResult {
        let deltaifier = deltaify_idb(program);
        let (nonrecursive, recursive) = make_sne_programs(program);

        let programs = [deltaifier, nonrecursive, recursive];
        let instance_evaluators = programs.map(|sugared_program| {
            let savory_program = idempotent_program_strong_intern(&mut self.interner, self.intern, &sugared_program);
            return Box::new(ParallelRewriting::new(&savory_program))
        });

        let mut evaluation = self.new_evaluation(instance_evaluators);

        evaluation.semi_naive(&self.edb.union(&self.idb));

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

        self.sugared_program = sort_program(&self.sugared_program);

        self.program = self
            .sugared_program
            .iter()
            .map(|sugared_rule| self.interner.intern_rule_weak(&sugared_rule))
            .collect();

        self.update_materialization()
    }

    // Update first processes deletions, then additions.
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
            additions.into_iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row);
            });

            self.update_materialization();
        }

    }



    fn triple_count(&self) -> usize {
        let edb_size: usize = self
            .edb
            .storage
            .iter()
            .map(|(_sym, rel)| return rel.len())
            .sum();

        let idb_size: usize = self
            .idb
            .storage
            .iter()
            .map(|(sym, rel)| return {
                let spur = Spur::try_from_usize(*sym as usize - 1).unwrap();
                let actual_string = self.interner.rodeo.try_resolve(&spur).unwrap();

                if !actual_string.contains(DELTA_PREFIX) {
                    return rel.len()
                }

                return 0
            })
            .sum();

        return edb_size + idb_size;
    }
}

impl RelationDropper for ChibiDatalog {
    fn drop_relation(&mut self, table: &str) {
        let sym = self.interner.rodeo.get_or_intern(table);

        self.edb.storage.remove(&sym.into_inner().get());
    }
}

impl Queryable for ChibiDatalog {
    fn contains_row(&self, table: &str, row: &Vec<Box<dyn Ty>>) -> bool {
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
                .edb
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
        let mut reasoner: ChibiDatalog = ChibiDatalog::new(false, false);

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
        let mut reasoner: ChibiDatalog = ChibiDatalog::new(false, false);
        reasoner.insert("edge", vec![Box::new("a"), Box::new("b")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("c")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("d")]);

        let new_tuples = reasoner
            .evaluate_program_bottom_up(&vec![
                SugaredRule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
                SugaredRule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
            ])
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
