use ahash::{HashMap, HashSet};
use lasso::{Key, Spur};
use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, Rule, SugaredProgram, SugaredRule, Ty, TypedValue};
use crate::models::instance::{Database, SimpleDatabase};
use crate::models::reasoner::{BottomUpEvaluator, Diff, Dynamic, DynamicTyped, EvaluationResult, Flusher, Materializer, RelationDropper};
use crate::models::relational_algebra::Row;
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::evaluation::{Evaluation, InstanceEvaluator};
use crate::reasoning::algorithms::rewriting::evaluate_rule;
use rayon::prelude::*;

pub struct Rewriting {
    pub program: Program,
}

impl Rewriting {
    fn new(program: &Program) -> Self {
        return Rewriting {
            program: program.clone(),
        };
    }
}

impl InstanceEvaluator<SimpleDatabase> for Rewriting {
    fn evaluate(&self, instance: &SimpleDatabase) -> SimpleDatabase {
        let mut out: SimpleDatabase = Default::default();

        self
            .program
            .iter()
            .for_each(|rule| {
                if let Some(eval) = evaluate_rule(&instance, &rule) {
                    eval
                        .into_iter()
                        .for_each(|row| {
                            out.insert_at(rule.head.relation_id.get(), row)
                        })
                }
            });

        return out
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

impl InstanceEvaluator<SimpleDatabase> for ParallelRewriting {
    fn evaluate(&self, instance: &SimpleDatabase) -> SimpleDatabase {
        let mut out: SimpleDatabase = Default::default();

        self
            .program
            .par_iter()
            .filter_map(|rule| {
                if let Some(eval) = evaluate_rule(&instance, &rule) {
                    return Some((rule.head.relation_id.get(), eval))
                }

                return None
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|(relation_id, eval)| {
                eval
                    .into_iter()
                    .for_each(|row| {
                        out.insert_at(relation_id, row)
                    })
            });

        return out
    }
}

pub struct ChibiDatalog {
    pub fact_store: SimpleDatabase,
    interner: Interner,
    parallel: bool,
    program: Program,
    sugared_program: SugaredProgram,
}

impl Default for ChibiDatalog {
    fn default() -> Self {
        ChibiDatalog {
            fact_store: Default::default(),
            interner: Default::default(),
            parallel: true,
            program: vec![],
            sugared_program: vec![],
        }
    }
}

impl ChibiDatalog {
    pub fn new(parallel: bool) -> Self {
        return Self {
            parallel,
            ..Default::default()
        };
    }
}

impl Dynamic for ChibiDatalog {
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut typed_row: Box<[TypedValue]> = row.iter().map(|ty| ty.to_typed_value()).collect();

        self.insert_typed(table, typed_row)
    }

    fn delete(&mut self, table: &str, row: &Vec<Box<dyn Ty>>) {
        let mut typed_row: Box<[TypedValue]> = row.iter().map(|ty| ty.to_typed_value()).collect();

        self.delete_typed(table, &typed_row)
    }
}

impl DynamicTyped for ChibiDatalog {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let typed_row = self.interner.intern_typed_values(&row);
        let relation_id = self.interner.rodeo.get_or_intern(table).into_inner().get();

        self.fact_store.insert_at(relation_id, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: &Row) {
        let typed_row = self.interner.intern_typed_values(row);
        let relation_id = self.interner.rodeo.get_or_intern(table).into_inner().get();

        self.fact_store.delete_at(relation_id, &typed_row)
    }
}

impl Flusher for ChibiDatalog {
    fn flush(&mut self, table: &str) {}
}

impl<'a> BottomUpEvaluator<'a> for ChibiDatalog {
    fn evaluate_program_bottom_up(&mut self, program: &Vec<SugaredRule>) -> HashMap<String, HashSet<Row>> {
        let sugared_program = program;

        let savory_program = sort_program(sugared_program)
            .iter()
            .map(|rule| self.interner.intern_rule(rule))
            .collect();

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(Rewriting::new(&savory_program)),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRewriting::new(&savory_program));
        }

        evaluation.semi_naive();

        return evaluation
            .output
            .storage
            .into_iter()
            .fold(Default::default(), |mut acc: EvaluationResult, (relation_id, row_set)| {
                let spur = Spur::try_from_usize(relation_id as usize).unwrap();
                let sym = self.interner.rodeo.resolve(&spur);

                acc.insert(sym.to_string(), row_set);
                acc
            })
    }
}

impl Materializer for ChibiDatalog {
    fn materialize(&mut self, program: &SugaredProgram) {
        program
            .iter()
            .for_each(|sugared_rule| {
                self.sugared_program.push(sugared_rule.clone());
            });

        self.sugared_program = sort_program(&self.sugared_program);

        self.program = self
            .sugared_program
            .iter()
            .map(|sugared_rule| self.interner.intern_rule(&sugared_rule))
            .collect();

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(Rewriting::new(&self.program)),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRewriting::new(&self.program));
        }

        evaluation.semi_naive();

        evaluation
            .output
            .storage
            .into_iter()
            .for_each(|(symbol, relation)| {
                relation.into_iter().for_each(|row| {
                    self.fact_store.insert_at(symbol, row);
                });
            });
    }

    // Update first processes deletions, then additions.
    fn update(&mut self, changes: Vec<Diff>) {
        let mut additions: Vec<(&str, Row)> = vec![];
        let mut retractions: Vec<(&str, Row)> = vec![];

        changes.iter().for_each(|(sign, (sym, value))| {
            let typed_row: Row = value
                .into_iter()
                .map(|untyped_value| untyped_value.to_typed_value())
                .collect();

            if *sign {
                additions.push((sym, typed_row));
            } else {
                retractions.push((sym, typed_row));
            }
        });

        let current_sugared_program = self.sugared_program.clone();

        if retractions.len() > 0 {
            delete_rederive(self, &current_sugared_program, retractions)
        }

        if additions.len() > 0 {
            additions.iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row.clone());
            });
            let mut evaluation = Evaluation::new(
                &self.fact_store,
                Box::new(Rewriting::new(&self.program)),
            );
            if self.parallel {
                evaluation.evaluator = Box::new(ParallelRewriting::new(&self.program));
            }

            evaluation.semi_naive();

            evaluation
                .output
                .storage
                .into_iter()
                .for_each(|(symbol, relation)| {
                    relation.into_iter().for_each(|row| {
                        self.fact_store.insert_at(symbol, row);
                    });
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
}

impl RelationDropper for ChibiDatalog {
    fn drop_relation(&mut self, table: &str) {
        let sym = self.interner.rodeo.get_or_intern(table);

        self.fact_store.storage.remove(&sym.into_inner().get());
    }
}
