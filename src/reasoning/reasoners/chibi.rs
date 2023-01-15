use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, SugaredAtom, SugaredRule, Ty, TypedValue};
use crate::models::index::ValueRowId;
use crate::models::instance::{SimpleDatabase};
use crate::models::reasoner::{
    BottomUpEvaluator, Diff, Dynamic, DynamicTyped, Flusher, Materializer, Queryable,
    RelationDropper,
};
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
        return self
            .program
            .clone()
            .into_iter()
            .filter_map(|rule| {
                return evaluate_rule(&instance, &rule);
            })
            .collect();
    }
}

pub struct ParallelRewriting {
    pub program: Vec<SugaredRule>,
}

impl ParallelRewriting {
    fn new(program: &Vec<SugaredRule>) -> Self {
        return ParallelRewriting {
            program: program.clone(),
        };
    }
}

impl InstanceEvaluator<SimpleDatabase> for ParallelRewriting {
    fn evaluate(&self, instance: &SimpleDatabase) -> SimpleDatabase {
        return self
            .program
            .clone()
            .into_par_iter()
            .filter_map(|rule| {
                return evaluate_rule(&instance, &rule);
            })
            .collect();
    }
}

pub struct ChibiDatalog {
    pub fact_store: SimpleDatabase,
    interner: Interner,
    parallel: bool,
    intern: bool,
    materialization: Program,
}

impl Default for ChibiDatalog {
    fn default() -> Self {
        ChibiDatalog {
            fact_store: Default::default(),
            interner: Default::default(),
            parallel: true,
            intern: true,
            materialization: vec![],
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
}

impl Dynamic for ChibiDatalog {
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut typed_row: Box<[TypedValue]> = row.iter().map(|ty| ty.to_typed_value()).collect();

        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.fact_store.insert_typed(table, typed_row)
    }

    fn delete(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut typed_row: Box<[TypedValue]> = row.iter().map(|ty| ty.to_typed_value()).collect();

        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.fact_store.delete_typed(table, typed_row)
    }
}

impl DynamicTyped for ChibiDatalog {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let mut typed_row = row.clone();
        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.fact_store.insert_typed(table, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: Row) {
        let mut typed_row = row.clone();
        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.fact_store.delete_typed(table, typed_row)
    }
}

impl Flusher for ChibiDatalog {
    fn flush(&mut self, table: &str) {
        if let Some(relation) = self.fact_store.database.get_mut(table) {
            relation.compact()
        }
    }
}

impl BottomUpEvaluator<Vec<ValueRowId>> for ChibiDatalog {
    fn evaluate_program_bottom_up(&mut self, program: Vec<SugaredRule>) -> SimpleDatabase {
        let mut program = program;
        if self.intern {
            program = program
                .iter()
                .map(|rule| self.interner.intern_rule(rule))
                .collect();
        }
        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(Rewriting::new(&sort_program(&program))),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRewriting::new(&program));
        }
        evaluation.semi_naive();

        return evaluation.output;
    }
}

impl Materializer for ChibiDatalog {
    fn materialize(&mut self, program: &Program) {
        program.iter().for_each(|rule| {
            let mut possibly_interned_rule = rule.clone();
            if self.intern {
                possibly_interned_rule = self.interner.intern_rule(&possibly_interned_rule);
            }
            self.materialization.push(possibly_interned_rule);
        });

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(Rewriting::new(&sort_program(&self.materialization))),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRewriting::new(&self.materialization));
        }

        evaluation.semi_naive();

        evaluation
            .output
            .database
            .iter()
            .for_each(|(symbol, relation)| {
                relation.ward.iter().for_each(|(row, _active)| {
                    self.insert_typed(symbol, row.clone());
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

        if retractions.len() > 0 {
            delete_rederive(self, &self.materialization.clone(), retractions)
        }

        if additions.len() > 0 {
            additions.iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row.clone());
            });
            let mut evaluation = Evaluation::new(
                &self.fact_store,
                Box::new(Rewriting::new(&sort_program(&self.materialization))),
            );
            if self.parallel {
                evaluation.evaluator = Box::new(ParallelRewriting::new(&self.materialization));
            }

            evaluation.semi_naive();

            evaluation
                .output
                .database
                .iter()
                .for_each(|(symbol, relation)| {
                    relation.ward.iter().for_each(|(row, _active)| {
                        self.insert_typed(symbol, row.clone());
                    });
                });
        }
    }

    fn triple_count(&self) -> usize {
        return self
            .fact_store
            .database
            .iter()
            .map(|(_sym, rel)| return rel.ward.len())
            .sum();
    }
}

impl Queryable for ChibiDatalog {
    fn contains(&mut self, atom: &SugaredAtom) -> bool {
        let rel = self.fact_store.view(&atom.relation_id);
        let mut boolean_query = atom
            .terms
            .iter()
            .map(|term| term.clone().into())
            .collect::<Vec<TypedValue>>()
            .into_boxed_slice();
        if self.intern {
            boolean_query = self.interner.intern_typed_values(boolean_query);
        }

        return rel.contains(&boolean_query);
    }
}

impl RelationDropper for ChibiDatalog {
    fn drop_relation(&mut self, table: &str) {
        self.fact_store.database.remove(table);
    }
}
