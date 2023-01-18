use lasso::{Key, Spur};
use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, Rule, SugaredAtom, SugaredProgram, SugaredRule, Ty, TypedValue};
use crate::models::index::ValueRowId;
use crate::models::instance::{Database, SimpleDatabase};
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

impl<'a> BottomUpEvaluator<'a> for ChibiDatalog {
    type IntoIter = std::vec::IntoIter<(&'a str, Row)>;

    fn evaluate_program_bottom_up(&mut self, program: Vec<SugaredRule>) -> Self::IntoIter {
        let sugared_program = program;

        let savory_program = &sort_program(&sugared_program)
            .iter()
            .map(|rule| self.interner.intern_rule(rule))
            .collect();

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(Rewriting::new(savory_program)),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRewriting::new(&savory_program));
        }

        return evaluation
            .output
            .storage
            .into_iter()
            .flat_map(|(relation_id, hashset)| {
                let spur = Spur::try_from_usize(relation_id as usize).unwrap();
                let sym = self.interner.rodeo.resolve(&spur);

                hashset
                    .into_iter()
                    .map(|row| (sym, row))
            })
            // This sort of defeats the purpose :D but oh well
            .collect()
            .into_iter()
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
            delete_rederive(self, &self.sugared_program, retractions)
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
                .iter()
                .for_each(|(symbol, relation)| {
                    relation.iter().for_each(|(row, _active)| {
                        self.fact_store.insert_at(*symbol, *row);
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
