use crate::misc::helpers::{idempotent_intern, idempotent_program_strong_intern, idempotent_program_weak_intern, ty_to_row};
use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{
    Program, Rule, SugaredProgram, SugaredRule, Ty,
};
use crate::models::instance::{Database, HashSetDatabase};
use crate::models::reasoner::{
    BottomUpEvaluator, Diff, Dynamic, DynamicTyped, EvaluationResult, Materializer,
    Queryable, RelationDropper,
};
use crate::models::relational_algebra::Row;
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::evaluation::{Evaluation, InstanceEvaluator};
use crate::reasoning::algorithms::rewriting::evaluate_rule;
use lasso::{Key, Spur};
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

impl InstanceEvaluator<HashSetDatabase> for Rewriting {
    fn evaluate(&self, instance: HashSetDatabase) -> HashSetDatabase {
        let mut out: HashSetDatabase = Default::default();

        self.program.iter().for_each(|rule| {
            if let Some(eval) = evaluate_rule(&instance, &rule) {
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
    pub fact_store: HashSetDatabase,
    interner: Interner,
    parallel: bool,
    intern: bool,
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
    fn new_evaluation(&self, program: &Program) -> Evaluation<HashSetDatabase> {
        return Evaluation::new(
            &self.fact_store,
            if self.parallel { Box::new(ParallelRewriting::new(program)) } else { Box::new(Rewriting::new(program))}
        );
    }
    fn update_materialization(&mut self) {
        let mut evaluation = self.new_evaluation(&self.program);

        evaluation.semi_naive();

        evaluation
            .output
            .storage
            .into_iter()
            .for_each(|(relation_id, relation)| {
                relation.into_iter().for_each(|row| {
                    self.fact_store.insert_at(relation_id, row);
                });
            });
    }
}

impl Dynamic for ChibiDatalog {
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        self.insert_typed(table, row.iter().map(|ty| ty.to_typed_value()).collect())
    }

    fn delete(&mut self, table: &str, row: &Vec<Box<dyn Ty>>) {
        self.delete_typed(table, &row.iter().map(|ty| ty.to_typed_value()).collect())
    }
}

impl DynamicTyped for ChibiDatalog {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let (relation_id, typed_row) = idempotent_intern(&mut self.interner, self.intern, table, row.clone());

        self.fact_store.insert_at(relation_id, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: &Row) {
        let (relation_id, typed_row) = idempotent_intern(&mut self.interner, self.intern, table, row.clone());

        self.fact_store.delete_at(relation_id, &typed_row)
    }
}

impl BottomUpEvaluator for ChibiDatalog {
    fn evaluate_program_bottom_up(
        &mut self,
        program: &Vec<SugaredRule>,
    ) -> EvaluationResult {
        let savory_program = idempotent_program_strong_intern(&mut self.interner, self.intern, program);

        let mut evaluation = self.new_evaluation(&savory_program);

        evaluation.semi_naive();

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
            .for_each(|sugared_rule| {
                self.sugared_program.push(sugared_rule)
            });

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

        let current_sugared_program = self.sugared_program.clone();

        if retractions.len() > 0 {
            delete_rederive(self, &current_sugared_program, retractions)
        }

        if additions.len() > 0 {
            additions.into_iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row);
            });

            self.update_materialization()
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

impl Queryable for ChibiDatalog {
    fn contains_row(&self, table: &str, row: &Vec<Box<dyn Ty>>) -> bool {
        if let Some(relation_id) = self.interner.rodeo.get(table) {
            if let Some(typed_row) = self.interner.try_intern_row(&ty_to_row(row)) {
                return self
                    .fact_store
                    .storage
                    .get(&relation_id.into_inner().get())
                    .unwrap()
                    .contains(&typed_row);
            }
        }

        return false;
    }
}
