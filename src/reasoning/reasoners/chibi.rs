use rayon::prelude::IntoParallelIterator;
use crate::implementations::evaluation::{Evaluation, InstanceEvaluator};
use crate::implementations::interning::Interner;
use crate::implementations::rule_graph::sort_program;
use crate::models::datalog::{Atom, Rule, Term, Ty};
use crate::models::datalog::Sign::Positive;
use crate::models::index::ValueRowId;
use crate::models::instance::Instance;
use crate::models::reasoner::{BottomUpEvaluator, Dynamic, DynamicTyped, Flusher};
use crate::models::relational_algebra::{Relation, Row};
use crate::reasoning::algorithms::rewriting::evaluate_rule;

pub struct Rewriting {
    pub program: Vec<Rule>,
}

impl Rewriting {
    fn new(program: &Vec<Rule>) -> Self {
        return Rewriting {
            program: program.clone()
        };
    }
}

impl InstanceEvaluator<Vec<ValueRowId>> for Rewriting {
    fn evaluate(&self, instance: &Instance<Vec<ValueRowId>>) -> Vec<Relation<Vec<ValueRowId>>> {
        return self.program
            .clone()
            .into_iter()
            .filter_map(|rule| {
                println!("evaluating: {}", rule);
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
            program: program.clone()
        };
    }
}

impl InstanceEvaluator<Vec<ValueRowId>> for ParallelRewriting {
    fn evaluate(&self, instance: &Instance<Vec<ValueRowId>>) -> Vec<Relation<Vec<ValueRowId>>> {
        return self.program
            .clone()
            .into_par_iter()
            .filter_map(|rule| {
                println!("evaluating: {}", rule);
                return evaluate_rule(&instance, &rule);
            })
            .collect();
    }
}

pub struct ChibiDatalog {
    pub fact_store: Instance<Vec<ValueRowId>>,
    interner: Interner,
    parallel: bool,
    intern: bool,
}

impl Default for ChibiDatalog {
    fn default() -> Self {
        ChibiDatalog {
            fact_store: Instance::new(false),
            interner: Interner::default(),
            parallel: true,
            intern: true,
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
        let mut atom = Atom {
            symbol: table.to_string(),
            terms: row
                .iter()
                .map(|ty| Term::Constant(ty.to_typed_value()))
                .collect(),
            sign: Positive
        };
        if self.intern {
            atom = self.interner.intern_atom(&atom)
        }
        self.fact_store.insert_atom(&atom)
    }

    fn delete(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut atom = Atom {
            symbol: table.to_string(),
            terms: row
                .iter()
                .map(|ty| Term::Constant(ty.to_typed_value()))
                .collect(),
            sign: Positive
        };
        if self.intern {
            atom = self.interner.intern_atom(&atom)
        }
        self.fact_store.delete_atom(&atom)
    }
}

impl DynamicTyped for ChibiDatalog {
    fn insert_typed(&mut self, table: &str, row: Row) {
        self.fact_store.insert_typed(table, row)
    }
    fn delete_typed(&mut self, table: &str, row: Row) { self.fact_store.delete_typed(table, row) }
}

impl Flusher for ChibiDatalog {
    fn flush(&mut self, table: &str) {
        if let Some(relation) = self.fact_store.database.get_mut(table) {
            relation.compact()
        }
    }
}

impl BottomUpEvaluator<Vec<ValueRowId>> for ChibiDatalog {
    fn evaluate_program_bottom_up(&mut self, program: Vec<Rule>) -> Instance<Vec<ValueRowId>> {
        let mut program = program;
        if self.intern {
            program = program
                .iter()
                .map(|rule| self.interner.intern_rule(rule))
                .collect();
        }
        let mut evaluation = Evaluation::new(&self.fact_store, Box::new(Rewriting::new(&sort_program(&program))));
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRewriting::new(&program));
        }
        evaluation.semi_naive();

        return evaluation.output;
    }
}