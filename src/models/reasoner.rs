use crate::models::datalog::{SugaredAtom, SugaredProgram, SugaredRule, Ty, TypedValue};
use crate::models::relational_algebra::Row;
use ahash::{HashMap, HashSet};
use indexmap::IndexSet;

// Utility interface for reasoners that only logically delete data, and require a "flushing"
// step in order to physically do so.
pub trait Flusher {
    // Deletes all marked as deleted
    fn flush(&mut self, table: &str);
}

// General API
pub trait Dynamic {
    // Inserts data
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>);
    // Marks as deleted
    fn delete(&mut self, table: &str, row: &Vec<Box<dyn Ty>>);
}

// For internal consumption only
pub trait DynamicTyped {
    // Inserts data
    fn insert_typed(&mut self, table: &str, row: Box<[TypedValue]>);
    // Marks as deleted
    fn delete_typed(&mut self, table: &str, row: &Box<[TypedValue]>);
}

pub trait RelationDropper {
    fn drop_relation(&mut self, table: &str);
}

pub type Diff<'a> = (bool, (&'a str, Vec<Box<dyn Ty>>));

pub trait Materializer {
    // merges the given program with the already being materialized programs, and updates
    fn materialize(&mut self, program: &SugaredProgram);
    // given the changes, incrementally maintain the materialization
    fn update(&mut self, changes: Vec<Diff>);
    // returns the amount of facts currently materialized(possibly extensional and intensional)
    fn triple_count(&self) -> usize;
}

pub trait Queryable {
    fn contains_row(&self, table: &str, row: &Vec<Box<dyn Ty>>) -> bool;
}

pub type EvaluationResult = HashMap<String, IndexSet<Row, ahash::RandomState>>;

pub trait BottomUpEvaluator {
    fn evaluate_program_bottom_up(&mut self, program: &Vec<SugaredRule>) -> EvaluationResult;
}

pub trait TopDownEvaluator {
    fn evaluate_program_top_down(
        &mut self,
        program: &Vec<SugaredRule>,
        query: &SugaredRule,
    ) -> EvaluationResult;
}
