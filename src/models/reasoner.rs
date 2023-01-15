use crate::models::datalog::{Atom, Program, SugaredRule, Ty, TypedValue};
use crate::models::index::IndexBacking;
use crate::models::instance::{Instance, SimpleDatabaseWithIndex};

// Utility interface for reasoners that only physically delete data
pub trait Flusher {
    // Deletes all marked as deleted
    fn flush(&mut self, table: &str);
}

// General API
pub trait Dynamic {
    // Inserts data
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>);
    // Marks as deleted
    fn delete(&mut self, table: &str, row: Vec<Box<dyn Ty>>);
}

// For internal consumption only
pub(crate) trait DynamicTyped {
    // Inserts data
    fn insert_typed(&mut self, table: &str, row: Box<[TypedValue]>);
    // Marks as deleted
    fn delete_typed(&mut self, table: &str, row: Box<[TypedValue]>);
}

pub trait RelationDropper {
    fn drop_relation(&mut self, table: &str);
}

pub type Diff<'a> = (bool, (&'a str, Vec<Box<dyn Ty>>));

pub trait Materializer {
    // merges the given program with the already being materialized programs, and updates
    fn materialize(&mut self, program: &Program);
    // given the changes, incrementally maintain the materialization
    fn update(&mut self, changes: Vec<Diff>);
    // returns the amount of facts currently materialized(possibly extensional and intensional)
    fn triple_count(&self) -> usize;
}

pub trait Queryable {
    fn contains(&mut self, atom: &Atom) -> bool;
}

pub trait BottomUpEvaluator<T: IndexBacking> {
    fn evaluate_program_bottom_up(&mut self, program: Vec<SugaredRule>) -> Instance;
}

pub trait TopDownEvaluator<T: IndexBacking> {
    fn evaluate_program_top_down(&mut self, program: Vec<SugaredRule>, query: &SugaredRule) -> Instance;
}
