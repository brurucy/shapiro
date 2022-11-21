use crate::models::datalog::{Atom, Program, Rule, Ty, TypedValue};
use crate::models::index::IndexBacking;
use crate::models::instance::Instance;

pub trait Flusher {
    // Deletes all marked as deleted
    fn flush(&mut self, table: &str);
}

pub trait Dynamic {
    // Inserts data
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>);
    // Marks as deleted
    fn delete(&mut self, table: &str, row: Vec<Box<dyn Ty>>);
}

// For internal consumption only
pub trait DynamicTyped {
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
    // merges the given program with the already being materialized programs
    fn materialize(&mut self, program: &Program);
    // given the changes, incrementally maintain the materialization
    fn update(&mut self, changes: Vec<Diff>);
    // returns whether all materializations are updated
    fn safe(&self) -> bool;
}

pub trait Queryable {
    fn contains(&self, atom: &Atom) -> bool;
}

pub trait BottomUpEvaluator<T : IndexBacking> {
    fn evaluate_program_bottom_up(&mut self, program: Vec<Rule>) -> Instance<T>;
}

pub trait TopDownEvaluator<T : IndexBacking> {
    fn evaluate_program_top_down(&mut self, program: Vec<Rule>, query: &Rule) -> Instance<T>;
}