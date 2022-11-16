use crate::models::datalog::{Rule, Ty, TypedValue};
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

pub trait ViewProvider {
    fn view(&self, table: &str) -> Vec<Box<[TypedValue]>>;
}

pub trait BottomUpEvaluator<T : IndexBacking> {
    fn evaluate_program_bottom_up(&mut self, program: Vec<Rule>) -> Instance<T>;
}

pub trait TopDownEvaluator<T : IndexBacking> {
    fn evaluate_program_top_down(&mut self, program: Vec<Rule>, query: &Rule) -> Instance<T>;
}