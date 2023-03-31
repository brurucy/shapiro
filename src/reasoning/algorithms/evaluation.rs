use crate::models::instance::Database;
use colored::Colorize;
use std::time::Instant;

pub trait ImmediateConsequenceOperator<T>
where
    T: Database,
{
    fn deltaify_idb(&self, _: &T) -> T;
    fn nonrecursive_program(&self, _: &T) -> T;
    fn recursive_program(&self, _: &T) -> T;
}

pub trait Set {
    fn union(&self, other: &Self) -> Self;
    fn difference(&self, other: &Self) -> Self;
    fn merge(&mut self, other: Self);
}

pub trait Empty {
    fn is_empty(&self) -> bool;
}

pub struct IncrementalEvaluation<T: Database + Set + Empty> {
    pub immediate_consequence_operator: Box<dyn ImmediateConsequenceOperator<T>>,
    pub output: T,
}

// Evaluation should be different for updates. At the moment it is sub optimal.
impl<T: Database + Set + Empty> IncrementalEvaluation<T> {
    pub(crate) fn new(
        immediate_consequence_operator: Box<dyn ImmediateConsequenceOperator<T>>,
    ) -> Self {
        return Self {
            immediate_consequence_operator,
            output: Default::default(),
        };
    }
    pub fn semi_naive(&mut self, fact_store: &T) {
        // println!("{}", "nonrecursive".blue());
        // let now = Instant::now();
        let pre_delta = self
            .immediate_consequence_operator
            .nonrecursive_program(fact_store);
        // println!(
        //     "duration: {}",
        //     now.elapsed().as_millis().to_string().green()
        // );
        let mut db = fact_store.union(&pre_delta);
        //println!("{}", "deltaified nonrecursive".blue());
        // let now = Instant::now();
        let mut delta = self.immediate_consequence_operator.deltaify_idb(&db);
        // println!(
        //     "duration: {}",
        //     now.elapsed().as_millis().to_string().green()
        // );
        loop {
            let db_u_delta = db.union(&delta);
            //println!("{}", "recursive".blue());
            // let now = Instant::now();
            let pre_delta = self
                .immediate_consequence_operator
                .recursive_program(&db_u_delta)
                .difference(&db);
            // println!(
            //     "duration: {}",
            //     now.elapsed().as_millis().to_string().green()
            // );
            db = db.union(&pre_delta);
            //println!("{}", "deltaified recursive".blue());
            // let now = Instant::now();
            delta = self.immediate_consequence_operator.deltaify_idb(&pre_delta);
            // println!(
            //     "duration: {}",
            //     now.elapsed().as_millis().to_string().green()
            // );
            if delta.is_empty() {
                self.output = db.difference(&fact_store);
                return;
            }
        }
    }
}
