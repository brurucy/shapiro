use std::time::Instant;
use crate::models::instance::Database;

pub trait InstanceEvaluator<T>
where
    T: Database,
{
    fn evaluate(&self, _: T) -> T;
}

pub trait Set {
    fn union(&self, other: &Self) -> Self;
    fn difference(&self, other: &Self) -> Self;
    fn merge(&mut self, other: Self);
}

pub trait Empty {
    fn is_empty(&self) -> bool;
}

pub struct Evaluation<'a, T: Database + Set + Empty> {
    pub input: &'a T,
    pub evaluator: Box<dyn InstanceEvaluator<T>>,
    pub delta: T,
    pub output: T,
}

impl<'a, T: Database + Set + Empty> Evaluation<'a, T> {
    pub(crate) fn new(database: &'a T, evaluator: Box<dyn InstanceEvaluator<T>>) -> Self {
        return Self {
            input: database,
            evaluator,
            delta: Default::default(),
            output: Default::default(),
        };
    }
    fn semi_naive_immediate_consequence(&mut self) {
        let union = self.input.union(&self.delta);

        let evaluation = self.evaluator.evaluate(union);

        self.delta = evaluation.difference(&self.output);


        self.output.merge(evaluation);
    }
    pub fn semi_naive(&mut self) {
        loop {
            self.semi_naive_immediate_consequence();

            if self.delta.is_empty() {
                break;
            }
        }
    }
}
