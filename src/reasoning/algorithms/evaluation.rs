use crate::models::instance::{Database};

pub trait InstanceEvaluator<T>
where
    T: Database,
{
    fn evaluate(&self, _: &T) -> T;
}

pub trait Set {
    fn union(&self, other: &Self) -> Self;
    fn difference(&self, other: &Self) -> Self;
}

pub trait Empty {
    fn is_empty(&self) -> bool;
}

pub struct Evaluation<'a, T : Database + Set + Empty> {
    pub input: &'a T,
    pub evaluator: Box<dyn InstanceEvaluator<T>>,
    pub current_delta: T,
    pub output: T,
}

impl<'a, T : Database + Set + Empty> Evaluation<'a, T>
{
    pub(crate) fn new(database: &'a T, evaluator: Box<dyn InstanceEvaluator<T>>) -> Self {
        return Self {
            input: database,
            evaluator,
            current_delta: Default::default(),
            output: Default::default(),
        };
    }
    fn semi_naive_immediate_consequence(&mut self) {
        let input_plus_previous_delta = self.input.union(&self.current_delta);

        let evaluation = self.evaluator.evaluate(&input_plus_previous_delta);
        self.current_delta = evaluation.difference(&input_plus_previous_delta);

        self.output.union(&self.current_delta);
    }
    pub fn semi_naive(&mut self) {
        loop {
            self.semi_naive_immediate_consequence();

            if self.current_delta.is_empty() {
                break;
            }
        }
    }
}
