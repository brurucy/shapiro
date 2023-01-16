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

pub struct Evaluation<'a, T : Database + Clone + Set> {
    pub input: &'a T,
    pub evaluator: Box<dyn InstanceEvaluator<T>>,
    pub previous_delta: T,
    pub current_delta: T,
    pub output: T,
}

impl<'a, T : Database + Clone + Set> Evaluation<'a, T>
{
    pub(crate) fn new(database: &'a T, evaluator: Box<dyn InstanceEvaluator<T>>) -> Self {
        return Self {
            input: database,
            evaluator,
            previous_delta: Default::default(),
            current_delta: Default::default(),
            output: Default::default(),
        };
    }
    fn semi_naive_immediate_consequence(&mut self) {
        self.previous_delta = self.current_delta.clone();
        let input_plus_previous_delta = self.input.union(&self.previous_delta);

        let evaluation = self.evaluator.evaluate(&input_plus_previous_delta);
        self.current_delta = evaluation.difference(&input_plus_previous_delta);

        self.output.union(&self.current_delta);
    }
    pub fn semi_naive(&mut self) {
        loop {
            self.semi_naive_immediate_consequence();

            if self.previous_delta == self.current_delta {
                break;
            }
        }
    }
}
