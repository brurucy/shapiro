use crate::models::index::{IndexBacking, ValueRowId};
use crate::models::instance::Instance;
use crate::models::relational_algebra::{Map, Relation};

pub trait InstanceEvaluator<T, K>
    where T : IndexBacking,
          K : Map {
    fn evaluate(&self, _: &Instance<T, K>) -> Vec<Relation<T, K>>;
}

pub struct Evaluation<'a, T, K>
    where T : IndexBacking,
          K : Map {
    pub input: &'a Instance<T, K>,
    pub evaluator: Box<dyn InstanceEvaluator<T, K>>,
    pub previous_delta: Instance<T, K>,
    pub current_delta: Instance<T, K>,
    pub output: Instance<T, K>,
}

impl<'a, T, K> Evaluation<'a, T, K>
    where T : IndexBacking,
          K : Map {
    pub(crate) fn new(instance: &'a Instance<T, K>, evaluator: Box<dyn InstanceEvaluator<T, K>>) -> Self {
        return Self {
            input: instance,
            evaluator,
            previous_delta: Instance::new(instance.use_indexes),
            current_delta: Instance::new(instance.use_indexes),
            output: Instance::new(instance.use_indexes),
        }
    }
    fn semi_naive_immediate_consequence(&mut self) {
        self.previous_delta = self.current_delta.clone();
        let mut input_plus_previous_delta = self.input.clone();
        self.previous_delta
            .database
            .iter()
            .for_each(|relation| {
                relation
                    .1
                    .ward
                    .clone()
                    .into_iter()
                    .for_each(|(row, notdeleted)| {
                        if notdeleted {
                            input_plus_previous_delta.insert_typed(&relation.0, row.clone())
                        }
                    })
            });

        self.current_delta = Instance::new(self.input.use_indexes);

        let evaluation = self.evaluator.evaluate(&input_plus_previous_delta);

        evaluation
            .iter()
            .for_each(|relation| {
                relation
                    .ward
                    .clone()
                    .into_iter()
                    .for_each(|(row, notdeleted)| {
                        if notdeleted {
                            self.current_delta.insert_typed(&relation.symbol, row.clone());
                            self.output.insert_typed(&relation.symbol, row.clone());
                        }
                    })
            });
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