use crate::models::instance::Instance;
use crate::models::relational_algebra::Relation;

pub trait InstanceEvaluator {
    fn evaluate(&self, _: &Instance) -> Vec<Relation>;
}

pub struct Evaluation<'a> {
    pub input: &'a Instance,
    pub evaluator: Box<dyn InstanceEvaluator>,
    pub previous_delta: Instance,
    pub current_delta: Instance,
    pub output: Instance,
}

impl<'a> Evaluation<'a> {
    pub(crate) fn new(instance: &'a Instance, evaluator: Box<dyn InstanceEvaluator>) -> Self {
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
                    .iter()
                    .for_each(|(row, notdeleted)| {
                        if *notdeleted {
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
                    .iter()
                    .for_each(|(row, notdeleted)| {
                        if *notdeleted {
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