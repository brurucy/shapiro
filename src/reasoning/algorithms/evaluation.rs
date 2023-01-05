use crate::models::index::IndexBacking;
use crate::models::instance::InstanceWithIndex;
use crate::models::relational_algebra::RelationWithIndex;

pub trait InstanceEvaluator<T>
where
    T: IndexBacking,
{
    fn evaluate(&self, _: &InstanceWithIndex<T>) -> Vec<RelationWithIndex<T>>;
}

pub struct Evaluation<'a, T>
where
    T: IndexBacking,
{
    pub input: &'a InstanceWithIndex<T>,
    pub evaluator: Box<dyn InstanceEvaluator<T>>,
    pub previous_delta: InstanceWithIndex<T>,
    pub current_delta: InstanceWithIndex<T>,
    pub output: InstanceWithIndex<T>,
}

impl<'a, T> Evaluation<'a, T>
where
    T: IndexBacking,
{
    pub(crate) fn new(instance: &'a InstanceWithIndex<T>, evaluator: Box<dyn InstanceEvaluator<T>>) -> Self {
        return Self {
            input: instance,
            evaluator,
            previous_delta: InstanceWithIndex::new(instance.use_indexes),
            current_delta: InstanceWithIndex::new(instance.use_indexes),
            output: InstanceWithIndex::new(instance.use_indexes),
        };
    }
    fn semi_naive_immediate_consequence(&mut self) {
        self.previous_delta = self.current_delta.clone();
        let mut input_plus_previous_delta = self.input.clone();
        self.previous_delta.database.iter().for_each(|relation| {
            relation.1.ward.iter().for_each(|(row, active)| {
                if *active {
                    input_plus_previous_delta.insert_typed(&relation.0, row.clone())
                }
            })
        });

        self.current_delta = InstanceWithIndex::new(self.input.use_indexes);

        let evaluation = self.evaluator.evaluate(&input_plus_previous_delta);

        evaluation.iter().for_each(|relation| {
            relation.ward.iter().for_each(|(row, active)| {
                if *active {
                    self.current_delta
                        .insert_typed(&relation.symbol, row.clone());
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
