use crate::models::instance::{Database};

pub trait InstanceEvaluator<T>
where
    T: Database,
{
    fn evaluate(&self, _: &T) -> T;
}

pub struct Evaluation<'a, T : Database> {
    pub input: &'a T,
    pub evaluator: Box<dyn InstanceEvaluator<T>>,
    pub previous_delta: T,
    pub current_delta: T,
    pub output: T,
}

impl<'a, T> Evaluation<'a, T>
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
        let mut input_plus_previous_delta = self.input.clone();
        self.previous_delta.database.iter().for_each(|relation| {
            // relation.1.ward.iter().for_each(|(row, active)| {
            //     if *active {
                     input_plus_previous_delta.insert_typed(*relation.0, row.clone())
            //    }
        });

        self.current_delta = Database::default();

        let evaluation = self.evaluator.evaluate(&input_plus_previous_delta);

        evaluation.iter().for_each(|relation| {
            relation.ward.iter().for_each(|(row, active)| {
                if *active {
                    self.current_delta
                        .insert_typed(relation.relation_id, row.clone());
                    self.output.insert_typed(relation.relation_id, row.clone());
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
