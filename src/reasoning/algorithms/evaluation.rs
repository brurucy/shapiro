use std::process::id;
use crate::models::instance::Database;
use std::time::Instant;

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
    pub previous_delta: T,
    pub output: T,
}

// Evaluation should be different for updates. At the moment it is sub optimal.
impl<'a, T: Database + Set + Empty> Evaluation<'a, T> {
    pub(crate) fn new(database: &'a T, evaluator: Box<dyn InstanceEvaluator<T>>) -> Self {
        return Self {
            input: database,
            evaluator,
            delta: Default::default(),
            previous_delta: Default::default(),
            output: Default::default(),
        };
    }
    fn semi_naive_immediate_consequence(&mut self) {
        let union = self.input.union(&self.delta);

        //let now = Instant::now();
        let evaluation = self.evaluator.evaluate(union);
        //println!("rule evaluation: {}", now.elapsed().as_millis());

        self.delta = evaluation.difference(&self.output).difference(self.input);

        self.output.merge(evaluation);
    }
    pub fn semi_naive(&mut self) {
        loop {
            let now = Instant::now();
            self.semi_naive_immediate_consequence();
            println!("iteration duration: {}", now.elapsed().as_millis());

            if self.delta.is_empty() {
                break;
            }
        }
    }
}

pub struct IncrementalEvaluation<T: Database + Set + Empty> {
    pub deltaifier: Box<dyn InstanceEvaluator<T>>,
    pub nonrecursive_evaluator: Box<dyn InstanceEvaluator<T>>,
    pub recursive_evaluator: Box<dyn InstanceEvaluator<T>>,
    pub previous: T,
    pub delta: T,
    pub output: T,
}

// Evaluation should be different for updates. At the moment it is sub optimal.
impl<T: Database + Set + Empty + Clone> IncrementalEvaluation<T> {
    pub(crate) fn new(
        deltaifier: Box<dyn InstanceEvaluator<T>>,
        nonrecursive_evaluator: Box<dyn InstanceEvaluator<T>>,
        recursive_evaluator: Box<dyn InstanceEvaluator<T>>,
    ) -> Self {
        return Self {
            deltaifier,
            nonrecursive_evaluator,
            recursive_evaluator,
            delta: Default::default(),
            previous: Default::default(),
            output: Default::default(),
        };
    }
    fn semi_naive_immediate_consequence(&mut self, edb: &T, idb: &T) {
        let union = edb.union(&self.delta);

        let evaluation = self.recursive_evaluator.evaluate(union);

        self.delta = evaluation
            .difference(&self.output)
            .union(&self.delta);

        self.output.merge(evaluation);
    }
    // let mut s_i = T::default();
    // let mut delta = self.nonrecursive_evaluator.evaluate(edb.clone());
    // loop {
    //     s_i = s_i.union(&delta);
    //     delta = self.recursive_evaluator
    //         .evaluate(edb.union(&delta))
    //         .difference(&s_i);
    //     if delta.is_empty() {
    //         break
    //         self.output = s_i;
    //     }
    // }

    pub fn semi_naive(&mut self, edb: &T) {
        let mut delta = self.nonrecursive_evaluator.evaluate(edb.clone());

        let mut db = delta
            .union(edb);
        loop {
            let db_u_delta = db
                .union(&delta);
            let pre_delta = self
                .recursive_evaluator
                .evaluate(db_u_delta)
                .difference(&db);

            db = db.union(&pre_delta);
            delta = self.deltaifier.evaluate(pre_delta);
            if delta.is_empty() {
                self.output = db
                    .difference(&edb);
                return;
            }
        }
    }
}
