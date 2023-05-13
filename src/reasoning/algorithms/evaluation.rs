use crate::models::instance::Database;

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
        let mut immediate_consequence = self
            .immediate_consequence_operator
            .nonrecursive_program(fact_store);
        let mut db = fact_store.union(&immediate_consequence);
        let mut delta = self.immediate_consequence_operator.deltaify_idb(&db);

        loop {
            let db_u_delta = db.union(&delta);

            immediate_consequence = self
                .immediate_consequence_operator
                .recursive_program(&db_u_delta)
                .difference(&db);

            db = db.union(&immediate_consequence);
            delta = self.immediate_consequence_operator.deltaify_idb(&immediate_consequence);
            if delta.is_empty() {
                self.output = db.difference(&fact_store);
                return;
            }
        }
    }
}
