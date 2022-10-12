use std::collections::HashMap;

use crate::implementations::relational_algebra::evaluate;
use crate::models::datalog::{Atom, Ty};
use crate::models::index::{IndexBacking, ValueRowId};
use crate::models::relational_algebra::Map;

use super::{
    datalog::TypedValue,
    relational_algebra::{Relation, RelationalExpression},
};

pub type Database<T, K> = HashMap<String, Relation<T, K>>;

#[derive(Clone, PartialEq)]
pub struct Instance<T, K>
where T : IndexBacking,
      K : Map {
    pub database: Database<T, K>,
    pub use_indexes: bool,
}

impl<T: IndexBacking, K : Map> Instance<T, K> {
    pub fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.insert(row)
        } else {
            let mut new_relation = Relation::new(table, row.len(), self.use_indexes);
            new_relation.insert(row);
            self.database
                .insert(new_relation.symbol.clone(), new_relation);
        }
    }
    pub(crate) fn insert_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.insert_typed(row)
        } else {
            let mut new_relation = Relation::new(table, row.len(), self.use_indexes);
            new_relation.insert_typed(row);
            self.database
                .insert(new_relation.symbol.clone(), new_relation);
        }
    }
    pub fn insert_relation(&mut self, relation: Relation<T, K>) {
        self.database
            .insert(relation.symbol.to_string(), relation);
    }
    pub fn insert_atom(&mut self, atom: &Atom) {
        let row = (&atom.terms)
            .into_iter()
            .map(|term| term.clone().into())
            .collect();
        self.insert_typed(&atom.symbol.to_string(), row)
    }
    pub fn view(&self, table: &str) -> Vec<Box<[TypedValue]>> {
        return if let Some(relation) = self.database.get(table) {
            relation.ward.clone().into_iter().map(|(k, v)| k).collect()
        } else {
            vec![]
        };
    }
    pub fn new(use_indexes: bool) -> Self {
        return Self {
            database: HashMap::new(),
            use_indexes,
        };
    }
    pub fn evaluate(&self, expression: &RelationalExpression, view_name: &str) -> Option<Relation<T, K>> {
        return evaluate(expression, &self.database, view_name);
    }
}
