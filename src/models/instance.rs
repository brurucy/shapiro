use std::collections::HashMap;
use im::HashSet;

use crate::models::datalog::{Atom, Ty};
use crate::models::index::IndexBacking;
use crate::models::relational_algebra::Row;
use crate::reasoning::algorithms::relational_algebra::evaluate;

use super::{
    datalog::TypedValue,
    relational_algebra::{RelationWithIndex, RelationalExpression},
};

pub type SimpleDatabase = HashMap<String, HashSet<Row>>;
pub type DatabaseWithIndex<T> = HashMap<String, RelationWithIndex<T>>;

pub struct Instance
{
    pub database: SimpleDatabase,
}

impl Instance {
    pub fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let typed_row = row.into_iter().map(|element| element.to_typed_value()).collect();
        if let Some(relation) = self.database.get_mut(table) {
            relation.insert(typed_row);
        } else {
            let mut new_relation = HashSet::new();
            new_relation.insert(typed_row);
            self.database.insert(table.to_string(),new_relation);
        }
    }
    pub(crate) fn insert_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.insert(row);
        } else {
            let mut new_relation = HashSet::new();
            new_relation.insert(row);
            self.database
                .insert(table.to_string(), new_relation);
        }
    }
    pub(crate) fn delete_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.remove(&row);
        }
    }
    pub fn insert_atom(&mut self, atom: &Atom) {
        let row = (&atom.terms)
            .into_iter()
            .map(|term| term.clone().into())
            .collect();
        self.insert_typed(&atom.symbol.to_string(), row)
    }
    pub fn delete_atom(&mut self, atom: &Atom) {
        let row = (&atom.terms)
            .into_iter()
            .map(|term| term.clone().into())
            .collect();
        self.delete_typed(&atom.symbol.to_string(), row)
    }
    pub fn view(&self, table: &str) -> Vec<Box<[TypedValue]>> {
        return if let Some(relation) = self.database.get(table) {
            relation.into_iter().map(|row| row.clone()).collect()
        } else {
            vec![]
        };
    }
    pub fn new() -> Self {
        return Self {
            database: HashMap::new(),
        };
    }
}

#[derive(Clone, PartialEq)]
pub struct InstanceWithIndex<T>
where
    T: IndexBacking,
{
    pub database: DatabaseWithIndex<T>,
    pub use_indexes: bool,
}

impl<T: IndexBacking> InstanceWithIndex<T> {
    pub fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.insert(row)
        } else {
            let mut new_relation = RelationWithIndex::new(table, row.len(), self.use_indexes);
            new_relation.insert(row);
            self.database
                .insert(new_relation.symbol.clone(), new_relation);
        }
    }
    pub(crate) fn insert_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.insert_typed(row)
        } else {
            let mut new_relation = RelationWithIndex::new(table, row.len(), self.use_indexes);
            new_relation.insert_typed(row);
            self.database
                .insert(new_relation.symbol.clone(), new_relation);
        }
    }
    pub(crate) fn delete_typed(&mut self, table: &str, row: Box<[TypedValue]>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.mark_deleted(&row)
        }
    }
    pub fn insert_relation(&mut self, relation: RelationWithIndex<T>) {
        self.database.insert(relation.symbol.to_string(), relation);
    }
    pub fn insert_atom(&mut self, atom: &Atom) {
        let row = (&atom.terms)
            .into_iter()
            .map(|term| term.clone().into())
            .collect();
        self.insert_typed(&atom.symbol.to_string(), row)
    }
    pub fn delete_atom(&mut self, atom: &Atom) {
        let row = (&atom.terms)
            .into_iter()
            .map(|term| term.clone().into())
            .collect();
        self.delete_typed(&atom.symbol.to_string(), row)
    }
    pub fn view(&self, table: &str) -> Vec<Box<[TypedValue]>> {
        return if let Some(relation) = self.database.get(table) {
            relation.ward.clone().into_iter().map(|(k, _v)| k).collect()
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
    pub fn evaluate(
        &self,
        expression: &RelationalExpression,
        view_name: &str,
    ) -> Option<RelationWithIndex<T>> {
        return evaluate(expression, &self.database, view_name);
    }
}
