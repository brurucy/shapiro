use std::collections::{HashMap, HashSet};

use crate::models::datalog::{Atom, Ty};
use crate::models::index::IndexBacking;
use crate::models::relational_algebra::{Container, Row};
use crate::reasoning::algorithms::relational_algebra::evaluate;

use super::{
    datalog::TypedValue,
    relational_algebra::{RelationWithOneIndexBacking, RelationalExpression},
};

pub type Relation = HashSet<Row, ahash::RandomState>;
pub type Storage = HashMap<u32, Relation>;
pub type StorageWithIndex<T> = HashMap<u32, RelationWithOneIndexBacking<T>>;

pub trait Database {
    fn insert_at(&mut self, relation_id: u32, row: Row);
    fn delete_at(&mut self, relation_id: u32, row: Row);
}

pub trait WithIndexes {
    fn index_column(&mut self, relation_id: u32, column_idx: usize);
}

pub struct SimpleDatabase
{
    pub storage: Storage,
}

impl Database for SimpleDatabase {
    fn insert_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.insert(row);
        } else {
            let mut new_relation: Relation = Default::default();
            new_relation.insert(row);
            self.storage.insert(relation_id, new_relation);
        }
    }

    fn delete_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.remove(&row);
        }
    }
}

impl Default for SimpleDatabase {
    fn default() -> Self {
        return Self {
            storage: Default::default(),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct SimpleDatabaseWithIndex<T>
where
    T: IndexBacking,
{
    pub storage: StorageWithIndex<T>,
}

impl<T> Database for SimpleDatabaseWithIndex<T> {
    fn insert_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.insert(row);
        } else {
            let mut new_relation = RelationWithOneIndexBacking::new(relation_id, row.len());
            new_relation.insert(row);
            self.storage.insert(relation_id, new_relation);
        }
    }

    fn delete_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.mark_deleted(&row)
        }
    }
}

impl<T: IndexBacking> SimpleDatabaseWithIndex<T> {
    pub fn insert_relation(&mut self, relation: RelationWithOneIndexBacking<T>) {
        self.storage.insert(relation.relation_id, relation);
    }
    pub fn view(&self, relation_id: u32) -> Vec<Box<[TypedValue]>> {
        return if let Some(relation) = self.storage.get(&relation_id) {
            relation.ward.clone().into_iter().map(|(k, _v)| k).collect()
        } else {
            vec![]
        };
    }
    pub fn new() -> Self {
        return Self {
            storage: HashMap::new(),
        };
    }
    pub fn evaluate(
        &self,
        expression: &RelationalExpression,
        view_name: &str,
    ) -> Option<RelationWithOneIndexBacking<T>> {
        return evaluate(expression, &self.storage, view_name);
    }
}
