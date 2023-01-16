use std::collections::{HashMap, HashSet};

use crate::models::index::IndexBacking;
use crate::models::relational_algebra::{Row};
use crate::reasoning::algorithms::relational_algebra::evaluate;

use super::{
    datalog::TypedValue,
    relational_algebra::{SimpleRelationWithOneIndexBacking, RelationalExpression},
};

pub type HashSetBacking = HashSet<Row, ahash::RandomState>;
pub type SimpleStorage = HashMap<u32, HashSetBacking>;
pub type StorageWithIndex<T> = HashMap<u32, SimpleRelationWithOneIndexBacking<T>>;

pub trait Database: Default + Eq {
    fn insert_at(&mut self, relation_id: u32, row: Row);
    fn delete_at(&mut self, relation_id: u32, row: Row);
    fn create_relation(&mut self, symbol: String, relation_id: u32);
}

pub trait WithIndexes {
    fn index_column(&mut self, relation_id: u32, column_idx: usize);
}

#[derive(Eq, PartialEq)]
pub struct SimpleDatabase
{
    pub storage: SimpleStorage,
}

impl Database for SimpleDatabase {
    fn insert_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.insert(row);
        } else {
            let mut new_relation: HashSetBacking = Default::default();
            new_relation.insert(row);
            self.storage.insert(relation_id, new_relation);
        }
    }

    fn delete_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.remove(&row);
        }
    }

    fn create_relation(&mut self, symbol: String, relation_id: u32) {
        let mut new_relation: HashSetBacking = Default::default();
        self.storage.insert(relation_id, new_relation);
    }
}

impl Default for SimpleDatabase {
    fn default() -> Self {
        return Self {
            storage: Default::default(),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SimpleDatabaseWithIndex<T>
where
    T: IndexBacking + Eq + PartialEq,
{
    pub storage: StorageWithIndex<T>,
}

impl<T : IndexBacking + Eq + PartialEq> Database for SimpleDatabaseWithIndex<T> {
    fn insert_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.insert(row);
        } else {
            new_relation.insert(row);
            self.storage.insert(relation_id, new_relation);
        }
    }

    fn delete_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.mark_deleted(&row)
        }
    }

    fn create_relation(&mut self, symbol: String, relation_id: u32) {
        let mut new_relation = Sim::new(relation_id, row.len());
    }
}

impl<T: IndexBacking + Eq + PartialEq> SimpleDatabaseWithIndex<T> {
    pub fn insert_relation(&mut self, relation: SimpleRelationWithOneIndexBacking<T>) {
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
    ) -> Option<SimpleRelationWithOneIndexBacking<T>> {
        return evaluate(expression, &self.storage, view_name);
    }
}

impl<T : IndexBacking + Eq + PartialEq> Default for SimpleDatabaseWithIndex<T> {
    fn default() -> Self {
        return Self {
            storage: HashMap::new(),
        }
    }
}
