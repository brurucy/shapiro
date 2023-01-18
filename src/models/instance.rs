use std::collections::{HashMap, HashSet};

use crate::models::index::IndexBacking;
use crate::models::relational_algebra::{Row};
use crate::reasoning::algorithms::evaluation::Set;
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
    fn create_relation(&mut self, symbol: String, relation_id: u32, arity: usize);
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

    fn create_relation(&mut self, symbol: String, relation_id: u32, arity: usize) {
        let mut new_relation: HashSetBacking = Default::default();
        self.storage.insert(relation_id, new_relation);
    }
}

impl Set for SimpleDatabase {
    fn union(&self, other: &Self) -> Self {
        let mut out = SimpleDatabase::default();
        self
            .storage
            .iter()
            .for_each(|(relation_id, relation)| {
                relation
                    .into_iter()
                    .for_each(|row| {
                        out.insert_at(*relation_id, row.clone())
                    })
            });
        other
            .storage
            .iter()
            .for_each(|(relation_id, relation)| {
                relation
                    .into_iter()
                    .for_each(|row| {
                        out.insert_at(*relation_id, row.clone())
                    })
            });

        return out
    }

    fn difference(&self, other: &Self) -> Self {
        let mut out = SimpleDatabase::default();
        self
            .storage
            .iter()
            .for_each(|(relation_id, relation)| {
                if let Some(other_relation) = other.storage.get(relation_id) {
                    relation
                        .into_iter()
                        .for_each(|row| {
                            if !other_relation.contains(row) {
                                out.insert_at(*relation_id, row.clone())
                            }
                        })
                }
            });

        out
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
        }
    }

    fn delete_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.mark_deleted(&row)
        }
    }

    fn create_relation(&mut self, symbol: String, relation_id: u32, arity: usize) {
        let mut new_relation = SimpleRelationWithOneIndexBacking::new(symbol, arity);
        self.storage.insert(relation_id, new_relation);
    }
}

impl<T: IndexBacking + Eq + PartialEq> SimpleDatabaseWithIndex<T> {
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
