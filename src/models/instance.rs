use std::collections::{HashMap, HashSet};

use crate::models::index::IndexBacking;
use crate::models::relational_algebra::{Container, Row};
use crate::reasoning::algorithms::evaluation::{Empty, Set};
use crate::reasoning::algorithms::relational_algebra::evaluate;

use super::{
    datalog::TypedValue,
    relational_algebra::{SimpleRelationWithOneIndexBacking, RelationalExpression},
};

pub type HashSetBacking = HashSet<Row, ahash::RandomState>;
pub type SimpleStorage = HashMap<u32, HashSetBacking>;
pub type StorageWithIndex<T> = HashMap<u32, SimpleRelationWithOneIndexBacking<T>>;

pub trait Database: Default + PartialEq {
    fn insert_at(&mut self, relation_id: u32, row: Row);
    fn delete_at(&mut self, relation_id: u32, row: Row);
    fn create_relation(&mut self, symbol: String, relation_id: u32);
    fn delete_relation(&mut self, symbol: &str, relation_id: u32);
}

pub trait WithIndexes {
    fn index_column(&mut self, relation_id: u32, column_idx: usize);
}

#[derive(PartialEq)]
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

    fn delete_relation(&mut self, symbol: &str, relation_id: u32) {
        self.storage.remove(&relation_id);
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

impl Empty for SimpleDatabase {
    fn is_empty(&self) -> bool {
        return self.storage.is_empty()
    }
}


impl Default for SimpleDatabase {
    fn default() -> Self {
        return Self {
            storage: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct SimpleDatabaseWithIndex<T>
where
    T: IndexBacking
{
    pub storage: StorageWithIndex<T>,
}

impl<T : IndexBacking> PartialEq for SimpleDatabaseWithIndex<T> {
    fn eq(&self, other: &Self) -> bool {
        return self.storage == other.storage
    }
}


impl<T : IndexBacking + PartialEq> Database for SimpleDatabaseWithIndex<T> {
    fn insert_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.insert_row(row);
        }
    }

    fn delete_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.mark_deleted(&row)
        }
    }

    fn create_relation(&mut self, symbol: String, relation_id: u32) {
        let mut new_relation = SimpleRelationWithOneIndexBacking::new(symbol);
        self.storage.insert(relation_id, new_relation);
    }
    fn delete_relation(&mut self, symbol: &str, relation_id: u32) {
        self.storage.remove(&relation_id);
    }
}

impl<T : IndexBacking + PartialEq> Set for SimpleDatabaseWithIndex<T> {
        fn union(&self, other: &Self) -> Self {
            let mut out = SimpleDatabaseWithIndex::default();
            self
                .storage
                .iter()
                .for_each(|(relation_id, relation)| {
                    relation
                        .ward
                        .iter()
                        .for_each(|(row, active)| {
                            if *active {
                                out.insert_at(*relation_id, row.clone())
                            }
                        })
                });

            other
                .storage
                .iter()
                .for_each(|(relation_id, relation)| {
                    relation
                        .ward
                        .iter()
                        .for_each(|(row, active)| {
                            if *active {
                                out.insert_at(*relation_id, row.clone())
                            }
                        })
                });

            return out
        }

        fn difference(&self, other: &Self) -> Self {
            let mut out = SimpleDatabaseWithIndex::default();
            self
                .storage
                .iter()
                .for_each(|(relation_id, relation)| {
                    if let Some(other_relation) = other.storage.get(relation_id) {
                        relation
                            .ward
                            .into_iter()
                            .for_each(|(row, active)| {
                                if active && !other_relation.ward.contains_key(&row) {
                                    out.insert_at(*relation_id, row.clone())
                                }
                            })
                    }
                });

            out
        }
}

impl<T : IndexBacking + PartialEq> Empty for SimpleDatabaseWithIndex<T> {
    fn is_empty(&self) -> bool {
        return self.storage.is_empty()
    }
}

impl<T: IndexBacking + PartialEq> SimpleDatabaseWithIndex<T> {
    pub fn view(&self, relation_id: u32) -> Vec<Box<[TypedValue]>> {
        return if let Some(relation) = self.storage.get(&relation_id) {
            relation.ward.clone().into_iter().map(|(k, _v)| k).collect()
        } else {
            vec![]
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

impl<T : IndexBacking + PartialEq> Default for SimpleDatabaseWithIndex<T> {
    fn default() -> Self {
        return Self {
            storage: HashMap::new(),
        }
    }
}
