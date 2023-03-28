use crate::misc::string_interning::Interner;
use indexmap::IndexSet;
use lasso::{Key, Spur};
use std::collections::HashMap;

use crate::models::index::IndexBacking;
use crate::models::relational_algebra::{Container, Row};
use crate::reasoning::algorithms::evaluation::{Empty, Set};
use crate::reasoning::algorithms::relational_algebra::evaluate;

use super::relational_algebra::{RelationalExpression, SimpleRelationWithOneIndexBacking};

pub type IndexedHashSetBacking = IndexSet<Row, ahash::RandomState>;
pub type HashSetStorage = HashMap<u32, IndexedHashSetBacking>;

pub trait Database: Default + PartialEq {
    fn insert_at(&mut self, relation_id: u32, row: Row);
    fn delete_at(&mut self, relation_id: u32, row: &Row);
    fn create_relation(&mut self, symbol: String, relation_id: u32);
    fn delete_relation(&mut self, symbol: &str, relation_id: u32);
}

pub trait WithIndexes {
    fn index_column(&mut self, relation_id: u32, column_idx: usize);
}

#[derive(PartialEq, Clone)]
pub struct HashSetDatabase {
    pub storage: HashSetStorage,
}

impl Database for HashSetDatabase {
    fn insert_at(&mut self, relation_id: u32, row: Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.insert(row);
        } else {
            let mut new_relation: IndexedHashSetBacking = Default::default();
            new_relation.insert(row);
            self.storage.insert(relation_id, new_relation);
        }
    }

    fn delete_at(&mut self, relation_id: u32, row: &Row) {
        if let Some(relation) = self.storage.get_mut(&relation_id) {
            relation.remove(row);
        }
    }

    fn create_relation(&mut self, _symbol: String, relation_id: u32) {
        self.storage.insert(relation_id, Default::default());
    }

    fn delete_relation(&mut self, _symbol: &str, relation_id: u32) {
        self.storage.remove(&relation_id);
    }
}

impl Set for HashSetDatabase {
    fn union(&self, other: &Self) -> Self {
        let mut out = HashSetDatabase::default();

        self.storage.iter().for_each(|(relation_id, relation)| {
            relation
                .into_iter()
                .for_each(|row| out.insert_at(*relation_id, row.clone()))
        });
        other.storage.iter().for_each(|(relation_id, relation)| {
            relation
                .into_iter()
                .for_each(|row| out.insert_at(*relation_id, row.clone()))
        });

        return out;
    }

    fn difference(&self, other: &Self) -> Self {
        let mut out = HashSetDatabase::default();

        self.storage.iter().for_each(|(relation_id, relation)| {
            if other.storage.contains_key(relation_id) {
                let other_relation = other.storage.get(relation_id).unwrap();

                relation.iter().for_each(|row| {
                    if !other_relation.contains(row) {
                        out.insert_at(*relation_id, row.clone())
                    }
                })
            } else {
                relation
                    .iter()
                    .for_each(|row| out.insert_at(*relation_id, row.clone()))
            }
        });

        out
    }

    fn merge(&mut self, other: Self) {
        other
            .storage
            .into_iter()
            .for_each(|(relation_id, row_set)| {
                row_set
                    .into_iter()
                    .for_each(|row| self.insert_at(relation_id, row))
            })
    }
}

impl Empty for HashSetDatabase {
    fn is_empty(&self) -> bool {
        let mut is_empty = false || self.storage.is_empty();

        self.storage.iter().for_each(|(_relation_id, row_set)| {
            if row_set.is_empty() {
                is_empty = true
            }
        });

        return is_empty;
    }
}

impl Default for HashSetDatabase {
    fn default() -> Self {
        return Self {
            storage: Default::default(),
        };
    }
}

pub type StorageWithIndex<T> = HashMap<String, SimpleRelationWithOneIndexBacking<T>>;

#[derive(Clone)]
pub struct SimpleDatabaseWithIndex<T>
where
    T: IndexBacking,
{
    pub storage: StorageWithIndex<T>,
    pub symbol_interner: Interner,
}

impl<T: IndexBacking + PartialEq> PartialEq for SimpleDatabaseWithIndex<T> {
    fn eq(&self, other: &Self) -> bool {
        return self.storage == other.storage;
    }
}

impl<T: IndexBacking + PartialEq> Database for SimpleDatabaseWithIndex<T> {
    fn insert_at(&mut self, relation_id: u32, row: Row) {
        let spur = Spur::try_from_usize(relation_id as usize - 1).unwrap();
        let symbol = self.symbol_interner.rodeo.resolve(&spur);

        if let Some(relation) = self.storage.get_mut(symbol) {
            relation.insert_row(row);
        } else {
            let mut new_relation = SimpleRelationWithOneIndexBacking::new(symbol.to_string());
            new_relation.ward.insert(row);
            self.storage.insert(symbol.to_string(), new_relation);
        }
    }
    fn delete_at(&mut self, relation_id: u32, row: &Row) {
        let spur = Spur::try_from_usize(relation_id as usize - 1).unwrap();
        let symbol = self.symbol_interner.rodeo.resolve(&spur);

        if let Some(relation) = self.storage.get_mut(symbol) {
            relation.remove_row(row)
        }
    }
    fn create_relation(&mut self, symbol: String, _relation_id: u32) {
        let new_relation = SimpleRelationWithOneIndexBacking::new(symbol.clone());
        self.storage.insert(symbol, new_relation);
    }
    fn delete_relation(&mut self, symbol: &str, _relation_id: u32) {
        self.storage.remove(symbol);
    }
}

impl<T: IndexBacking + PartialEq> Set for SimpleDatabaseWithIndex<T> {
    fn union(&self, other: &Self) -> Self {
        let mut out: SimpleDatabaseWithIndex<T> = SimpleDatabaseWithIndex::new(Interner::default());

        self.storage.iter().for_each(|(symbol, relation)| {
            let relation_id = out
                .symbol_interner
                .rodeo
                .get_or_intern(symbol)
                .into_inner()
                .get();

            relation
                .ward
                .iter()
                .for_each(|row| out.insert_at(relation_id, row.clone()))
        });

        other.storage.iter().for_each(|(symbol, relation)| {
            let relation_id = out
                .symbol_interner
                .rodeo
                .get_or_intern(symbol)
                .into_inner()
                .get();

            relation
                .ward
                .iter()
                .for_each(|row| out.insert_at(relation_id, row.clone()))
        });

        return out;
    }

    fn difference(&self, other: &Self) -> Self {
        let mut out: SimpleDatabaseWithIndex<T> = SimpleDatabaseWithIndex::new(Interner::default());

        self.storage.iter().for_each(|(symbol, relation)| {
            let relation_id = out
                .symbol_interner
                .rodeo
                .get_or_intern(symbol)
                .into_inner()
                .get();

            if other.storage.contains_key(symbol) {
                let other_relation = other.storage.get(symbol).unwrap();

                relation.ward.iter().for_each(|row| {
                    if !other_relation.ward.contains(row) {
                        out.insert_at(relation_id, row.clone())
                    }
                })
            } else {
                relation
                    .ward
                    .iter()
                    .for_each(|row| out.insert_at(relation_id, row.clone()))
            }
        });

        out
    }

    fn merge(&mut self, other: Self) {
        other.storage.into_iter().for_each(|(symbol, row_set)| {
            let relation_id = self
                .symbol_interner
                .rodeo
                .get_or_intern(symbol)
                .into_inner()
                .get();

            row_set
                .ward
                .into_iter()
                .for_each(|row| self.insert_at(relation_id, row))
        })
    }
}

impl<T: IndexBacking + PartialEq> Empty for SimpleDatabaseWithIndex<T> {
    fn is_empty(&self) -> bool {
        return self.storage.is_empty();
    }
}

impl<T: IndexBacking + PartialEq> SimpleDatabaseWithIndex<T> {
    pub fn evaluate(
        &self,
        expression: &RelationalExpression,
        view_name: &str,
    ) -> Option<SimpleRelationWithOneIndexBacking<T>> {
        return evaluate(expression, &self.storage, view_name);
    }
}

impl<T: IndexBacking + PartialEq> Default for SimpleDatabaseWithIndex<T> {
    fn default() -> Self {
        return Self {
            storage: Default::default(),
            symbol_interner: Default::default(),
        };
    }
}

impl<T: IndexBacking + PartialEq> SimpleDatabaseWithIndex<T> {
    pub(crate) fn new(symbol_interner: Interner) -> SimpleDatabaseWithIndex<T> {
        return Self {
            storage: Default::default(),
            symbol_interner,
        };
    }
}
