use crate::data_structures::spine::Spine;
use crate::misc::joins::sort_merge_join;
use crate::models::datalog::TypedValue;
use im::{HashMap, Vector};
use indexmap::IndexMap;
use rayon::prelude::*;
use std::collections::BTreeSet;

pub type ValueRowId = (TypedValue, usize);
pub type HashMapIndex = HashMap<TypedValue, Vec<usize>, ahash::RandomState>;
pub type IndexedHashMapIndex = IndexMap<TypedValue, Vec<usize>, ahash::RandomState>;
pub type ImmutableVectorIndex = Vector<ValueRowId>;
pub type VecIndex = Vec<ValueRowId>;
pub type SpineIndex = Spine<ValueRowId>;
pub type BTreeIndex = BTreeSet<ValueRowId>;

// IndexBacking allows the type that implements it to be used as an index
pub trait IndexBacking: Default + Clone + Sync + Send + PartialEq {
    fn insert_row(&mut self, _: ValueRowId) -> bool;
    fn join(&self, other: &Self, f: impl FnMut(usize, usize));
}

impl IndexBacking for BTreeIndex {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        return self.insert(value);
    }
    fn join(&self, other: &BTreeIndex, f: impl FnMut(usize, usize)) {
        sort_merge_join(self, other, f);
    }
}

impl IndexBacking for SpineIndex {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        return self.insert(value);
    }
    fn join(&self, other: &SpineIndex, f: impl FnMut(usize, usize)) {
        sort_merge_join(self, other, f);
    }
}

impl IndexBacking for VecIndex {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        self.push(value);
        return true;
    }
    fn join(&self, other: &VecIndex, f: impl FnMut(usize, usize)) {
        let mut left = self.clone();
        let mut right = other.clone();
        rayon::join(
            || left.par_sort_unstable(),
            || {
                right.par_sort_unstable();
            },
        );
        sort_merge_join(&left, &right, f);
    }
}

impl IndexBacking for ImmutableVectorIndex {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        self.push_back(value);
        return true;
    }

    fn join(&self, other: &Self, f: impl FnMut(usize, usize)) {
        let mut left = self.clone();
        let mut right = other.clone();
        rayon::join(
            || left.sort(),
            || {
                right.sort();
            },
        );
        sort_merge_join(&left, &right, f);
    }
}

impl IndexBacking for IndexedHashMapIndex {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        if !self.contains_key(&value.0) {
            self.insert(value.0, vec![value.1]);
        } else {
            let idxs = self.get_mut(&value.0).unwrap();
            idxs.push(value.1);
        }
        return true;
    }
    fn join(&self, other: &IndexedHashMapIndex, mut f: impl FnMut(usize, usize)) {
        self.into_iter().for_each(|(value, left_row_set)| {
            if let Some(right_row_set) = other.get(value) {
                left_row_set.iter().for_each(|left_row_idx| {
                    right_row_set.iter().for_each(|right_row_idx| {
                        f(left_row_idx.clone(), right_row_idx.clone());
                    })
                })
            }
        })
    }
}

impl IndexBacking for HashMapIndex {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        if !self.contains_key(&value.0) {
            self.insert(value.0, vec![value.1]);
        } else {
            let idxs = self.get_mut(&value.0).unwrap();
            idxs.push(value.1);
        }
        return true;
    }
    fn join(&self, other: &HashMapIndex, mut f: impl FnMut(usize, usize)) {
        self.into_iter().for_each(|(value, left_row_set)| {
            if let Some(right_row_set) = other.get(value) {
                left_row_set.iter().for_each(|left_row_idx| {
                    right_row_set.iter().for_each(|right_row_idx| {
                        f(left_row_idx.clone(), right_row_idx.clone());
                    })
                })
            }
        })
    }
}

// Assumes both iterables to be sorted
#[derive(Clone, Debug, PartialEq)]
pub struct Index<T>
where
    T: IndexBacking,
{
    pub index: T,
    pub active: bool,
}
