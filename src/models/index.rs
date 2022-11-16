use crate::data_structures::spine::Spine;
use crate::models::datalog::TypedValue;
use std::collections::BTreeSet;
use im::{HashMap, Vector};
use rayon::prelude::*;
use crate::data_structures::hashmap::IndexedHashMap;
use crate::misc::generic_binary_join::generic_join_for_each;

pub type ValueRowId = (TypedValue, usize);

// IndexBacking allows the type that implements it to be used as an index
pub trait IndexBacking: Default + Clone + Sync + Send + PartialEq {
    fn insert_row(&mut self, _: ValueRowId) -> bool;
    fn join(&self, other: &Self, f: impl FnMut(usize, usize));
}

impl IndexBacking for BTreeSet<ValueRowId> {
    fn insert_row(&mut self, value: ValueRowId) -> bool { return self.insert(value) }
    fn join(&self, other: &BTreeSet<ValueRowId>, f: impl FnMut(usize, usize)) {
        generic_join_for_each(
            self,
            other,
            f
        );
    }
}

impl IndexBacking for Spine<ValueRowId> {
    fn insert_row(&mut self, value: ValueRowId) -> bool { return self.insert(value) }
    fn join(&self, other: &Spine<ValueRowId>, f: impl FnMut(usize, usize)) {
        generic_join_for_each(
            self,
            other,
            f
        );
    }
}

impl IndexBacking for Vec<ValueRowId> {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        self.push(value);
        return true
    }
    fn join(&self, other: &Vec<ValueRowId>, f: impl FnMut(usize, usize)) {
        let mut left = self.clone();
        let mut right = other.clone();
        rayon::join(|| {left.par_sort_unstable()}, || {right.par_sort_unstable();});
        generic_join_for_each(
            &left,
            &right,
            f);
    }
}

impl IndexBacking for Vector<ValueRowId> {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        self.push_back(value);
        return true;
    }

    fn join(&self, other: &Self, f: impl FnMut(usize, usize)) {
        let mut left = self.clone();
        let mut right = other.clone();
        rayon::join(|| {left.sort()}, || {right.sort();});
        generic_join_for_each(
            &left,
            &right,
            f);
    }
}

impl IndexBacking for IndexedHashMap<TypedValue, Vec<usize>> {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        if !self.contains_key(&value.0) {
            self.insert(value.0, vec![value.1]);
        } else {
            let idxs = self.get_mut(&value.0).unwrap();
            idxs.push(value.1);
        }
        return true;
    }
    fn join(&self, other: &IndexedHashMap<TypedValue, Vec<usize>>, mut f: impl FnMut(usize, usize)) {
        self
            .into_iter()
            .for_each(|(value, left_row_set)| {
                if let Some(right_row_set) = other.get(value) {
                    left_row_set
                        .iter()
                        .for_each(|left_row_idx| {
                            right_row_set
                                .iter()
                                .for_each(|right_row_idx| {
                                    f(*left_row_idx, *right_row_idx);
                                })
                        })
                }
            })
    }
}

impl IndexBacking for HashMap<TypedValue, Vec<usize>, ahash::RandomState> {
    fn insert_row(&mut self, value: ValueRowId) -> bool {
        if !self.contains_key(&value.0) {
            self.insert(value.0, vec![value.1]);
        } else {
            let idxs = self.get_mut(&value.0).unwrap();
            idxs.push(value.1);
        }
        return true;
    }
    fn join(&self, other: &HashMap<TypedValue, Vec<usize>, ahash::RandomState>, mut f: impl FnMut(usize, usize)) {
        self
            .into_iter()
            .for_each(|(value, left_row_set)| {
                if let Some(right_row_set) = other.get(value) {
                    left_row_set
                        .iter()
                        .for_each(|left_row_idx| {
                            right_row_set
                                .iter()
                                .for_each(|right_row_idx| {
                                    f(*left_row_idx, *right_row_idx);
                                })
                        })
                }
            })
    }
}

// Assumes both iterables to be sorted
#[derive(Clone, Debug, PartialEq)]
pub struct Index<T>
where T : IndexBacking,
{
    pub index: T,
    pub active: bool,
}