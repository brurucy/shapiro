use crate::data_structures::spine::Spine;
use crate::models::datalog::TypedValue;
use std::cmp::Ordering;
use std::collections::BTreeSet;

// Assumes both iterables to be sorted
#[derive(Clone, Debug, PartialEq)]
pub struct Index {
    pub index: BTreeSet<(TypedValue, usize)>,
    pub active: bool,
}