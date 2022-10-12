use crate::data_structures::spine::Spine;
use crate::models::datalog::TypedValue;
use std::collections::{BTreeSet, HashMap};

pub type ValueRowId = (TypedValue, Box<[TypedValue]>);

pub trait IndexBacking: IntoIterator<Item=ValueRowId> + Default + Clone + Sync + Send + PartialEq {
    fn insert(&mut self, _: ValueRowId) -> bool;
}

impl IndexBacking for BTreeSet<ValueRowId> {
    fn insert(&mut self, value: ValueRowId) -> bool {
        return self.insert(value)
    }
}

impl IndexBacking for Spine<ValueRowId> {
    fn insert(&mut self, value: ValueRowId) -> bool { return self.insert(value) }
}

// Do not use this.
// This is a dummy implementation for scenarios where an index is not needed.
impl IndexBacking for Vec<ValueRowId> {
    fn insert(&mut self, _: ValueRowId) -> bool { unreachable!() }
}

// Assumes both iterables to be sorted
#[derive(Clone, Debug, PartialEq)]
pub struct Index<T>
where T : IndexBacking,
{
    pub index: T,
    pub active: bool,
}