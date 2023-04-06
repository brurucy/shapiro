use crate::reasoning::reasoners::differential::abomonated_model::AbomonatedTypedValue;
use abomonation_derive::Abomonation;
use itertools::Itertools;
use std::fmt::{Display, Formatter};

pub type AbomonatedSubstitution = (u8, AbomonatedTypedValue);

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Abomonation, Hash)]
pub struct AbomonatedSubstitutions {
    pub inner: AbomonatedVertebra,
}

impl Default for AbomonatedSubstitutions {
    fn default() -> Self {
        AbomonatedSubstitutions {
            inner: Default::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Abomonation, Hash)]
pub struct AbomonatedVertebra {
    pub inner: Vec<AbomonatedSubstitution>,
}

impl AbomonatedVertebra {
    pub fn new() -> Self {
        return Self { inner: Vec::new() };
    }
    pub fn get(&self, key: u8) -> Option<AbomonatedTypedValue> {
        let idx = self.inner.partition_point(|item| item.0 < key);
        if let Some(value_at_idx) = self.inner.get(idx) {
            if value_at_idx.0 == key {
                return Some(value_at_idx.1.clone());
            }
        }
        return None;
    }

    pub fn insert(&mut self, value: AbomonatedSubstitution) -> Option<AbomonatedSubstitution> {
        let idx = self.inner.partition_point(|item| item.0 < value.0);
        if let Some(value_at_idx) = self.inner.get(idx) {
            if value_at_idx.0 != value.0 {
                self.inner.insert(idx, value.clone());
            } else {
                return None;
            }
        } else {
            self.inner.push(value.clone())
        }
        return Some(value.clone());
    }
    pub fn len(&self) -> usize {
        return self.inner.len();
    }
    pub fn extend(&mut self, other: &Self) {
        other.inner.iter().for_each(|sub| {
            self.insert(sub.clone());
        })
    }
}

impl Default for AbomonatedVertebra {
    fn default() -> Self {
        return Self { inner: Vec::new() };
    }
}

impl Display for AbomonatedVertebra {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.inner
                .iter()
                .map(|(v, c)| format!("(?{}: Const({}))", v, c))
                .join(" ")
        )
    }
}