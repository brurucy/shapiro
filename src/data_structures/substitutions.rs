use crate::models::datalog::TypedValue;
use std::fmt::{write, Debug, Display, Formatter};

pub type Substitution = (u8, TypedValue);

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Substitutions {
    pub inner: Vec<(u8, TypedValue)>,
}

impl Substitutions {
    pub fn new() -> Self {
        return Self { inner: Vec::new() };
    }
    pub fn get(&self, key: u8) -> Option<&TypedValue> {
        for sub in &self.inner {
            if sub.0 == key {
                return Some(&sub.1);
            }
        }

        // let idx = self.inner.partition_point(|item| item.0 < key);
        // if let Some(value_at_idx) = self.inner.get(idx) {
        //     if value_at_idx.0 == key {
        //         return Some(value_at_idx.1.clone());
        //     }
        // }
        return None;
    }

    pub fn insert(&mut self, value: Substitution) {
        if let None = self.get(value.0) {
            self.inner.push(value)
        }

        // let idx = self.inner.partition_point(|item| item.0 < value.0);
        // if let Some(value_at_idx) = self.inner.get(idx) {
        //     if value_at_idx.0 != value.0 {
        //         self.inner.insert(idx, value.clone());
        //     } else {
        //         return None;
        //     }
        // } else {
        //     self.inner.push(value.clone())
        // }
        // return Some(value.clone());
    }
    pub fn len(&self) -> usize {
        return self.inner.len();
    }
    pub fn extend(&mut self, other: Self) {
        other.inner.into_iter().for_each(|sub| {
            self.insert(sub);
        })
    }
}

impl Default for Substitutions {
    fn default() -> Self {
        return Self { inner: Vec::new() };
    }
}

impl Display for Substitutions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut subs = vec![];

        self.inner.iter().for_each(|substitution| {
            let formatted_sub = format!("{} |-> {}", substitution.0, substitution.1);
            subs.push(formatted_sub);
        });

        write!(f, "[{}]", subs.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use crate::data_structures::substitutions::Substitutions;
    use crate::models::datalog::TypedValue;

    // #[test]
    // fn test_insert() {
    //     let mut subs = Substitutions::new();
    //
    //     let zeroth_sub = (1, TypedValue::Str("one".to_string()));
    //     let first_sub = (0, TypedValue::Str("one".to_string()));
    //     let second_sub = (0, TypedValue::Str("two".to_string()));
    //
    //     subs.insert(first_sub.clone());
    //     assert_eq!(subs.get(first_sub.0).unwrap(), first_sub);
    //
    //
    //     assert_eq!(subs.insert(first_sub.clone()), Some(first_sub.clone()));
    //     assert_eq!(subs.insert(second_sub), None);
    //     assert_eq!(subs.insert(zeroth_sub.clone()), Some(zeroth_sub.clone()));
    //     assert_eq!(subs.inner, vec![first_sub, zeroth_sub])
    // }
    //
    // #[test]
    // fn test_get() {
    //     let mut subs = Substitutions::new();
    //
    //     let zeroth_sub = (1, TypedValue::Str("one".to_string()));
    //     let first_sub = (0, TypedValue::Str("one".to_string()));
    //
    //     subs.insert(zeroth_sub);
    //     subs.insert(first_sub);
    //
    //     assert_eq!(subs.get(0), Some(TypedValue::Str("one".to_string())));
    //     assert_eq!(subs.get(1), Some(TypedValue::Str("one".to_string())))
    // }
    //
    // #[test]
    // fn test_extend() {
    //     let mut subs_left = Substitutions::new();
    //     let zeroth_sub = (0, TypedValue::Str("one".to_string()));
    //     subs_left.insert(zeroth_sub);
    //
    //     let mut subs_right = Substitutions::new();
    //     let first_sub = (1, TypedValue::Str("one".to_string()));
    //     subs_right.insert(first_sub);
    //
    //     subs_left.extend(&subs_right);
    //
    //     assert_eq!(
    //         subs_left.inner,
    //         vec![
    //             (0, TypedValue::Str("one".to_string())),
    //             (1, TypedValue::Str("one".to_string())),
    //         ]
    //     )
    // }
}
