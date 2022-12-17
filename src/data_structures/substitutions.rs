use crate::models::datalog::TypedValue;

pub type Substitution = (u8, TypedValue);

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Substitutions {
    pub inner: Vec<(u8, TypedValue)>,
}

impl Substitutions {
    pub fn new() -> Self {
        return Self { inner: Vec::new() };
    }
    pub fn get(&self, key: u8) -> Option<TypedValue> {
        let idx = self.inner.partition_point(|item| item.0 < key);
        if let Some(value_at_idx) = self.inner.get(idx) {
            if value_at_idx.0 == key {
                return Some(value_at_idx.1.clone());
            }
        }
        return None;
    }

    pub fn insert(&mut self, value: Substitution) -> Option<Substitution> {
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

impl Default for Substitutions {
    fn default() -> Self {
        return Self { inner: Vec::new() };
    }
}

#[cfg(test)]
mod tests {
    use crate::data_structures::substitutions::Substitutions;
    use crate::models::datalog::TypedValue;

    #[test]
    fn test_insert() {
        let mut subs = Substitutions::new();

        let zeroth_sub = (1, TypedValue::Str("one".to_string()));
        let first_sub = (0, TypedValue::Str("one".to_string()));
        let second_sub = (0, TypedValue::Str("two".to_string()));

        assert_eq!(subs.insert(first_sub.clone()), Some(first_sub.clone()));
        assert_eq!(subs.insert(second_sub), None);
        assert_eq!(subs.insert(zeroth_sub.clone()), Some(zeroth_sub.clone()));
        assert_eq!(subs.inner, vec![first_sub, zeroth_sub])
    }

    #[test]
    fn test_get() {
        let mut subs = Substitutions::new();

        let zeroth_sub = (1, TypedValue::Str("one".to_string()));
        let first_sub = (0, TypedValue::Str("one".to_string()));

        subs.insert(zeroth_sub);
        subs.insert(first_sub);

        assert_eq!(subs.get(0), Some(TypedValue::Str("one".to_string())));
        assert_eq!(subs.get(1), Some(TypedValue::Str("one".to_string())))
    }

    #[test]
    fn test_extend() {
        let mut subs_left = Substitutions::new();
        let zeroth_sub = (0, TypedValue::Str("one".to_string()));
        subs_left.insert(zeroth_sub);

        let mut subs_right = Substitutions::new();
        let first_sub = (1, TypedValue::Str("one".to_string()));
        subs_right.insert(first_sub);

        subs_left.extend(&subs_right);

        assert_eq!(
            subs_left.inner,
            vec![
                (0, TypedValue::Str("one".to_string())),
                (1, TypedValue::Str("one".to_string())),
            ]
        )
    }
}
