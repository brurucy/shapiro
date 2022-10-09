use super::vertebra::{Vertebra, INNER_SIZE};

#[derive(Debug, Clone, PartialEq)]
pub struct Spine<T>
where
    T: Clone + Ord,
{
    inner: Vec<Vertebra<T>>,
    len: usize,
}

impl<T: Clone + Ord> Spine<T> {
    pub fn new() -> Self {
        return Self {
            inner: vec![Vertebra::new()],
            len: 0,
        };
    }
    pub fn insert(&mut self, value: T) {
        let vertebrae = self.len;
        if vertebrae == 0 {
            self.inner[0].insert(value).unwrap();
            self.len += 1;
            return;
        }
        let mut idx = self
            .inner
            .partition_point(|vertebra| vertebra.max.clone().unwrap() < value);
        if let None = self.inner.get(idx) {
            idx = idx - 1
        }

        match self.inner[idx].insert(value.clone()) {
            // The err only occurs if the arrayvec's capacity is full
            Err(_) => {
                // We do the halving
                let new_vertebra = self.inner[idx].halve();
                // Get the minimum
                let new_vertebra_min = new_vertebra.inner[0].clone();
                // Insert the new vertebra
                self.inner.insert(idx + 1, new_vertebra);
                if value < new_vertebra_min {
                    self.inner[idx].insert(value).unwrap();
                } else {
                    self.inner[idx + 1].insert(value).unwrap();
                }
                self.len += 1;
                return;
            }
            Ok(added) => {
                if added {
                    self.len += 1;
                }
            }
        }
    }
    pub fn len(&self) -> usize {
        return self.len;
    }
}

impl<T> Default for Spine<T>
where
    T: Clone + Ord,
{
    fn default() -> Self {
        return Self {
            inner: vec![Vertebra::new()],
            len: 0,
        };
    }
}

impl<'a, T> IntoIterator for &'a Spine<T>
where
    T: Clone + Ord,
{
    type Item = &'a T;

    type IntoIter = SpineIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        return SpineIterator {
            spine: &self,
            current_idx: 0,
            current_iterator: self.inner[0].inner.iter(),
        };
    }
}

pub struct SpineIterator<'a, T>
where
    T: Clone + Ord,
{
    spine: &'a Spine<T>,
    current_idx: usize,
    current_iterator: std::slice::Iter<'a, T>,
}

impl<'a, T> Iterator for SpineIterator<'a, T>
where
    T: Clone + Ord,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(value) = self.current_iterator.next() {
            return Some(value);
        } else {
            let current_vertebra_idx = self.current_idx.clone();
            if (current_vertebra_idx + 1) >= self.spine.inner.len() {
                return None;
            }
            self.current_idx += 1;
            self.current_iterator = self.spine.inner[self.current_idx].inner.iter();
            if let Some(value) = self.current_iterator.next() {
                return Some(value);
            }
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Spine;

    #[test]
    fn test_insert() {
        let input: Vec<isize> = vec![1, 9, 2, 7, 6, 3, 5, 4, 10, 8];
        let expected_output: Vec<isize> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        let spine: Spine<isize> = input.iter().fold(Spine::new(), |mut acc, curr| {
            acc.insert(curr.clone());
            acc
        });

        let actual_output: Vec<isize> = spine.into_iter().cloned().collect();

        assert_eq!(expected_output, actual_output);
    }

    use rand::seq::SliceRandom;
    use rand::thread_rng;
    #[test]
    fn test_insert_with_balancing() {
        let mut rng = thread_rng();
        let mut input: Vec<isize> = (1..100_000).collect();
        input.shuffle(&mut rng);

        let expected_output: Vec<isize> = (1..100_000).collect();

        let spine: Spine<isize> = input.iter().fold(Spine::new(), |mut acc, curr| {
            acc.insert(curr.clone());
            acc
        });

        let actual_output: Vec<isize> = spine.into_iter().cloned().collect();

        assert_eq!(expected_output, actual_output);
    }
}
