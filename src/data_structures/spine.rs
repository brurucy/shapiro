use super::vertebra::Vertebra;
use crate::data_structures::fenwick_tree::FenwickTree;

#[derive(Debug, Clone, PartialEq)]
pub struct Spine<T>
where
    T: Clone + Ord,
{
    inner: Vec<Vertebra<T>>,
    index: FenwickTree,
    len: usize,
}

impl<T: Clone + Ord> Spine<T> {
    pub fn new() -> Self {
        return Self {
            ..Default::default()
        };
    }
    pub fn insert(&mut self, value: T) -> bool {
        let added = false;
        let vertebrae = self.len;
        if vertebrae == 0 {
            self.inner[0].insert(value).unwrap();
            self.len += 1;
            return true;
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
                self.index = FenwickTree::new(&self.inner, |vertebra| vertebra.len());
                return added;
            }
            Ok(added) => {
                if added {
                    self.len += 1;
                    self.index.increase_length(idx)
                }
                return added;
            }
        }
    }
    pub fn get(&self, idx: usize) -> Option<&T> {
        let vertebra_index = self.index.index_of(idx);
        let mut offset = 0;

        if vertebra_index != 0 {
            offset = self.index.prefix_sum(vertebra_index);
        }

        let mut position_within_vertebra = idx - offset;

        let mut vertebra = self.inner.get(vertebra_index);
        if let Some(candidate_vertebra) = self.inner.get(vertebra_index) {
            if position_within_vertebra >= candidate_vertebra.len() {
                if let Some(candidate_two_vertebra) = self.inner.get(vertebra_index + 1) {
                    vertebra = Some(candidate_two_vertebra);
                    position_within_vertebra = 0;
                } else {
                    return None;
                }
            }
        } else {
            return None;
        }

        if let Some(value) = vertebra.unwrap().get(position_within_vertebra) {
            return Some(value);
        }

        return None;
    }
    pub fn binary_search_by(&self, lower_bound: usize, mut cmp: impl FnMut(&T) -> bool) -> usize {
        let mut hi = self.len();
        let mut lo = lower_bound;
        while lo < hi {
            let mid = (hi + lo) / 2;
            let el: &T = self.get(mid).unwrap();
            if cmp(el) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
    pub fn seek(&self, target: &T) -> Option<&T> {
        let mut vertebra_idx = self
            .inner
            .partition_point(|vertebra| vertebra.max.clone().unwrap() < *target);

        if let None = self.inner.get(vertebra_idx) {
            vertebra_idx = vertebra_idx - 1
        }

        let idx = self.inner[vertebra_idx]
            .inner
            .partition_point(|item| item.clone() < *target);

        return self.inner[vertebra_idx].get(idx);
    }
    pub fn len(&self) -> usize {
        return self.len;
    }
}

impl<T> FromIterator<T> for Spine<T>
where
    T: Ord + Clone,
{
    fn from_iter<K: IntoIterator<Item = T>>(iter: K) -> Self {
        let mut spine = Spine::new();
        iter.into_iter().for_each(|item| {
            spine.insert(item);
        });
        return spine;
    }
}

impl<T> Default for Spine<T>
where
    T: Clone + Ord,
{
    fn default() -> Self {
        let v = vec![Vertebra::new()];
        return Self {
            inner: v.clone(),
            index: FenwickTree::new(v, |vertebra| vertebra.len()),
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
        return if let Some(value) = self.current_iterator.next() {
            Some(value)
        } else {
            let current_vertebra_idx = self.current_idx;
            if (current_vertebra_idx + 1) >= self.spine.inner.len() {
                return None;
            }
            self.current_idx += 1;
            self.current_iterator = self.spine.inner[self.current_idx].inner.iter();
            if let Some(value) = self.current_iterator.next() {
                return Some(value);
            }
            None
        };
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
    fn test_insert_with_balancing_fuzz() {
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

    #[test]
    fn test_get_fuzz() {
        let mut rng = thread_rng();
        let mut input: Vec<usize> = (1..100_000).collect();
        input.shuffle(&mut rng);

        let expected_output: Vec<usize> = (1..100_000).collect();

        let spine: Spine<usize> = input.iter().fold(Spine::new(), |mut acc, curr| {
            acc.insert(curr.clone());
            acc
        });

        expected_output
            .into_iter()
            .for_each(|item| assert_eq!(*&spine.get(item - 1).cloned().unwrap(), item));
    }

    #[test]
    fn test_binary_search_fuzz() {
        let mut rng = thread_rng();
        let mut input: Vec<usize> = (1..100_000).collect();
        input.shuffle(&mut rng);

        let expected_output: Vec<usize> = (1..100_000).collect();

        let spine: Spine<usize> = input.iter().fold(Spine::new(), |mut acc, curr| {
            acc.insert(curr.clone());
            acc
        });

        expected_output
            .into_iter()
            .for_each(|item| assert_eq!(item - 1, spine.binary_search_by(0, |el| item > *el)));
    }
}
