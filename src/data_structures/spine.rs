use std::cmp::Ordering;
use crate::data_structures::fenwick_tree::FenwickTree;
use super::vertebra::{Vertebra};

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
        }
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
                self.index = FenwickTree::new(&self.inner, |vertebra| vertebra.len());
                return;
            }
            Ok(added) => {
                if added {
                    self.len += 1;
                    self.index.increase_length(idx)
                }
            }
        }
    }
    pub fn get(&self, idx: usize) -> Option<&T> {
        let vertebra_index = self.index.index_of(idx);
        let offset = self.index.prefix_sum(vertebra_index);
        return self.inner[vertebra_index].get(idx - offset)
    }
    pub fn binary_search(&self, target: &T, lower_bound: usize) -> usize {
        let mut hi = self.len();
        let mut lo = lower_bound;
        while lo < hi {
            let mid = (hi + lo) / 2;
            let el: &T = self.get(mid).unwrap();
            if target > el {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
    pub fn join(&self, other: &Spine<T>, mut f: impl FnMut(&T, &T)) {
        let mut left_iter = 0;
        let mut right_iter = 0;

        let (mut current_left, mut current_right) = (self.get(0), other.get(0));
        loop {
            if let Some(left) = current_left.clone() {
                if let Some(right) = current_right.clone() {
                    match left.cmp(&right) {
                        Ordering::Less => {
                            left_iter = self.binary_search(&right, 0);
                            current_left = self.get(left_iter);
                        }
                        Ordering::Equal => {
                            let mut left_matches: Vec<&T> = vec![];
                            left_matches.push(&left);
                            let mut right_matches: Vec<&T> = vec![];
                            right_matches.push(&right);

                            loop {
                                current_left = self.get(left_iter + 1);
                                if let Some(left_next) = current_left.as_ref() {
                                    if left_next.cmp(&left) == Ordering::Equal {
                                        left_matches.push(&left);
                                    } else {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }

                            loop {
                                current_right = self.get(right_iter + 1);
                                if let Some(right_next) = current_right.as_ref() {
                                    if right_next.cmp(&right) == Ordering::Equal {
                                        right_matches.push(&right);
                                    } else {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }

                            let mut matches = 0;
                            if left_matches.len() * right_matches.len() != 0 {
                                left_matches.into_iter().for_each(|left_value| {
                                    right_matches.clone().into_iter().for_each(|right_value| {
                                        matches += 1;
                                        f(left_value, right_value);
                                    })
                                });
                            }
                        }
                        Ordering::Greater => {
                            right_iter = other.binary_search(&left, 0);
                            current_right = other.get(right_iter);
                        }
                    }
                }
            }
        }
    }
    pub fn seek(&self, target: &T) -> Option<&T> {
        let mut vertebra_idx = self
            .inner
            .partition_point(|vertebra| vertebra.max.clone().unwrap() < *target);

        if let None = self.inner.get(vertebra_idx) {
            vertebra_idx = vertebra_idx - 1
        }

        let idx = self.inner[vertebra_idx].inner.partition_point(|item| item.clone() < *target);

        return self.inner[vertebra_idx].get(idx)
    }
    pub fn iter(&self) -> SpineIterator<T> {
        return self.into_iter()
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
            let current_vertebra_idx = self.current_idx.clone();
            if (current_vertebra_idx + 1) >= self.spine.inner.len() {
                return None;
            }
            self.current_idx += 1;
            self.current_iterator = self.spine.inner[self.current_idx].inner.iter();
            if let Some(value) = self.current_iterator.next() {
                return Some(value);
            }
            None
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
        let mut input: Vec<isize> = (1..100_000).collect();
        input.shuffle(&mut rng);

        let expected_output: Vec<isize> = (1..100_000).collect();

        let spine: Spine<isize> = input.iter().fold(Spine::new(), |mut acc, curr| {
            acc.insert(curr.clone());
            acc
        });

        let actual_output: Vec<isize> = spine.into_iter().cloned().collect();

        expected_output
            .iter()
            .for_each(|item| {
                assert_eq!(spine.get((item - 1) as usize).unwrap(), item)
            });
    }

    #[test]
    fn test_binary_search_fuzz() {
        let mut rng = thread_rng();
        let mut input: Vec<isize> = (1..100_000).collect();
        input.shuffle(&mut rng);

        let expected_output: Vec<isize> = (1..100_000).collect();

        let spine: Spine<isize> = input.iter().fold(Spine::new(), |mut acc, curr| {
            acc.insert(curr.clone());
            acc
        });

        let actual_output: Vec<isize> = spine.into_iter().cloned().collect();

        expected_output
            .iter()
            .for_each(|item| {
                assert_eq!((item - 1) as usize, spine.binary_search(item, 0))
            });
    }
}
