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
        if self.len == 0 {
            self.inner[0].insert(value).unwrap();
            self.len += 1;
            return;
        }
        let idx = self
            .inner
            .partition_point(|vertebra| vertebra.max.clone().unwrap() < value);
        match self.inner[idx].insert(value.clone()) {
            Err(_) => {
                let new_vertebra = self.inner[idx].halve();
                self.inner.insert(idx + 1, new_vertebra);
                self.inner[idx + 1].insert(value).unwrap();
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

// It took me so long to figure this out :)
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

        let spine = input.iter().fold(Spine::new(), |mut acc, curr| {
            acc.insert(curr);
            acc
        });

        let actual_output: Vec<isize> = spine.into_iter().cloned().cloned().collect();

        assert_eq!(expected_output, actual_output);
    }
}
