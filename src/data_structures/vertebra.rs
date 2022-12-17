use arrayvec::ArrayVec;

pub const INNER_SIZE: usize = 1024;
const CUTOFF: usize = INNER_SIZE / 2;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Vertebra<T>
where
    T: Clone + Ord,
{
    pub inner: ArrayVec<T, INNER_SIZE>,
    pub max: Option<T>,
}

impl<T: Clone + Ord> Vertebra<T> {
    pub fn new() -> Self {
        return Self {
            inner: ArrayVec::<T, INNER_SIZE>::new(),
            max: None,
        };
    }
    pub fn get(&self, index: usize) -> Option<&T> {
        return self.inner.get(index);
    }
    pub fn insert(&mut self, value: T) -> Result<bool, arrayvec::CapacityError<T>> {
        let mut added = false;
        let idx = self.inner.partition_point(|item| item.clone() < value);
        if let Some(value_at_idx) = self.inner.get(idx) {
            if *value_at_idx != value {
                let insert = self.inner.try_insert(idx, value.clone());
                if let Err(err) = insert {
                    return Err(err);
                }
                added = true;
            }
        } else {
            if let Err(err) = self.inner.try_push(value.clone()) {
                return Err(err);
            }
            added = true;
        }
        if let Some(max) = &self.max {
            if value > max.clone() {
                self.max = Some(value)
            }
        } else {
            self.max = Some(value)
        }
        return Ok(added);
    }
    pub fn halve(&mut self) -> Self {
        let mut latter_half = Self::new();
        let mut idx = 0;
        self.inner.retain(|item| {
            if idx >= CUTOFF {
                latter_half.insert(item.clone()).unwrap();
            }
            let current_idx = idx.clone();
            idx += 1;
            current_idx < CUTOFF
        });
        self.max = self.inner.get(self.inner.len() - 1).cloned();
        return latter_half;
    }
    pub fn len(&self) -> usize {
        return self.inner.len();
    }
}

#[cfg(test)]
mod tests {
    use super::{Vertebra, CUTOFF, INNER_SIZE};

    #[test]
    fn test_insert() {
        let input: Vec<isize> = vec![1, 9, 2, 7, 6, 3, 5, 4, 10, 8];

        let expected_output: Vec<isize> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        let actual_vertebra = input.iter().fold(Vertebra::new(), |mut acc, curr| {
            acc.insert(curr).unwrap();
            acc
        });

        let actual_output: Vec<isize> = actual_vertebra.inner.into_iter().cloned().collect();

        assert_eq!(expected_output, actual_output);
        assert_eq!(*actual_vertebra.max.unwrap(), 10);
    }

    #[test]
    fn test_halve() {
        let mut input: Vec<isize> = vec![];
        for item in 0..INNER_SIZE {
            input.push(item.clone() as isize);
        }

        let mut former_vertebra = Vertebra::new();
        input.iter().for_each(|item| {
            former_vertebra.insert(item.clone()).unwrap();
        });
        let latter_vertebra = former_vertebra.halve();

        let expected_former_output: Vec<isize> = input[0..CUTOFF].to_vec();
        let expected_latter_output: Vec<isize> = input[CUTOFF..].to_vec();

        let actual_former_output: Vec<isize> = former_vertebra.inner.iter().cloned().collect();
        let actual_latter_output: Vec<isize> = latter_vertebra.inner.iter().cloned().collect();

        assert_eq!(expected_former_output, actual_former_output);
        assert_eq!(expected_latter_output, actual_latter_output);
    }
}
