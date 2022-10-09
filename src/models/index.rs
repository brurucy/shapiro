use crate::data_structures::spine::Spine;
use crate::models::datalog::TypedValue;
use std::cmp::Ordering;
use std::collections::BTreeSet;

// Assumes both iterables to be sorted
pub fn generic_join_for_each<'a, K: 'a, V: 'a>(
    left_iter: impl IntoIterator<Item = (K, V)>,
    right_iter: impl IntoIterator<Item = (K, V)>,
    mut f: impl FnMut(&V, &V),
) where
    K: Ord + Clone,
    V: Clone,
{
    let mut left_iterator = left_iter.into_iter();

    let mut right_iterator = right_iter.into_iter();

    let (mut current_left, mut current_right) = (left_iterator.next(), right_iterator.next());
    loop {
        if let Some(left) = current_left.clone() {
            if let Some(right) = current_right.clone() {
                match left.0.cmp(&right.0) {
                    Ordering::Less => {
                        current_left = left_iterator.next();
                    }
                    Ordering::Equal => {
                        let mut left_matches: Vec<&V> = vec![];
                        left_matches.push(&left.1);
                        let mut right_matches: Vec<&V> = vec![];
                        right_matches.push(&right.1);
                        loop {
                            current_left = left_iterator.next();
                            if let Some(left_next) = current_left.as_ref() {
                                if left_next.0.cmp(&left.0) == Ordering::Equal {
                                    left_matches.push(&left.1);
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        loop {
                            current_right = right_iterator.next();
                            if let Some(right_next) = current_right.as_ref() {
                                if right_next.0.cmp(&right.0) == Ordering::Equal {
                                    right_matches.push(&right.1);
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
                        current_right = right_iterator.next();
                    }
                }
            } else {
                break;
            }
        } else {
            break;
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Index {
    pub index: BTreeSet<(TypedValue, usize)>,
    pub active: bool,
}

#[cfg(test)]
mod tests {
    use crate::models::index::generic_join_for_each;

    #[test]
    fn test_generic_join() {
        let left = vec![(1, 1), (1, 2), (2, 1)];
        let right = vec![(1, 2), (1, 3), (2, 2)];

        let mut actual_product = vec![];

        generic_join_for_each(left.into_iter(), right.into_iter(), |l, r| {
            actual_product.push((l.clone(), r.clone()))
        });
    }
}
