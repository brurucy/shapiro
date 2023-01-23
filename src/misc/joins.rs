use std::cmp::Ordering;

pub fn nested_loop_join<'a, K: 'a, V: 'a, Left: 'a, Right: 'a>(
    left_iter: &'a Left,
    right_iter: &'a Right,
    mut f: impl FnMut(V, V),
) where
    &'a Left: 'a + IntoIterator<Item = &'a (K, V)>,
    &'a Right: 'a + IntoIterator<Item = &'a (K, V)>,
    K: Ord + Clone,
    V: Clone,
{
    left_iter.into_iter().for_each(|left_row| {
        right_iter.into_iter().for_each(|right_row| {
            f(left_row.1.clone(), right_row.1.clone());
        })
    })
}

pub fn sort_merge_join<'a, K: 'a, V: 'a, Left: 'a, Right: 'a>(
    left_iter: &'a Left,
    right_iter: &'a Right,
    mut f: impl FnMut(V, V),
) where
    &'a Left: 'a + IntoIterator<Item = &'a (K, V)>,
    &'a Right: 'a + IntoIterator<Item = &'a (K, V)>,
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
                        let mut left_matches: Vec<&'a V> = vec![];
                        left_matches.push(&left.1);
                        let mut right_matches: Vec<&'a V> = vec![];
                        right_matches.push(&right.1);

                        loop {
                            current_left = left_iterator.next();
                            if let Some(left_next) = current_left.as_ref() {
                                if left_next.0.cmp(&left.0) == Ordering::Equal {
                                    left_matches.push(&left_next.1);
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
                                    right_matches.push(&right_next.1);
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        if left_matches.len() * right_matches.len() != 0 {
                            left_matches.into_iter().for_each(|left_value| {
                                right_matches.iter().for_each(|right_value| {
                                    f(left_value.clone(), right_value.clone().clone());
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

#[cfg(test)]
mod tests {
    use crate::misc::joins::sort_merge_join;

    #[test]
    fn test_generic_join() {
        let left = vec![(1, 1), (1, 2), (2, 1)];
        let right = vec![(1, 2), (1, 3), (2, 2)];

        let mut actual_product = vec![];

        sort_merge_join(&left, &right, |l, r| {
            actual_product.push((l.clone(), r.clone()))
        });

        assert_eq!(vec![(1, 2), (1, 3), (2, 2), (2, 3), (1, 2)], actual_product)
    }
}
