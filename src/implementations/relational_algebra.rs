use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::time::Instant;

use crate::models::datalog::TypedValue;
use crate::models::instance::Database;
use crate::models::relational_algebra::{
    Index, Relation, RelationalExpression, SelectionTypedValue, Term,
};

pub fn select_value(relation: &mut Relation, column_idx: usize, value: SelectionTypedValue) {
    relation.ward.clone().iter().for_each(|(k, _v)| {
        if k[column_idx] != value.clone().try_into().unwrap() {
            relation.mark_deleted(&k);
        }
    });
}

pub fn select_equality(relation: &mut Relation, left_column_idx: usize, right_column_idx: usize) {
    relation.ward.clone().iter().for_each(|(k, v)| {
        if k[left_column_idx] != k[right_column_idx] {
            relation.mark_deleted(&k);
        }
    });
}

pub fn product(left_relation: &Relation, right_relation: &Relation) -> Relation {
    let mut relation = Relation::new(&(left_relation.symbol.to_string() + &right_relation.symbol), left_relation.get_row(0).len() + right_relation.get_row(0).len(), false);

    left_relation.ward.iter().for_each(|(left_k, left_v)| {
        if *left_v {
            right_relation.ward.iter().for_each(|(right_k, right_v)| {
                if *right_v {
                    relation.insert_typed(
                        left_k
                            .clone()
                            .iter()
                            .chain(right_k.iter())
                            .cloned()
                            .collect()
                    )
                }
            })
        }
    });

    return relation;
}

pub fn hash_join(
    left_relation: &Relation,
    right_relation: &Relation,
    left_index: usize,
    right_index: usize,
) -> Relation {
    let mut relation = Relation::new(&(left_relation.symbol.to_string() + &right_relation.symbol), left_relation.get_row(0).len() + right_relation.get_row(0).len(), false);

    let builder = left_relation
        .ward
        .iter()
        .fold(HashMap::new(), |mut acc, (row, notdeleted)| {
            if *notdeleted {
                if !acc.contains_key(&row[left_index]) {
                    acc.insert(row[left_index].clone(), vec![row]);
                } else {
                    let rows = acc.get_mut(&row[left_index]).unwrap();
                    rows.push(row);
                }
            }
            acc
        });

    right_relation
        .ward
        .iter()
        .for_each(|(right_row, notdeleted)| {
            if *notdeleted {
                if let Some(row_set) = builder.get(&right_row[right_index]) {
                    row_set.into_iter().for_each(|left_row| {
                        relation.insert_typed(
                            left_row
                                .clone()
                                .iter()
                                .chain(right_row.iter())
                                .cloned()
                                .collect()
                        )
                    })
                }
            }
    });

    return relation
}

pub fn join(
    left_relation: &Relation,
    right_relation: &Relation,
    left_index: usize,
    right_index: usize,
) -> Relation {
    let mut left_iterator = left_relation.indexes[left_index]
        .index
        .clone()
        .into_iter()
        .filter_map(|idx| {
            let row = left_relation.get_row(idx.1);
            let sign = left_relation.ward.get(&row).unwrap();
            if *sign == true {
                return Some((row, idx));
            }
            return None;
        });

    let mut right_iterator = right_relation.indexes[right_index]
        .index
        .clone()
        .into_iter()
        .filter_map(|idx| {
            let row = right_relation.get_row(idx.1);
            let sign = right_relation.ward.get(&row).unwrap();
            if *sign == true {
                return Some((row, idx));
            }
            return None;
        });

    let mut relation = Relation::new(&(left_relation.symbol.to_string() + &right_relation.symbol), left_relation.get_row(0).len() + right_relation.get_row(0).len(), true);

    let (mut current_left, mut current_right) = (left_iterator.next(), right_iterator.next());
    let mut cnt = 0;
    loop {
        cnt+=1;
        if let Some(left_zip) = current_left.clone() {
            if let Some(right_zip) = current_right.clone() {
                let left_index_value = left_zip.1;
                let right_index_value = right_zip.1;

                match left_index_value.0.cmp(&right_index_value.0) {
                    Ordering::Less => {
                        current_left = left_iterator.next();
                    }
                    Ordering::Equal => {
                        let mut left_matches: Vec<(Box<[TypedValue]>)> = vec![];
                        left_matches.push(left_zip.0);
                        let mut right_matches: Vec<(Box<[TypedValue]>)> = vec![];
                        right_matches.push(right_zip.0);
                        loop {
                            current_left = left_iterator.next();
                            if let Some(left) = current_left.as_ref() {
                                if left.1 .0.cmp(&left_index_value.0) == Ordering::Equal {
                                    left_matches.push(left.clone().0);
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        loop {
                            current_right = right_iterator.next();
                            if let Some(right) = current_right.as_ref() {
                                if right.1 .0.cmp(&right_index_value.0) == Ordering::Equal {
                                    right_matches.push(right.clone().0);
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        let mut matches = 0;
                        if left_matches.len() * right_matches.len() != 0 {
                            left_matches.iter().for_each(|left_value| {
                                right_matches.iter().for_each(|right_value| {
                                    matches += 1;
                                    let row = left_value
                                        .clone()
                                        .iter()
                                        .chain(right_value.into_iter())
                                        .cloned()
                                        .collect();
                                    relation.insert_typed(row);
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

    return relation;
}

pub fn project(
    relation: &Relation,
    column_indexes: &Vec<SelectionTypedValue>,
    new_symbol: &str,
) -> Relation {
    let mut new_relation = Relation::new(new_symbol, column_indexes.len(), false);

    relation
        .ward
        .iter()
        .for_each(|(row, sign)| {
            if *sign {
                let row = column_indexes
                    .clone()
                    .into_iter()
                    .map(|column_idx| match column_idx {
                        SelectionTypedValue::Column(idx) => row[idx].clone(),
                        _ => column_idx.try_into().unwrap(),
                    })
                    .collect();
                new_relation.insert_typed(row)
            }
        });

    return new_relation
}

pub fn evaluate(
    expr: &RelationalExpression,
    database: &Database,
    new_symbol: &str,
) -> Option<Relation> {
    if let Some(root_addr) = expr.root {
        let root_node = expr.arena[root_addr].clone();

        match root_node.value {
            Term::Relation(atom) => return database.get(&atom.symbol).cloned(),
            Term::Product => {
                let left_subtree = expr.branch_at(root_node.left_child.unwrap());
                let right_subtree = expr.branch_at(root_node.right_child.unwrap());

                let left_subtree_evaluation = evaluate(&left_subtree, database, new_symbol);

                if let Some(left_relation) = left_subtree_evaluation {
                    let right_subtree_evaluation = evaluate(&right_subtree, database, new_symbol);
                    if let Some(right_relation) = right_subtree_evaluation {
                        return Some(product(&left_relation, &right_relation));
                    }
                }

                return None;
            }
            Term::Join(left_column_idx, right_column_idx) => {
                let left_subtree = expr.branch_at(root_node.left_child.unwrap());
                let right_subtree = expr.branch_at(root_node.right_child.unwrap());

                let left_subtree_evaluation = evaluate(&left_subtree, database, new_symbol);
                if let Some(mut left_relation) = left_subtree_evaluation {
                    //left_relation.compact();
                    let right_subtree_evaluation = evaluate(&right_subtree, database, new_symbol);
                    if let Some(mut right_relation) = right_subtree_evaluation {
                        //right_relation.compact();
                        let join_result = hash_join(
                            &left_relation,
                            &right_relation,
                            left_column_idx,
                            right_column_idx,
                        );
                        //println!("Join duration: {}", now.elapsed().as_millis());
                        return Some(join_result);
                    }
                }

                return None;
            }
            unary_operators => {
                let left_subtree = expr.branch_at(root_node.left_child.unwrap());

                match unary_operators {
                    Term::Selection(column_index, selection_target) => {
                        return match selection_target {
                            SelectionTypedValue::Column(idx) => {
                                let evaluation = evaluate(&left_subtree, database, new_symbol);
                                if let Some(mut relation) = evaluation {
                                    select_equality(&mut relation, column_index, idx);
                                    Some(relation)
                                } else {
                                    None
                                }
                            }
                            _ => {
                                let evaluation = evaluate(&left_subtree, database, new_symbol);
                                if let Some(mut relation) = evaluation {
                                    select_value(&mut relation, column_index, selection_target);
                                    Some(relation)
                                } else {
                                    None
                                }
                            }
                        }
                    }
                    Term::Projection(column_idxs) => {
                        let evaluation = &evaluate(&left_subtree, database, new_symbol);
                        return if let Some(relation) = evaluation {
                            return Some(project(relation, &column_idxs, new_symbol));
                        } else {
                            return None;
                        };
                    }
                    _ => {}
                }
            }
        }
    }
    return None;
}

#[cfg(test)]
mod tests {
    use crate::implementations::relational_algebra::{
        join, product, select_equality, select_value,
    };
    use crate::models::datalog::{Rule, Ty, TypedValue};
    use crate::models::instance::Instance;
    use crate::models::relational_algebra::{
        Index, Relation, RelationalExpression,
        SelectionTypedValue,
    };

    #[test]
    fn select_value_test() {
        let mut relation = Relation::new(&"X", 2, false);
        let relation_data = vec![(true, 1), (true, 4), (false, 4)];
        relation_data.into_iter().for_each(|tuple| {
            relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
        });

        let expected_selection_data = vec![(true, 4), (false, 4)];
        let mut expected_selection = Relation::new(&"X", 2, false);
        expected_selection_data.into_iter().for_each(|tuple| {
            expected_selection.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
        });

        select_value(&mut relation, 1, SelectionTypedValue::UInt(4));
        relation.compact();
        assert_eq!(expected_selection, relation);
    }

    #[test]
    fn select_equality_test() {
        let mut relation = Relation::new(&"four", 3, false);
        let rel_data = vec![(true, 1, 3), (true, 4, 4), (false, 4, 4)];
        rel_data.into_iter().for_each(|tuple| {
            relation.insert(vec![
                Box::new(tuple.0),
                Box::new(tuple.1),
                Box::new(tuple.2),
            ]);
        });

        let expected_selection_data = vec![(true, 4, 4), (false, 4, 4)];
        let mut expected_selection = Relation::new(&"four", 3, false);
        expected_selection_data.into_iter().for_each(|tuple| {
            expected_selection.insert(vec![
                Box::new(tuple.0),
                Box::new(tuple.1),
                Box::new(tuple.2),
            ]);
        });

        select_equality(&mut relation, 1, 2);
        relation.compact();
        assert_eq!(expected_selection, relation);
    }

    use itertools::Itertools;

    #[test]
    fn product_test() {
        let mut left_relation = Relation::new(&"X", 2, false);
        let left_data = vec![
            (1001, "Arlis"),
            (1002, "Robert"),
            (1003, "Rego"),
            (1004, "Michael"),
            (1005, "Rucy"),
        ];
        left_data.clone().into_iter().for_each(|tuple| {
            left_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
        });

        let mut right_relation = Relation::new(&"Y", 2, false);
        let right_data = vec![
            (1001, "Bulbasaur"),
            (1002, "Charmander"),
            (1003, "Squirtle"),
        ];
        right_data
            .clone()
            .into_iter()
            .for_each(|tuple| right_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));

        let mut expected_product = Relation::new(&"XY", 4, false);

        left_data
            .into_iter()
            .cartesian_product(right_data.into_iter())
            .for_each(|tuple| {
                expected_product.insert(vec![
                    Box::new(tuple.0 .0),
                    Box::new(tuple.0 .1),
                    Box::new(tuple.1 .0),
                    Box::new(tuple.1 .1),
                ]);
            });

        let actual_product = product(&left_relation, &right_relation);
        assert_eq!(expected_product, actual_product);
    }

    #[test]
    fn join_test() {
        let mut left_relation = Relation::new(&"X", 2, true);
        let left_data = vec![
            (1001, "Arlis"),
            (1002, "Robert"),
            (1003, "Rego"),
            (1004, "Michael"),
            (1005, "Rucy"),
        ];
        left_data.clone().into_iter().for_each(|tuple| {
            left_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
        });

        let mut right_relation = Relation::new(&"Y", 2, true);
        let right_data = vec![
            (1001, "Bulbasaur"),
            (1002, "Charmander"),
            (1003, "Squirtle"),
        ];
        right_data
            .clone()
            .into_iter()
            .for_each(|tuple| right_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));


        let mut expected_join = Relation::new(&"XY", 4, true);
        let expected_join_data = vec![
            (1001, "Arlis", 1001, "Bulbasaur"),
            (1002, "Robert", 1002, "Charmander"),
            (1003, "Rego", 1003, "Squirtle"),
        ];
        expected_join_data.clone().into_iter().for_each(|tuple| {
            expected_join.insert(vec![
                Box::new(tuple.0),
                Box::new(tuple.1),
                Box::new(tuple.2),
                Box::new(tuple.3),
            ])
        });

        let actual_join = join(&left_relation, &right_relation, 0, 0);
        assert_eq!(expected_join, actual_join);
    }

    #[test]
    fn evaluate_test() {
        let rule =
            "mysticalAncestor(?x, ?z) <- [child(?x, ?y), child(?y, ?z), subClassOf(?y, demiGod)]";

        let expression = RelationalExpression::from(&Rule::from(rule));

        let mut instance = Instance::new(true);
        vec![
            ("adam", "jumala"),
            ("vanasarvik", "jumala"),
            ("eve", "adam"),
            ("jumala", "cthulu"),
        ]
        .into_iter()
        .for_each(|tuple| instance.insert("child", vec![Box::new(tuple.0), Box::new(tuple.1)]));

        vec![
            ("adam", "human"),
            ("vanasarvik", "demiGod"),
            ("eve", "human"),
            ("jumala", "demiGod"),
            ("cthulu", "demiGod"),
        ]
        .into_iter()
        .for_each(|tuple| {
            instance.insert("subClassOf", vec![Box::new(tuple.0), Box::new(tuple.1)])
        });

        let mut expected_relation = Relation::new(&"ancestor", 2, true);
        let expected_relation_data = vec![("adam", "cthulu"), ("vanasarvik", "cthulu")];
        expected_relation_data
            .clone()
            .into_iter()
            .for_each(|tuple| expected_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));

        let mut actual_relation = instance.evaluate(&expression, "ancestor").unwrap();
        actual_relation.compact();

        assert_eq!(expected_relation, actual_relation);
    }
}
