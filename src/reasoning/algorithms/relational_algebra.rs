use crate::models::index::IndexBacking;
use crate::models::instance::StorageWithIndex;
use crate::models::relational_algebra::{
    RelationWithOneIndexBacking, RelationalExpression, SelectionTypedValue, Term,
};

pub fn select_value<T: IndexBacking>(
    relation: &mut RelationWithOneIndexBacking<T>,
    column_idx: usize,
    value: SelectionTypedValue,
) {
    relation.ward.clone().into_iter().for_each(|(k, _v)| {
        if k[column_idx] != value.clone().try_into().unwrap() {
            relation.mark_deleted(&k);
        }
    });
}

pub fn select_equality<T: IndexBacking>(
    relation: &mut RelationWithOneIndexBacking<T>,
    left_column_idx: usize,
    right_column_idx: usize,
) {
    relation.ward.clone().into_iter().for_each(|(k, _v)| {
        if k[left_column_idx] != k[right_column_idx] {
            relation.mark_deleted(&k);
        }
    });
}

pub fn product<T: IndexBacking>(
    left_relation: &RelationWithOneIndexBacking<T>,
    right_relation: &RelationWithOneIndexBacking<T>,
) -> RelationWithOneIndexBacking<T>
where
    T: IndexBacking,
{
    let mut relation = RelationWithOneIndexBacking::new(
        &(left_relation.relation_id.to_string() + &right_relation.relation_id),
        left_relation.indexes.len() + right_relation.indexes.len(),
    );

    left_relation
        .ward
        .clone()
        .into_iter()
        .for_each(|(left_k, left_v)| {
            if left_v {
                right_relation
                    .ward
                    .clone()
                    .into_iter()
                    .for_each(|(right_k, right_v)| {
                        if right_v {
                            relation.insert_typed(
                                left_k
                                    .clone()
                                    .iter()
                                    .chain(right_k.iter())
                                    .cloned()
                                    .collect(),
                            )
                        }
                    })
            }
        });

    return relation;
}

pub fn join<T: IndexBacking>(
    left_relation: RelationWithOneIndexBacking<T>,
    right_relation: RelationWithOneIndexBacking<T>,
    left_index: usize,
    right_index: usize,
) -> RelationWithOneIndexBacking<T> {
    let mut relation = RelationWithOneIndexBacking::new(
        &(left_relation.relation_id.to_string() + &right_relation.relation_id),
        left_relation.indexes.len() + right_relation.indexes.len(),
    );

    left_relation.indexes[left_index].index.join(
        &right_relation.indexes[right_index].index,
        |l, r| {
            if let Some(left_row) = left_relation.ward.get_index(l) {
                if *left_row.1 {
                    if let Some(right_row) = right_relation.ward.get_index(r) {
                        if *right_row.1 {
                            relation.insert_typed(
                                left_row
                                    .0
                                    .into_iter()
                                    .chain(right_row.0.into_iter())
                                    .cloned()
                                    .collect(),
                            )
                        }
                    }
                }
            }
        },
    );

    return relation;
}

pub fn project<T: IndexBacking>(
    relation: &RelationWithOneIndexBacking<T>,
    column_indexes: &Vec<SelectionTypedValue>,
    new_symbol: &str,
) -> RelationWithOneIndexBacking<T> {
    let mut new_relation = RelationWithOneIndexBacking::new(new_symbol.to_string(), column_indexes.len());

    relation.ward.clone().into_iter().for_each(|(row, sign)| {
        if sign {
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

    return new_relation;
}

pub fn evaluate<T: IndexBacking>(
    expr: &RelationalExpression,
    database: &StorageWithIndex<T>,
    new_symbol: &str,
) -> Option<RelationWithOneIndexBacking<T>>
where
    T: IndexBacking,
{
    if let Some(root_addr) = expr.root {
        let root_node = expr.arena[root_addr].clone();

        match root_node.value {
            Term::Relation(atom) => return database.get(&atom.relation_id).cloned(),
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
                    let right_subtree_evaluation = evaluate(&right_subtree, database, new_symbol);
                    if let Some(mut right_relation) = right_subtree_evaluation {
                        rayon::join(
                            || {
                                left_relation.compact_physical(left_column_idx);
                            },
                            || right_relation.compact_physical(right_column_idx),
                        );
                        let join_result = join(
                            left_relation,
                            right_relation,
                            left_column_idx,
                            right_column_idx,
                        );
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
                            Some(project(relation, &column_idxs, new_symbol))
                        } else {
                            None
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
    use crate::models::datalog::SugaredRule;
    use crate::models::instance::SimpleDatabaseWithIndex;
    use crate::models::relational_algebra::{RelationWithOneIndexBacking, RelationalExpression, SelectionTypedValue, Container};

    #[test]
    fn select_value_test() {
        let mut relation: RelationWithOneIndexBacking<BTreeIndex> = RelationWithOneIndexBacking::new(&"X", 2, false);
        let relation_data = vec![(true, 1), (true, 4), (false, 4)];
        relation_data.into_iter().for_each(|tuple| {
            relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
        });

        let expected_selection_data = vec![(true, 4), (false, 4)];
        let mut expected_selection = RelationWithOneIndexBacking::new(&"X", 2, false);
        expected_selection_data.into_iter().for_each(|tuple| {
            expected_selection.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
        });

        select_value(&mut relation, 1, SelectionTypedValue::UInt(4));
        relation.compact();
        assert_eq!(expected_selection, relation);
    }

    #[test]
    fn select_equality_test() {
        let mut relation: RelationWithOneIndexBacking<BTreeIndex> = RelationWithOneIndexBacking::new(&"four", 3, false);
        let rel_data = vec![(true, 1, 3), (true, 4, 4), (false, 4, 4)];
        rel_data.into_iter().for_each(|tuple| {
            relation.insert(vec![
                Box::new(tuple.0),
                Box::new(tuple.1),
                Box::new(tuple.2),
            ]);
        });

        let expected_selection_data = vec![(true, 4, 4), (false, 4, 4)];
        let mut expected_selection = RelationWithOneIndexBacking::new(&"four", 3, false);
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

    use crate::models::index::BTreeIndex;
    use crate::reasoning::algorithms::relational_algebra::{
        join, product, select_equality, select_value,
    };
    use itertools::Itertools;

    #[test]
    fn product_test() {
        let mut left_relation: RelationWithOneIndexBacking<BTreeIndex> = RelationWithOneIndexBacking::new(&"X", 2, false);
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

        let mut right_relation = RelationWithOneIndexBacking::new(&"Y", 2, false);
        let right_data = vec![
            (1001, "Bulbasaur"),
            (1002, "Charmander"),
            (1003, "Squirtle"),
        ];
        right_data
            .clone()
            .into_iter()
            .for_each(|tuple| right_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));

        let mut expected_product = RelationWithOneIndexBacking::new(&"XY", 4, false);

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
        let mut left_relation: RelationWithOneIndexBacking<BTreeIndex> = RelationWithOneIndexBacking::new(&"X", 2, true);
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
        left_relation.compact_physical(0);

        let mut right_relation = RelationWithOneIndexBacking::new(&"Y", 2, true);
        let right_data = vec![
            (1001, "Bulbasaur"),
            (1002, "Charmander"),
            (1003, "Squirtle"),
        ];
        right_data
            .clone()
            .into_iter()
            .for_each(|tuple| right_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));
        right_relation.compact_physical(0);

        let mut expected_join = RelationWithOneIndexBacking::new(&"XY", 4, false);
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

        let actual_join = join(left_relation, right_relation, 0, 0);
        assert_eq!(expected_join, actual_join);
    }

    #[test]
    fn evaluate_test() {
        let rule =
            "mysticalAncestor(?x, ?z) <- [child(?x, ?y), child(?y, ?z), subClassOf(?y, demiGod)]";

        let expression = RelationalExpression::from(&SugaredRule::from(rule));

        let mut instance: SimpleDatabaseWithIndex<BTreeIndex> = SimpleDatabaseWithIndex::new(false);
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

        let mut expected_relation = RelationWithOneIndexBacking::new(&"ancestor", 2, false);
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
