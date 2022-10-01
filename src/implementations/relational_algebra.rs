use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};

use crate::models::datalog::TypedValue;
use crate::models::instance::Database;
use crate::models::relational_algebra::{
    Column, Expression, Index, Relation, SelectionTypedValue, Term,
};

pub fn select_value(
    relation: &Relation,
    column_idx: usize,
    value: SelectionTypedValue,
) -> Relation {
    let symbol = relation.symbol.clone();
    let mut columns: Vec<Column> = relation
        .columns
        .clone()
        .into_iter()
        .map(|column| Column {
            ty: column.ty,
            contents: vec![],
        })
        .collect();
    let mut ward = HashSet::new();

    let filtered_rows: Vec<Vec<TypedValue>> = relation
        .clone()
        .into_iter()
        .filter(|row| match row[column_idx].clone() {
            TypedValue::Str(outer) => match value.clone() {
                SelectionTypedValue::Str(inner) => outer == inner,
                _ => false,
            },
            TypedValue::Bool(outer) => match value {
                SelectionTypedValue::Bool(inner) => outer == inner,
                _ => false,
            },
            TypedValue::UInt(outer) => match value {
                SelectionTypedValue::UInt(inner) => outer == inner,
                _ => false,
            },
            TypedValue::Float(outer) => match value {
                SelectionTypedValue::Float(inner) => outer == inner,
                _ => false,
            },
        })
        .collect();

    filtered_rows.clone().into_iter().for_each(|row| {
        ward.insert(row.clone());
        row.into_iter()
            .enumerate()
            .for_each(|(idx, column_value)| columns[idx].contents.push(column_value))
    });

    return Relation {
        symbol,
        columns,
        ward,
        ..Default::default()
    };
}

pub fn select_equality(
    relation: &Relation,
    left_column_idx: usize,
    right_column_idx: usize,
) -> Relation {
    let symbol = relation.symbol.clone();
    let mut columns: Vec<Column> = relation
        .columns
        .clone()
        .into_iter()
        .map(|column| Column {
            ty: column.ty,
            contents: vec![],
        })
        .collect();
    let mut ward = HashSet::new();

    let filtered_rows: Vec<Vec<TypedValue>> = relation
        .clone()
        .into_iter()
        .filter(|row| row[left_column_idx] == row[right_column_idx])
        .collect();

    filtered_rows.clone().into_iter().for_each(|row| {
        ward.insert(row.clone());
        row.into_iter()
            .enumerate()
            .for_each(|(idx, column_value)| columns[idx].contents.push(column_value))
    });

    return Relation {
        symbol,
        columns,
        ward,
        ..Default::default()
    };
}

pub fn product(left_relation: &Relation, right_relation: &Relation) -> Relation {
    let mut columns: Vec<Column> = left_relation
        .columns
        .clone()
        .into_iter()
        .chain(right_relation.columns.clone().into_iter())
        .map(|column| Column {
            ty: column.ty,
            contents: vec![],
        })
        .collect();
    let indexes = (&left_relation.columns)
        .into_iter()
        .chain((&right_relation.columns).into_iter())
        .map(|_ty| {
            return Index {
                index: BTreeSet::new(),
                active: false,
            };
        })
        .collect();

    let mut ward = HashSet::new();

    let product: Vec<Vec<TypedValue>> = left_relation
        .clone()
        .into_iter()
        .flat_map(|left_row| {
            right_relation
                .clone()
                .into_iter()
                .map(|right_row| {
                    left_row
                        .clone()
                        .into_iter()
                        .chain(right_row.into_iter())
                        .collect::<Vec<TypedValue>>()
                })
                .collect::<Vec<Vec<TypedValue>>>()
        })
        .collect();

    product.clone().into_iter().for_each(|row| {
        ward.insert(row.clone());
        row.into_iter()
            .enumerate()
            .for_each(|(column_idx, column_value)| columns[column_idx].contents.push(column_value))
    });

    return Relation {
        columns,
        symbol: left_relation.symbol.to_string() + &right_relation.symbol,
        ward,
        indexes,
    };
}

pub fn hash_join(
    left_relation: &Relation,
    right_relation: &Relation,
    left_index: usize,
    right_index: usize,
) -> Relation {
    todo!()
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
        .map(|idx| (left_relation.get_row(idx.1), idx));

    let mut right_iterator = right_relation.indexes[right_index]
        .index
        .clone()
        .into_iter()
        .map(|idx| (right_relation.get_row(idx.1), idx));

    let columns: Vec<Column> = left_relation
        .columns
        .clone()
        .into_iter()
        .chain(right_relation.columns.clone().into_iter())
        .map(|column| Column {
            contents: vec![],
            ty: column.ty,
        })
        .collect();
    let ward = HashSet::new();

    let indexes: Vec<Index> = left_relation
        .indexes
        .clone()
        .into_iter()
        .chain(right_relation.indexes.clone().into_iter())
        .map(|_| Index {
            index: BTreeSet::new(),
            active: false,
        })
        .collect();

    let mut result = Relation {
        columns: columns.clone(),
        symbol: left_relation.symbol.to_string() + &right_relation.symbol,
        indexes: indexes.clone(),
        ward,
    };

    let (mut current_left, mut current_right) = (left_iterator.next(), right_iterator.next());
    loop {
        if let Some(left_zip) = current_left.clone() {
            if let Some(right_zip) = current_right.clone() {
                let left_index_value = left_zip.1;
                let right_index_value = right_zip.1;

                match left_index_value.0.cmp(&right_index_value.0) {
                    Ordering::Less => {
                        current_left = left_iterator.next();
                    }
                    Ordering::Equal => {
                        let mut left_matches: Vec<(Vec<TypedValue>)> = vec![];
                        left_matches.push(left_zip.0);
                        let mut right_matches: Vec<(Vec<TypedValue>)> = vec![];
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
                                if right.1 .0.cmp(&left_index_value.0) == Ordering::Equal {
                                    right_matches.push(right.clone().0);
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        left_matches.into_iter().for_each(|left_value| {
                            right_matches.clone().into_iter().for_each(|right_value| {
                                let row = left_value
                                    .clone()
                                    .into_iter()
                                    .chain(right_value.into_iter())
                                    .collect();
                                result.insert(&row)
                            })
                        });
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

    return result;
}

pub fn project(relation: &Relation, column_indexes: &Vec<usize>, new_symbol: &str) -> Relation {
    let columns: Vec<Column> = column_indexes
        .clone()
        .into_iter()
        .map(|column_idx| relation.columns[column_idx].clone())
        .collect();
    let ward = relation
        .ward
        .clone()
        .into_iter()
        .map(|row| {
            column_indexes
                .clone()
                .into_iter()
                .map(|column_idx| row[column_idx].clone())
                .collect()
        })
        .collect();

    return Relation {
        symbol: new_symbol.to_string(),
        columns,
        ward,
        ..Default::default()
    };
}

pub fn evaluate(expr: &Expression, database: Database, new_symbol: &str) -> Relation {
    let output = Relation {
        symbol: new_symbol.to_string(),
        ..Default::default()
    };

    if let Some(root_addr) = expr.root {
        let root_node = expr.arena[root_addr].clone();

        match root_node.value {
            Term::Relation(atom) => return database.get(&atom.symbol).unwrap().clone(),
            Term::Product => {
                let left_subtree = expr.branch_at(root_node.left_child.unwrap());
                let right_subtree = expr.branch_at(root_node.right_child.unwrap());

                return product(
                    &evaluate(&left_subtree, database.clone(), new_symbol),
                    &evaluate(&right_subtree, database, new_symbol),
                );
            }
            Term::Join(left_column_idx, right_column_idx) => {
                let left_subtree = expr.branch_at(root_node.left_child.unwrap());
                let right_subtree = expr.branch_at(root_node.right_child.unwrap());

                let mut left_subtree_evaluation =
                    evaluate(&left_subtree, database.clone(), new_symbol);
                left_subtree_evaluation.activate_index(left_column_idx);

                let mut right_subtree_evaluation =
                    evaluate(&right_subtree, database.clone(), new_symbol);
                right_subtree_evaluation.activate_index(right_column_idx);

                let join_result = join(
                    &left_subtree_evaluation,
                    &right_subtree_evaluation,
                    left_column_idx,
                    right_column_idx,
                );

                return join_result;
            }
            unary_operators => {
                let left_subtree = expr.branch_at(root_node.left_child.unwrap());

                match unary_operators {
                    Term::Selection(column_index, selection_target) => match selection_target {
                        SelectionTypedValue::Column(idx) => {
                            let evaluation = &evaluate(&left_subtree, database, new_symbol);
                            return select_equality(evaluation, column_index, idx);
                        }
                        _ => {
                            let evaluation = &evaluate(&left_subtree, database, new_symbol);
                            return select_value(evaluation, column_index, selection_target);
                        }
                    },
                    Term::Projection(column_idxs) => {
                        let evaluation = &evaluate(&left_subtree, database, new_symbol);
                        return project(evaluation, &column_idxs, new_symbol);
                    }
                    _ => {}
                }
            }
        }
    }

    return output;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::implementations::relational_algebra::{
        join, product, select_equality, select_value,
    };
    use crate::models::datalog::{Rule, TypedValue};
    use crate::models::instance::Instance;
    use crate::models::relational_algebra::{
        Column, ColumnType, Expression, Index, Relation, RelationSchema, SelectionTypedValue,
    };

    #[test]
    fn select_value_test() {
        let mut rel = Relation::default();
        rel.symbol = "four".to_string();
        let rel_data = vec![(true, 1), (true, 4), (false, 4)];
        rel_data.into_iter().for_each(|tuple| {
            rel.insert(&vec![tuple.0, TypedValue::UInt(tuple.1)]);
        });

        let expected_select_application = Relation {
            columns: vec![
                Column {
                    ty: ColumnType::Bool,
                    contents: vec![TypedValue::Bool(true), TypedValue::Bool(false)],
                },
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![TypedValue::UInt(4), TypedValue::UInt(4)],
                },
            ],
            symbol: "four".to_string(),
            ..Default::default()
        };

        let actual_select_application = select_value(&rel, 1, SelectionTypedValue::UInt(4));
        assert_eq!(expected_select_application, actual_select_application);
    }

    #[test]
    fn select_equality_test() {
        let rel = Relation {
            columns: vec![
                Column {
                    ty: ColumnType::Bool,
                    contents: vec![
                        TypedValue::Bool(true),
                        TypedValue::Bool(true),
                        TypedValue::Bool(false),
                    ],
                },
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![
                        TypedValue::UInt(1),
                        TypedValue::UInt(4),
                        TypedValue::UInt(4),
                    ],
                },
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![
                        TypedValue::UInt(3),
                        TypedValue::UInt(4),
                        TypedValue::UInt(4),
                    ],
                },
            ],
            symbol: "four".to_string(),
            ..Default::default()
        };

        let expected_select_application = Relation {
            columns: vec![
                Column {
                    ty: ColumnType::Bool,
                    contents: vec![TypedValue::Bool(true), TypedValue::Bool(false)],
                },
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![TypedValue::UInt(4), TypedValue::UInt(4)],
                },
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![TypedValue::UInt(4), TypedValue::UInt(4)],
                },
            ],
            symbol: "four".to_string(),
            ..Default::default()
        };

        let actual_select_application = select_equality(&rel, 1, 2);
        assert_eq!(expected_select_application, actual_select_application);
    }

    use itertools::Itertools;

    #[test]
    fn product_test() {
        let mut left_relation = Relation::new(&RelationSchema {
            column_types: vec![ColumnType::UInt, ColumnType::Str],
            symbol: "X".to_string(),
        });
        let left_data = vec![
            (1001, "Arlis"),
            (1002, "Robert"),
            (1003, "Rego"),
            (1004, "Michael"),
            (1005, "Rucy"),
        ];
        left_data.clone().into_iter().for_each(|(id, name)| {
            left_relation.insert(&vec![
                TypedValue::UInt(id),
                TypedValue::Str(name.to_string()),
            ])
        });

        let mut right_relation = Relation::new(&RelationSchema {
            column_types: vec![ColumnType::UInt, ColumnType::Str],
            symbol: "Y".to_string(),
        });
        let right_data = vec![
            (1001, "Bulbasaur"),
            (1002, "Charmander"),
            (1003, "Squirtle"),
        ];
        right_data.clone().into_iter().for_each(|(id, name)| {
            right_relation.insert(&vec![
                TypedValue::UInt(id),
                TypedValue::Str(name.to_string()),
            ])
        });

        let mut expected_product = Relation::new(&RelationSchema {
            column_types: vec![
                ColumnType::UInt,
                ColumnType::Str,
                ColumnType::UInt,
                ColumnType::Str,
            ],
            symbol: "XY".to_string(),
        });

        left_data
            .into_iter()
            .cartesian_product(right_data.into_iter())
            .for_each(|((left_id, left_name), (right_id, right_name))| {
                expected_product.insert(&vec![
                    TypedValue::UInt(left_id),
                    TypedValue::Str(left_name.to_string()),
                    TypedValue::UInt(right_id),
                    TypedValue::Str(right_name.to_string()),
                ]);
            });

        let actual_product = product(&left_relation, &right_relation);
        assert_eq!(expected_product, actual_product);
    }

    #[test]
    fn join_test() {
        let mut left_relation = Relation {
            columns: vec![
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1003),
                        TypedValue::UInt(1004),
                        TypedValue::UInt(1005),
                    ],
                },
                Column {
                    ty: ColumnType::Str,
                    contents: vec![
                        TypedValue::Str("Arlis".to_string()),
                        TypedValue::Str("Robert".to_string()),
                        TypedValue::Str("Rego".to_string()),
                        TypedValue::Str("Michael".to_string()),
                        TypedValue::Str("Rucy".to_string()),
                    ],
                },
            ],
            symbol: "X".to_string(),
            ..Default::default()
        };
        left_relation.activate_index(0);

        let mut right_relation = Relation {
            columns: vec![
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1003),
                    ],
                },
                Column {
                    ty: ColumnType::Str,
                    contents: vec![
                        TypedValue::Str("Bulbasaur".to_string()),
                        TypedValue::Str("Charmander".to_string()),
                        TypedValue::Str("Squirtle".to_string()),
                    ],
                },
            ],
            symbol: "Y".to_string(),
            ..Default::default()
        };
        right_relation.activate_index(0);

        let expected_product = Relation {
            columns: vec![
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1003),
                    ],
                },
                Column {
                    ty: ColumnType::Str,
                    contents: vec![
                        TypedValue::Str("Arlis".to_string()),
                        TypedValue::Str("Robert".to_string()),
                        TypedValue::Str("Rego".to_string()),
                    ],
                },
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1003),
                    ],
                },
                Column {
                    ty: ColumnType::Str,
                    contents: vec![
                        TypedValue::Str("Bulbasaur".to_string()),
                        TypedValue::Str("Charmander".to_string()),
                        TypedValue::Str("Squirtle".to_string()),
                    ],
                },
            ],
            symbol: "XY".to_string(),
            indexes: vec![
                Index {
                    index: BTreeSet::new(),
                    active: false,
                },
                Index {
                    index: BTreeSet::new(),
                    active: false,
                },
                Index {
                    index: BTreeSet::new(),
                    active: false,
                },
                Index {
                    index: BTreeSet::new(),
                    active: false,
                },
            ],
            ..Default::default()
        };
        let actual_product = join(&left_relation, &right_relation, 0, 0);
        assert_eq!(expected_product, actual_product);
    }

    #[test]
    fn evaluate_test() {
        let rule =
            "mysticalAncestor(?x, ?z) <- [child(?x, ?y), child(?y, ?z), subClassOf(?y, demiGod)]";

        let expression = Expression::from(&Rule::from(rule));

        let mut instance = Instance::new();
        vec![
            vec![
                TypedValue::Str("adam".to_string()),
                TypedValue::Str("jumala".to_string()),
            ],
            vec![
                TypedValue::Str("vanasarvik".to_string()),
                TypedValue::Str("jumala".to_string()),
            ],
            vec![
                TypedValue::Str("eve".to_string()),
                TypedValue::Str("adam".to_string()),
            ],
            vec![
                TypedValue::Str("jumala".to_string()),
                TypedValue::Str("cthulu".to_string()),
            ],
        ]
        .into_iter()
        .for_each(|row| instance.insert("child", row));

        vec![
            vec![
                TypedValue::Str("adam".to_string()),
                TypedValue::Str("human".to_string()),
            ],
            vec![
                TypedValue::Str("vanasarvik".to_string()),
                TypedValue::Str("demiGod".to_string()),
            ],
            vec![
                TypedValue::Str("eve".to_string()),
                TypedValue::Str("human".to_string()),
            ],
            vec![
                TypedValue::Str("jumala".to_string()),
                TypedValue::Str("demiGod".to_string()),
            ],
            vec![
                TypedValue::Str("cthulu".to_string()),
                TypedValue::Str("demiGod".to_string()),
            ],
        ]
        .into_iter()
        .for_each(|row| instance.insert("subClassOf", row));

        let expected_relation = Relation {
            columns: vec![
                Column {
                    ty: ColumnType::Str,
                    contents: vec![
                        TypedValue::Str("adam".to_string()),
                        TypedValue::Str("vanasarvik".to_string()),
                    ],
                },
                Column {
                    ty: ColumnType::Str,
                    contents: vec![
                        TypedValue::Str("cthulu".to_string()),
                        TypedValue::Str("cthulu".to_string()),
                    ],
                },
            ],
            symbol: "ancestor".to_string(),
            ..Default::default()
        };
        let actual_relation = instance.evaluate(&expression, "ancestor");

        assert_eq!(expected_relation, actual_relation);
    }
}
