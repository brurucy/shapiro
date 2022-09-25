use std::collections::HashSet;

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

    let filtered_rows: Vec<(usize, Vec<TypedValue>)> = relation
        .clone()
        .into_iter()
        .enumerate()
        .filter(|(_row_id, row)| match row[column_idx].clone() {
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

    filtered_rows
        .clone()
        .into_iter()
        .for_each(|(_row_id, row)| {
            row.into_iter()
                .enumerate()
                .for_each(|(idx, column_value)| columns[idx].contents.push(column_value))
        });

    let filtered_row_set: HashSet<usize> = filtered_rows
        .into_iter()
        .map(|(row_id, _row)| row_id)
        .collect();

    let indexes: Vec<Index> = relation
        .clone()
        .indexes
        .into_iter()
        .map(|idx| {
            return Index {
                index: idx
                    .index
                    .into_iter()
                    .filter(|(_value, row_id)| {
                        return filtered_row_set.contains(row_id);
                    })
                    .collect(),
                active: idx.active,
            };
        })
        .collect();

    return Relation {
        symbol,
        columns,
        indexes,
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

    let filtered_rows: Vec<(usize, Vec<TypedValue>)> = relation
        .clone()
        .into_iter()
        .enumerate()
        .filter(|(_row_id, row)| row[left_column_idx] == row[right_column_idx])
        .collect();

    filtered_rows
        .clone()
        .into_iter()
        .for_each(|(_row_id, row)| {
            row.into_iter()
                .enumerate()
                .for_each(|(idx, column_value)| columns[idx].contents.push(column_value))
        });

    let filtered_row_set: HashSet<usize> = filtered_rows
        .into_iter()
        .map(|(row_id, _row)| row_id)
        .collect();

    let indexes: Vec<Index> = relation
        .clone()
        .indexes
        .into_iter()
        .map(|idx| {
            return Index {
                index: idx
                    .index
                    .into_iter()
                    .filter(|(_value, row_id)| {
                        return filtered_row_set.contains(row_id);
                    })
                    .collect(),
                active: idx.active,
            };
        })
        .collect();

    return Relation {
        symbol: symbol,
        columns: columns,
        indexes: indexes,
    };
}

// Indexes need to be merged
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
        row.into_iter()
            .enumerate()
            .for_each(|(column_idx, column_value)| columns[column_idx].contents.push(column_value))
    });

    let indexes: Vec<Index> = left_relation
        .indexes
        .clone()
        .into_iter()
        .chain(right_relation.indexes.clone().into_iter())
        .enumerate()
        .map(|(column_idx, idx)| {
            let mut new_index = idx.index.clone();
            if idx.active {
                new_index.clear();
                product
                    .clone()
                    .into_iter()
                    .enumerate()
                    .for_each(|(row_id, row)| {
                        new_index.insert((row[column_idx].clone(), row_id));
                    })
            }
            return Index {
                index: new_index,
                active: idx.active,
            };
        })
        .collect();

    return Relation {
        columns,
        symbol: left_relation.symbol.to_string() + &right_relation.symbol,
        indexes,
    };
}

pub fn project(relation: &Relation, column_indexes: &Vec<usize>, new_symbol: &str) -> Relation {
    let columns: Vec<Column> = column_indexes
        .clone()
        .into_iter()
        .map(|column_idx| relation.columns[column_idx].clone())
        .collect();

    let indexes: Vec<Index> = column_indexes
        .clone()
        .into_iter()
        .map(|column_idx| relation.indexes[column_idx].clone())
        .collect();

    return Relation {
        symbol: new_symbol.to_string(),
        columns,
        indexes,
    };
}

pub fn evaluate(expr: &Expression, database: Database, new_symbol: &str) -> Relation {
    let output: Relation = Relation {
        columns: vec![],
        symbol: new_symbol.to_string(),
        indexes: vec![],
    };

    if let Some(root_addr) = expr.root {
        let root_node = expr.arena[root_addr].clone();

        match root_node.value {
            Term::Relation(atom) => return database.get(&atom.symbol).unwrap().clone(),
            Term::Product => {
                let mut left_subtree = expr.clone();
                left_subtree.set_root(root_node.left_child.unwrap());
                let mut right_subtree = expr.clone();
                right_subtree.set_root(root_node.right_child.unwrap());

                return product(
                    &evaluate(&left_subtree, database.clone(), new_symbol),
                    &evaluate(&right_subtree, database, new_symbol),
                );
            }
            unary_operators => {
                let mut left_subtree = expr.clone();
                left_subtree.set_root(root_node.left_child.unwrap());

                match unary_operators {
                    Term::Selection(column_index, selection_target) => match selection_target {
                        SelectionTypedValue::Column(idx) => {
                            return select_equality(
                                &evaluate(&left_subtree, database, new_symbol),
                                column_index,
                                idx,
                            )
                        }
                        _ => {
                            return select_value(
                                &evaluate(&left_subtree, database, new_symbol),
                                column_index,
                                selection_target,
                            )
                        }
                    },
                    Term::Projection(column_idxs) => {
                        return project(
                            &evaluate(&left_subtree, database, new_symbol),
                            &column_idxs,
                            new_symbol,
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    return output;
}

mod test {
    use std::collections::{BTreeSet, HashMap};

    use crate::implementations::relational_algebra::{
        evaluate, product, select_equality, select_value,
    };
    use crate::models::datalog::TypedValue;
    use crate::models::instance::Instance;
    use crate::models::relational_algebra::{
        Column, ColumnType, Expression, Index, Relation, SelectionTypedValue,
    };
    use crate::parsers::datalog::parse_rule;

    #[test]
    fn select_value_test() {
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
            ],
            symbol: "four".to_string(),
            indexes: vec![],
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
            ],
            symbol: "four".to_string(),
            indexes: vec![],
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
            indexes: vec![],
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
            indexes: vec![],
        };

        let actual_select_application = select_equality(&rel, 1, 2);
        assert_eq!(expected_select_application, actual_select_application);
    }

    #[test]
    fn product_test() {
        let left_relation = Relation {
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
                        TypedValue::Str("Mihkel".to_string()),
                        TypedValue::Str("Glenn".to_string()),
                    ],
                },
            ],
            symbol: "X".to_string(),
            indexes: vec![],
        };

        let right_relation = Relation {
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
            indexes: vec![],
        };

        let expected_product = Relation {
            columns: vec![
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1003),
                        TypedValue::UInt(1003),
                        TypedValue::UInt(1003),
                        TypedValue::UInt(1004),
                        TypedValue::UInt(1004),
                        TypedValue::UInt(1004),
                        TypedValue::UInt(1005),
                        TypedValue::UInt(1005),
                        TypedValue::UInt(1005),
                    ],
                },
                Column {
                    ty: ColumnType::Str,
                    contents: vec![
                        TypedValue::Str("Arlis".to_string()),
                        TypedValue::Str("Arlis".to_string()),
                        TypedValue::Str("Arlis".to_string()),
                        TypedValue::Str("Robert".to_string()),
                        TypedValue::Str("Robert".to_string()),
                        TypedValue::Str("Robert".to_string()),
                        TypedValue::Str("Rego".to_string()),
                        TypedValue::Str("Rego".to_string()),
                        TypedValue::Str("Rego".to_string()),
                        TypedValue::Str("Mihkel".to_string()),
                        TypedValue::Str("Mihkel".to_string()),
                        TypedValue::Str("Mihkel".to_string()),
                        TypedValue::Str("Glenn".to_string()),
                        TypedValue::Str("Glenn".to_string()),
                        TypedValue::Str("Glenn".to_string()),
                    ],
                },
                Column {
                    ty: ColumnType::UInt,
                    contents: vec![
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1003),
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1003),
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1003),
                        TypedValue::UInt(1001),
                        TypedValue::UInt(1002),
                        TypedValue::UInt(1003),
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
                        TypedValue::Str("Bulbasaur".to_string()),
                        TypedValue::Str("Charmander".to_string()),
                        TypedValue::Str("Squirtle".to_string()),
                        TypedValue::Str("Bulbasaur".to_string()),
                        TypedValue::Str("Charmander".to_string()),
                        TypedValue::Str("Squirtle".to_string()),
                        TypedValue::Str("Bulbasaur".to_string()),
                        TypedValue::Str("Charmander".to_string()),
                        TypedValue::Str("Squirtle".to_string()),
                        TypedValue::Str("Bulbasaur".to_string()),
                        TypedValue::Str("Charmander".to_string()),
                        TypedValue::Str("Squirtle".to_string()),
                    ],
                },
            ],
            symbol: "XY".to_string(),
            indexes: vec![],
        };
        let actual_product = product(&left_relation, &right_relation);
        assert_eq!(expected_product, actual_product);
    }

    #[test]
    fn evaluate_test() {
        let rule =
            "mysticalAncestor(?x, ?z) <- [child(?x, ?y), child(?y, ?z), subClassOf(?y, demiGod)]";

        let expression = Expression::from(&parse_rule(rule));

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
            indexes: vec![
                Index {
                    index: BTreeSet::new(),
                    active: false,
                },
                Index {
                    index: BTreeSet::new(),
                    active: false,
                },
            ],
        };
        let actual_relation = evaluate(&expression, instance.database, "ancestor");

        assert_eq!(expected_relation, actual_relation);
    }
}
