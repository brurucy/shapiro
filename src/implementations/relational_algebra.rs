use crate::models::datalog::TypedValue;
use crate::models::relational_algebra::{
    Column, Database, ExpressionArena, Relation, SelectionTypedValue, Term,
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

    relation
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
        .for_each(|row| {
            row.into_iter()
                .enumerate()
                .for_each(|(idx, column_value)| columns[idx].contents.push(column_value))
        });

    return Relation {
        symbol: symbol,
        columns: columns,
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

    relation
        .clone()
        .into_iter()
        .filter(|row| row[left_column_idx] == row[right_column_idx])
        .for_each(|row| {
            row.into_iter()
                .enumerate()
                .for_each(|(idx, column_value)| columns[idx].contents.push(column_value))
        });

    return Relation {
        symbol: symbol,
        columns: columns,
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

    left_relation
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
        .for_each(|row| {
            row.into_iter()
                .enumerate()
                .for_each(|(column_idx, column_value)| {
                    columns[column_idx].contents.push(column_value)
                })
        });

    return Relation {
        columns,
        symbol: left_relation.symbol.to_string() + &right_relation.symbol,
    };
}

pub fn project(relation: &Relation, indexes: &Vec<usize>, new_symbol: &str) -> Relation {
    let columns: Vec<Column> = indexes
        .clone()
        .into_iter()
        .map(|column_idx| relation.columns[column_idx].clone())
        .collect();

    return Relation {
        symbol: new_symbol.to_string(),
        columns,
    };
}

pub fn evaluate(expr: &ExpressionArena, database: Database, new_symbol: &str) -> Relation {
    let output: Relation = Relation {
        columns: vec![],
        symbol: new_symbol.to_string(),
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
    use std::collections::HashMap;

    use crate::implementations::relational_algebra::{
        evaluate, product, select_equality, select_value,
    };
    use crate::models::datalog::TypedValue;
    use crate::models::relational_algebra::{
        Column, ColumnType, ExpressionArena, Relation, SelectionTypedValue,
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
        };
        let actual_product = product(&left_relation, &right_relation);
        assert_eq!(expected_product, actual_product);
    }

    #[test]
    fn evaluate_test() {
        let rule =
            "mysticalAncestor(?x, ?z) <- [child(?x, ?y), child(?y, ?z), subClassOf(?y, demiGod)]";

        let expression = ExpressionArena::from(&parse_rule(rule));

        let mut database = HashMap::new();
        database.insert(
            "child".to_string(),
            Relation {
                columns: vec![
                    Column {
                        ty: ColumnType::Str,
                        contents: vec![
                            TypedValue::Str("adam".to_string()),
                            TypedValue::Str("vanasarvik".to_string()),
                            TypedValue::Str("eve".to_string()),
                            TypedValue::Str("jumala".to_string()),
                        ],
                    },
                    Column {
                        ty: ColumnType::Str,
                        contents: vec![
                            TypedValue::Str("jumala".to_string()),
                            TypedValue::Str("jumala".to_string()),
                            TypedValue::Str("adam".to_string()),
                            TypedValue::Str("cthulu".to_string()),
                        ],
                    },
                ],
                symbol: "child".to_string(),
            },
        );
        database.insert(
            "subClassOf".to_string(),
            Relation {
                columns: vec![
                    Column {
                        ty: ColumnType::Str,
                        contents: vec![
                            TypedValue::Str("adam".to_string()),
                            TypedValue::Str("vanasarvik".to_string()),
                            TypedValue::Str("eve".to_string()),
                            TypedValue::Str("jumala".to_string()),
                            TypedValue::Str("cthulu".to_string()),
                        ],
                    },
                    Column {
                        ty: ColumnType::Str,
                        contents: vec![
                            TypedValue::Str("human".to_string()),
                            TypedValue::Str("demiGod".to_string()),
                            TypedValue::Str("human".to_string()),
                            TypedValue::Str("demiGod".to_string()),
                            TypedValue::Str("demiGod".to_string()),
                        ],
                    },
                ],
                symbol: "subClassOf".to_string(),
            },
        );

        let expected_relation = Relation {
            symbol: "ancestor".to_string(),
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
        };
        let actual_relation = evaluate(&expression, database, "ancestor");

        assert_eq!(expected_relation, actual_relation);
    }
}
