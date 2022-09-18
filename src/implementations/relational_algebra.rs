use crate::models::datalog::TypedValue;
use crate::models::relational_algebra::{Column, Relation, SelectionTypedValue};

pub fn select_value(column: &Column, value: SelectionTypedValue) -> Column {
    return Column {
        ty: column.ty,
        contents: column
            .contents
            .clone()
            .into_iter()
            .filter(|item| match item {
                TypedValue::Str(outer) => match &value {
                    SelectionTypedValue::Str(inner) => outer == inner,
                    _ => false,
                },
                TypedValue::Bool(outer) => match &value {
                    SelectionTypedValue::Bool(inner) => outer == inner,
                    _ => false,
                },
                TypedValue::UInt(outer) => match &value {
                    SelectionTypedValue::UInt(inner) => outer == inner,
                    _ => false,
                },
                TypedValue::Float(outer) => match &value {
                    SelectionTypedValue::Float(inner) => outer == inner,
                    _ => false,
                },
            })
            .collect(),
    };
}

pub fn select_equality(left_column: &Column, right_column: &Column) -> Column {
    return Column {
        ty: left_column.ty,
        contents: left_column
            .contents
            .clone()
            .into_iter()
            .zip(right_column.contents.clone().into_iter())
            .filter(|(left_item, right_item)| left_item == right_item)
            .map(|(left_item, right_item)| left_item)
            .collect(),
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

pub fn project(relation: &Relation, indexes: &Vec<usize>) -> Relation {
    todo!()
}

pub fn evaluate() -> Relation {
    todo!()
}

mod test {
    use crate::implementations::relational_algebra::{product, select_equality, select_value};
    use crate::models::datalog::TypedValue;
    use crate::models::relational_algebra::{Column, ColumnType, Relation, SelectionTypedValue};

    #[test]
    fn select_value_test() {
        let col = Column {
            ty: ColumnType::Bool,
            contents: vec![
                TypedValue::Bool(true),
                TypedValue::Bool(true),
                TypedValue::Bool(false),
            ],
        };

        let expected_select_application = Column {
            ty: ColumnType::Bool,
            contents: vec![TypedValue::Bool(false)],
        };
        let actual_select_application = select_value(&col, SelectionTypedValue::Bool(false));
        assert_eq!(expected_select_application, actual_select_application);
    }

    #[test]
    fn select_equality_test() {
        let left_column = Column {
            ty: ColumnType::Bool,
            contents: vec![
                TypedValue::Bool(true),
                TypedValue::Bool(true),
                TypedValue::Bool(false),
            ],
        };
        let right_column = Column {
            ty: ColumnType::Bool,
            contents: vec![
                TypedValue::Bool(false),
                TypedValue::Bool(true),
                TypedValue::Bool(false),
            ],
        };

        let expected_select_application = Column {
            ty: ColumnType::Bool,
            contents: vec![TypedValue::Bool(true), TypedValue::Bool(false)],
        };
        let actual_select_application = select_equality(&left_column, &right_column);
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
            symbol: "".to_string(),
        };
        let actual_product = product(&left_relation, &right_relation);
        assert_eq!(expected_product, actual_product);
    }
}
