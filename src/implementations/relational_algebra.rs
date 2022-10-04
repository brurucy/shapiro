use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::time::Instant;

use crate::models::datalog::TypedValue;
use crate::models::instance::Database;
use crate::models::relational_algebra::{
    Column, Index, Relation, RelationalExpression, SelectionTypedValue, Term,
};

pub fn select_value(relation: &mut Relation, column_idx: usize, value: SelectionTypedValue) {
    relation.clone().into_iter().for_each(|row| {
        if row[column_idx] == value.clone().try_into().unwrap() {
            relation.mark_deleted(&row);
        }
    });
}

pub fn select_equality(relation: &mut Relation, left_column_idx: usize, right_column_idx: usize) {
    relation.clone().into_iter().for_each(|row| {
        if row[left_column_idx] == row[right_column_idx] {
            relation.mark_deleted(&row);
        }
    });
}

pub fn product(left_relation: &Relation, right_relation: &Relation) -> Relation {
    let columns: Vec<Column> = left_relation
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
                active: true,
            };
        })
        .collect();

    let ward = HashMap::new();

    let mut relation = Relation {
        columns,
        symbol: left_relation.symbol.to_string() + &right_relation.symbol,
        ward,
        indexes,
        ..Default::default()
    };

    left_relation.clone().into_iter().for_each(|left_row| {
        if let Some(left_sign) = left_relation.ward.get(&left_row) {
            if *left_sign {
                right_relation.clone().into_iter().for_each(|right_row| {
                    if let Some(right_sign) = right_relation.ward.get(&right_row) {
                        if *right_sign {
                            relation.insert_typed(
                                &left_row
                                    .clone()
                                    .into_iter()
                                    .chain(right_row.into_iter())
                                    .collect(),
                            )
                        }
                    }
                })
            }
        }
    });

    return relation;
}

// pub fn hash_join(
//     left_relation: &Relation,
//     right_relation: &Relation,
//     left_index: usize,
//     right_index: usize,
// ) -> Relation {
//     let columns: Vec<Column> = left_relation
//         .columns
//         .clone()
//         .into_iter()
//         .chain(right_relation.columns.clone().into_iter())
//         .map(|column| Column {
//             contents: vec![],
//             ty: column.ty,
//         })
//         .collect();
//     let ward = HashSet::new();

//     let indexes: Vec<Index> = left_relation
//         .indexes
//         .clone()
//         .into_iter()
//         .chain(right_relation.indexes.clone().into_iter())
//         .map(|_| Index {
//             index: BTreeSet::new(),
//             active: false,
//         })
//         .collect();

//     let mut result = Relation {
//         columns: columns.clone(),
//         symbol: left_relation.symbol.to_string() + &right_relation.symbol,
//         indexes: indexes.clone(),
//         ward,
//         ..Default::default()
//     };

//     let builder = left_relation
//         .clone()
//         .into_iter()
//         .fold(HashMap::new(), |mut acc, curr| {
//             if !acc.contains_key(&curr[left_index]) {
//                 acc.insert(curr[left_index].clone(), vec![curr]);
//             } else {
//                 let rows = acc.get_mut(&curr[left_index]).unwrap();
//                 rows.push(curr);
//             }
//             acc
//         });

//     right_relation.clone().into_iter().for_each(|right_row| {
//         if let Some(row_set) = builder.get(&right_row[right_index]) {
//             row_set.into_iter().for_each(|left_row| {
//                 result.insert_typed(
//                     &left_row
//                         .clone()
//                         .into_iter()
//                         .chain(right_row.clone().into_iter())
//                         .collect::<Vec<TypedValue>>(),
//                 )
//             })
//         }
//     });

//     todo!()
// }

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
            if *sign {
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
            if *sign {
                return Some((row, idx));
            }
            return None;
        });

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

    let ward = HashMap::new();

    let indexes: Vec<Index> = left_relation
        .indexes
        .clone()
        .into_iter()
        .chain(right_relation.indexes.clone().into_iter())
        .map(|_| Index {
            index: BTreeSet::new(),
            active: true,
        })
        .collect();

    let mut result = Relation {
        columns: columns.clone(),
        symbol: left_relation.symbol.to_string() + &right_relation.symbol,
        indexes: indexes.clone(),
        ward,
        ..Default::default()
    };

    let (mut current_left, mut current_right) = (left_iterator.next(), right_iterator.next());
    let mut cnt = 0;
    loop {
        //println!("loop {}", cnt);
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
                        println!("{} == {}", left_index_value.0, right_index_value.0);
                        let mut left_matches: Vec<(Vec<TypedValue>)> = vec![];
                        left_matches.push(left_zip.0);
                        let mut right_matches: Vec<(Vec<TypedValue>)> = vec![];
                        right_matches.push(right_zip.0);
                        let mut left_cnt = 0;
                        loop {
                            //println!("left loop: {}", left_cnt);
                            left_cnt += 1;
                            current_left = left_iterator.next();
                            if let Some(left) = current_left.as_ref() {
                                if left.1 .0.cmp(&left_index_value.0) == Ordering::Equal {
                                    //println!("found: {} == {}", left.1.0, left_index_value.0);
                                    left_matches.push(left.clone().0);
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        let mut right_cnt = 0;
                        loop {
                            //println!("right loop: {}", right_cnt);
                            right_cnt += 1;
                            current_right = right_iterator.next();
                            if let Some(right) = current_right.as_ref() {
                                if right.1 .0.cmp(&right_index_value.0) == Ordering::Equal {
                                    //println!("found: {} == {}", right.1.0, right_index_value.0);
                                    right_matches.push(right.clone().0);
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        let mut matches = 0;
                        println!("left * right: {}", left_cnt * right_cnt);
                        left_matches.iter().for_each(|left_value| {
                            right_matches.iter().for_each(|right_value| {
                                //println!("matches: {}", matches);
                                matches += 1;
                                let row = left_value
                                    .clone()
                                    .iter()
                                    .chain(right_value.into_iter())
                                    .cloned()
                                    .collect();
                                let now = Instant::now();
                                result.insert_typed(&row);
                                println!("time to insert a row: {}, size: {}", now.elapsed().as_micros(), result.columns[0].contents.len())
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
    println!("loop end");

    return result;
}

pub fn project(
    relation: &Relation,
    column_indexes: &Vec<SelectionTypedValue>,
    new_symbol: &str,
) -> Relation {
    let columns: Vec<Column> = column_indexes
        .clone()
        .into_iter()
        .map(|column_idx| match column_idx {
            SelectionTypedValue::Column(idx) => relation.columns[idx].clone(),
            _ => Column {
                ty: column_idx.clone().try_into().unwrap(),
                contents: vec![column_idx.try_into().unwrap(); relation.columns[0].contents.len()],
            },
        })
        .collect();

    let ward = relation
        .ward
        .clone()
        .into_iter()
        .map(|(row, sign)| {
            let row = column_indexes
                .clone()
                .into_iter()
                .map(|column_idx| match column_idx {
                    SelectionTypedValue::Column(idx) => row[idx].clone(),
                    _ => column_idx.try_into().unwrap(),
                })
                .collect();
            (row, sign)
        })
        .collect();

    let indexes: Vec<Index> = column_indexes
        .clone()
        .into_iter()
        .map(|_| Index {
            index: Default::default(),
            active: false,
        })
        .collect();

    return Relation {
        symbol: new_symbol.to_string(),
        columns,
        ward,
        indexes,
        ..Default::default()
    };
}

pub fn evaluate(
    expr: &RelationalExpression,
    database: &Database,
    new_symbol: &str,
) -> Option<Relation> {
    if let Some(root_addr) = expr.root {
        println!("expr: {}", expr.to_string());
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
                let now = Instant::now();
                println!("Join {} {}", left_column_idx, right_column_idx);
                let left_subtree = expr.branch_at(root_node.left_child.unwrap());
                let right_subtree = expr.branch_at(root_node.right_child.unwrap());

                let left_subtree_evaluation = evaluate(&left_subtree, database, new_symbol);

                if let Some(left_relation) = left_subtree_evaluation {
                    let right_subtree_evaluation = evaluate(&right_subtree, database, new_symbol);
                    if let Some(right_relation) = right_subtree_evaluation {
                        let join_result = join(
                            &left_relation,
                            &right_relation,
                            left_column_idx,
                            right_column_idx,
                        );
                        println!("Join duration: {}", now.elapsed().as_millis());
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
    use std::collections::BTreeSet;

    use crate::implementations::relational_algebra::{
        join, product, select_equality, select_value,
    };
    use crate::models::datalog::{Rule, Ty, TypedValue};
    use crate::models::instance::Instance;
    use crate::models::relational_algebra::{
        Column, ColumnType, Index, Relation, RelationSchema, RelationalExpression,
        SelectionTypedValue,
    };

    #[test]
    fn select_value_test() {
        let mut relation = Relation::new(&RelationSchema {
            column_types: vec![ColumnType::Bool, ColumnType::UInt],
            symbol: "four".to_string(),
        });
        let relation_data = vec![(true, 1), (true, 4), (false, 4)];
        relation_data.into_iter().for_each(|tuple| {
            relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
        });

        let expected_selection_data = vec![(true, 4), (false, 4)];
        let mut expected_selection = Relation::new(&RelationSchema {
            column_types: vec![ColumnType::Bool, ColumnType::UInt],
            symbol: relation.symbol.to_string(),
        });
        expected_selection_data.into_iter().for_each(|tuple| {
            expected_selection.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
        });

        select_value(&mut relation, 1, SelectionTypedValue::UInt(4));
        relation.compact();
        assert_eq!(expected_selection, relation);
    }

    #[test]
    fn select_equality_test() {
        let mut relation = Relation::new(&RelationSchema {
            column_types: vec![ColumnType::Bool, ColumnType::UInt, ColumnType::UInt],
            symbol: "four".to_string(),
        });
        let rel_data = vec![(true, 1, 3), (true, 4, 4), (false, 4, 4)];
        rel_data.into_iter().for_each(|tuple| {
            relation.insert(vec![
                Box::new(tuple.0),
                Box::new(tuple.1),
                Box::new(tuple.2),
            ]);
        });

        let expected_selection_data = vec![(true, 4, 4), (false, 4, 4)];
        let mut expected_selection = Relation::new(&RelationSchema {
            column_types: vec![ColumnType::Bool, ColumnType::UInt, ColumnType::UInt],
            symbol: relation.symbol.to_string(),
        });
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
        left_data.clone().into_iter().for_each(|tuple| {
            left_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
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
        right_data
            .clone()
            .into_iter()
            .for_each(|tuple| right_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));

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
        left_data
            .clone()
            .into_iter()
            .for_each(|tuple| left_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));
        left_relation.activate_index(0);

        let mut right_relation = Relation::new(&RelationSchema {
            column_types: vec![ColumnType::UInt, ColumnType::Str],
            symbol: "Y".to_string(),
        });
        let right_data = vec![
            (1001, "Bulbasaur"),
            (1002, "Charmander"),
            (1003, "Squirtle"),
        ];
        right_data
            .clone()
            .into_iter()
            .for_each(|tuple| right_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));
        right_relation.activate_index(0);

        let mut expected_join = Relation::new(&RelationSchema {
            column_types: vec![
                ColumnType::UInt,
                ColumnType::Str,
                ColumnType::UInt,
                ColumnType::Str,
            ],
            symbol: left_relation.symbol.to_string() + &right_relation.symbol.to_string(),
        });
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

        let mut instance = Instance::new();
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

        let mut expected_relation = Relation::new(&RelationSchema {
            column_types: vec![ColumnType::Str, ColumnType::Str],
            symbol: "ancestor".to_string(),
        });
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
