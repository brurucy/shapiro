use crate::models::index::IndexBacking;
use crate::models::instance::StorageWithIndex;
use crate::models::relational_algebra::{SimpleRelationWithOneIndexBacking, RelationalExpression, SelectionTypedValue, Term, Relation, Container};

impl<T : IndexBacking> Relation for SimpleRelationWithOneIndexBacking<T> {
    fn select_value(&mut self, column_idx: usize, value: &SelectionTypedValue) {
        self.ward.clone().into_iter().for_each(|(k, _v)| {
            if k[column_idx] != value.clone().try_into().unwrap() {
                self.mark_deleted(&k);
            }
        });
    }

    fn select_equality(&mut self, left_column_idx: usize, right_column_idx: usize) {
        self.ward.clone().into_iter().for_each(|(k, _v)| {
            if k[left_column_idx] != k[right_column_idx] {
                self.mark_deleted(&k);
            }
        });
    }

    fn product(&self, other: &Self) -> Self {
        let mut relation = SimpleRelationWithOneIndexBacking::new(
            self.symbol() + &other.symbol(),
        );

        self
            .ward
            .clone()
            .into_iter()
            .for_each(|(left_k, left_v)| {
                if left_v {
                    other
                        .ward
                        .clone()
                        .into_iter()
                        .for_each(|(right_k, right_v)| {
                            if right_v {
                                relation.insert_row(
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

    fn join(&self, other: &Self, left_column_idx: usize, right_column_idx: usize) -> Self {
        let mut relation = SimpleRelationWithOneIndexBacking::new(
            self.symbol() + &other.symbol(),
        );

        self.index.join(
            &other.index,
            |l, r| {
                if let Some(left_row) = self.ward.get_index(l) {
                    if *left_row.1 {
                        if let Some(right_row) = other.ward.get_index(r) {
                            if *right_row.1 {
                                relation.insert_row(
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

    fn project(&self, new_column_indexes_and_values: Vec<SelectionTypedValue>, new_symbol: String) -> Self {
        let mut new_relation = SimpleRelationWithOneIndexBacking::new(new_symbol.to_string());

        self.ward.clone().into_iter().for_each(|(row, sign)| {
            if sign {
                let row = new_column_indexes_and_values
                    .clone()
                    .into_iter()
                    .map(|column_idx| match column_idx {
                        SelectionTypedValue::Column(idx) => row[idx].clone(),
                        _ => column_idx.try_into().unwrap(),
                    })
                    .collect();
                new_relation.insert_row(row)
            }
        });

        return new_relation;
    }

    fn symbol(&self) -> String {
        return self.symbol.clone()
    }
}

// TODO make this generic over the database
pub fn evaluate<T: IndexBacking>(
    expr: &RelationalExpression,
    database: &StorageWithIndex<T>,
    new_symbol: &str,
) -> Option<SimpleRelationWithOneIndexBacking<T>>
where
    T: IndexBacking,
{
    if let Some(root_addr) = expr.root {
        let root_node = expr.arena[root_addr].clone();

        match root_node.value {
            Term::Relation(atom) => return database.get(atom.symbol).cloned(),
            Term::Product => {
                let left_subtree = expr.branch_at(root_node.left_child.unwrap());
                let right_subtree = expr.branch_at(root_node.right_child.unwrap());

                let left_subtree_evaluation = evaluate(&left_subtree, database, new_symbol);

                if let Some(left_relation) = left_subtree_evaluation {
                    let right_subtree_evaluation = evaluate(&right_subtree, database, new_symbol);
                    if let Some(right_relation) = right_subtree_evaluation {
                        return Some(left_relation.product(&right_relation));
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
                                left_relation
                                    .ward
                                    .iter()
                                    .enumerate()
                                    .for_each(|(idx, (row, active))| {
                                        left_relation.index.insert_row((row[left_column_idx].clone(), idx));
                                    });
                            },
                            || {
                                right_relation
                                    .ward
                                    .iter()
                                    .enumerate()
                                    .for_each(|(idx, (row, active))| {
                                        right_relation.index.insert_row((row[left_column_idx].clone(), idx));
                                    });

                            },
                        );

                        let join_result = left_relation.join(
                            &right_relation,
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
                                    relation.select_equality(column_index, idx);
                                    Some(relation)
                                } else {
                                    None
                                }
                            }
                            _ => {
                                let evaluation = evaluate(&left_subtree, database, new_symbol);
                                if let Some(mut relation) = evaluation {
                                    relation.select_value(column_index, &selection_target);
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
                            Some(relation.project(column_idxs, new_symbol.to_string()))
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

// #[cfg(test)]
// mod tests {
//     use crate::models::datalog::SugaredRule;
//     use crate::models::instance::SimpleDatabaseWithIndex;
//     use crate::models::relational_algebra::{SimpleRelationWithOneIndexBacking, RelationalExpression, SelectionTypedValue, Container, Relation};
//
//     #[test]
//     fn select_value_test() {
//         let mut relation: SimpleRelationWithOneIndexBacking<BTreeIndex> = SimpleRelationWithOneIndexBacking::new("X".to_string(), 2);
//         let relation_data = vec![(true, 1), (true, 4), (false, 4)];
//         relation_data.into_iter().for_each(|tuple| {
//             relation.insert_row(vec![Box::new(tuple.0), Box::new(tuple.1)]);
//         });
//
//         let expected_selection_data = vec![(true, 4), (false, 4)];
//         let mut expected_selection = SimpleRelationWithOneIndexBacking::new("X".to_string(), 2);
//         expected_selection_data.into_iter().for_each(|tuple| {
//             expected_selection.insert_row(vec![Box::new(tuple.0), Box::new(tuple.1)]);
//         });
//
//         relation.select_value(1, &SelectionTypedValue::UInt(4));
//         relation.compact();
//         assert_eq!(expected_selection, relation);
//     }
//
//     #[test]
//     fn select_equality_test() {
//         let mut relation: SimpleRelationWithOneIndexBacking<BTreeIndex> = SimpleRelationWithOneIndexBacking::new("four".to_string(), 3);
//         let rel_data = vec![(true, 1, 3), (true, 4, 4), (false, 4, 4)];
//         rel_data.into_iter().for_each(|tuple| {
//             relation.insert(vec![
//                 Box::new(tuple.0),
//                 Box::new(tuple.1),
//                 Box::new(tuple.2),
//             ]);
//         });
//
//         let expected_selection_data = vec![(true, 4, 4), (false, 4, 4)];
//         let mut expected_selection = SimpleRelationWithOneIndexBacking::new("four".to_string(), 3);
//         expected_selection_data.into_iter().for_each(|tuple| {
//             expected_selection.insert(vec![
//                 Box::new(tuple.0),
//                 Box::new(tuple.1),
//                 Box::new(tuple.2),
//             ]);
//         });
//
//         relation.select_equality( 1, 2);
//         relation.compact();
//         assert_eq!(expected_selection, relation);
//     }
//
//     use crate::models::index::BTreeIndex;
//     use itertools::Itertools;
//
//     #[test]
//     fn product_test() {
//         let mut left_relation: SimpleRelationWithOneIndexBacking<BTreeIndex> = SimpleRelationWithOneIndexBacking::new("X".to_string(), 2);
//         let left_data = vec![
//             (1001, "Arlis"),
//             (1002, "Robert"),
//             (1003, "Rego"),
//             (1004, "Michael"),
//             (1005, "Rucy"),
//         ];
//         left_data.clone().into_iter().for_each(|tuple| {
//             left_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
//         });
//
//         let mut right_relation = SimpleRelationWithOneIndexBacking::new("Y".to_string(), 2);
//         let right_data = vec![
//             (1001, "Bulbasaur"),
//             (1002, "Charmander"),
//             (1003, "Squirtle"),
//         ];
//         right_data
//             .clone()
//             .into_iter()
//             .for_each(|tuple| right_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));
//
//         let mut expected_product = SimpleRelationWithOneIndexBacking::new("XY".to_string(), 4);
//
//         left_data
//             .into_iter()
//             .cartesian_product(right_data.into_iter())
//             .for_each(|tuple| {
//                 expected_product.insert(vec![
//                     Box::new(tuple.0 .0),
//                     Box::new(tuple.0 .1),
//                     Box::new(tuple.1 .0),
//                     Box::new(tuple.1 .1),
//                 ]);
//             });
//
//         let actual_product = left_relation.product(&right_relation);
//         assert_eq!(expected_product, actual_product);
//     }
//
//     #[test]
//     fn join_test() {
//         let mut left_relation: SimpleRelationWithOneIndexBacking<BTreeIndex> = SimpleRelationWithOneIndexBacking::new("X".to_string(), 2);
//         let left_data = vec![
//             (1001, "Arlis"),
//             (1002, "Robert"),
//             (1003, "Rego"),
//             (1004, "Michael"),
//             (1005, "Rucy"),
//         ];
//         left_data.clone().into_iter().for_each(|tuple| {
//             left_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]);
//         });
//         left_relation.compact_physical(0);
//
//         let mut right_relation = SimpleRelationWithOneIndexBacking::new("Y".to_string(), 2);
//         let right_data = vec![
//             (1001, "Bulbasaur"),
//             (1002, "Charmander"),
//             (1003, "Squirtle"),
//         ];
//         right_data
//             .clone()
//             .into_iter()
//             .for_each(|tuple| right_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));
//         right_relation.compact_physical(0);
//
//         let mut expected_join = SimpleRelationWithOneIndexBacking::new("XY".to_string(), 4);
//         let expected_join_data = vec![
//             (1001, "Arlis", 1001, "Bulbasaur"),
//             (1002, "Robert", 1002, "Charmander"),
//             (1003, "Rego", 1003, "Squirtle"),
//         ];
//         expected_join_data.clone().into_iter().for_each(|tuple| {
//             expected_join.insert(vec![
//                 Box::new(tuple.0),
//                 Box::new(tuple.1),
//                 Box::new(tuple.2),
//                 Box::new(tuple.3),
//             ])
//         });
//
//         let actual_join = left_relation.join(&right_relation, 0, 0);
//         assert_eq!(expected_join, actual_join);
//     }
//
//     #[test]
//     fn evaluate_test() {
//         let rule =
//             "mysticalAncestor(?x, ?z) <- [child(?x, ?y), child(?y, ?z), subClassOf(?y, demiGod)]";
//
//         let expression = RelationalExpression::from(&SugaredRule::from(rule));
//
//         let mut instance: SimpleDatabaseWithIndex<BTreeIndex> = SimpleDatabaseWithIndex::new();
//         vec![
//             ("adam", "jumala"),
//             ("vanasarvik", "jumala"),
//             ("eve", "adam"),
//             ("jumala", "cthulu"),
//         ]
//         .into_iter()
//         .for_each(|tuple| instance.insert("child", vec![Box::new(tuple.0), Box::new(tuple.1)]));
//
//         vec![
//             ("adam", "human"),
//             ("vanasarvik", "demiGod"),
//             ("eve", "human"),
//             ("jumala", "demiGod"),
//             ("cthulu", "demiGod"),
//         ]
//         .into_iter()
//         .for_each(|tuple| {
//             instance.insert("subClassOf", vec![Box::new(tuple.0), Box::new(tuple.1)])
//         });
//
//         let mut expected_relation = SimpleRelationWithOneIndexBacking::new("ancestor".to_string(), 2);
//         let expected_relation_data = vec![("adam", "cthulu"), ("vanasarvik", "cthulu")];
//         expected_relation_data
//             .clone()
//             .into_iter()
//             .for_each(|tuple| expected_relation.insert(vec![Box::new(tuple.0), Box::new(tuple.1)]));
//
//         let mut actual_relation = instance.evaluate(&expression, "ancestor").unwrap();
//         actual_relation.compact();
//
//         assert_eq!(expected_relation, actual_relation);
//     }
// }
