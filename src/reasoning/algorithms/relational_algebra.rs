use crate::models::index::IndexBacking;
use crate::models::instance::StorageWithIndex;
use crate::models::relational_algebra::{
    Container, Relation, RelationalExpression, SelectionTypedValue,
    SimpleRelationWithOneIndexBacking, Term,
};

impl<T: IndexBacking> Relation for SimpleRelationWithOneIndexBacking<T> {
    fn select_value(self, column_idx: usize, value: SelectionTypedValue) -> Self {
        let mut relation = SimpleRelationWithOneIndexBacking::new(self.symbol());
        let typed_value = value.try_into().unwrap();

        self.ward
            .into_iter()
            .filter(|row| row[column_idx] == typed_value)
            .for_each(|row| relation.insert_row(row));

        return relation;
    }

    fn select_equality(self, left_column_idx: usize, target_column_ix: usize) -> Self {
        let mut relation = SimpleRelationWithOneIndexBacking::new(self.symbol());

        self.ward
            .into_iter()
            .filter(|row| row[left_column_idx] == row[target_column_ix])
            .for_each(|row| relation.insert_row(row.clone()));

        return relation;
    }

    fn product(self, other: &Self) -> Self {
        let mut relation = SimpleRelationWithOneIndexBacking::new(self.symbol() + &other.symbol());

        self.ward.iter().for_each(|left_k| {
            other.ward.iter().for_each(|right_k| {
                relation.insert_row(left_k.iter().chain(right_k.iter()).cloned().collect())
            })
        });

        return relation;
    }

    fn join(self, other: &Self, _left_column_idx: usize, _right_column_idx: usize) -> Self {
        let mut relation = SimpleRelationWithOneIndexBacking::new(self.symbol() + &other.symbol());

        self.index.join(&other.index, |l, r| {
            if let Some(left_row) = self.ward.get_index(l) {
                if let Some(right_row) = other.ward.get_index(r) {
                    relation.insert_row(
                        left_row
                            .into_iter()
                            .chain(right_row.into_iter())
                            .cloned()
                            .collect(),
                    )
                }
            }
        });

        return relation;
    }

    fn project(
        self,
        new_column_indexes_and_values: Vec<SelectionTypedValue>,
        new_symbol: String,
    ) -> Self {
        let mut new_relation = SimpleRelationWithOneIndexBacking::new(new_symbol.to_string());

        self.ward.into_iter().for_each(|row| {
            let row = new_column_indexes_and_values
                .iter()
                .map(|column_idx| match column_idx {
                    SelectionTypedValue::Column(idx) => row[*idx].clone(),
                    _ => column_idx.clone().try_into().unwrap(),
                })
                .collect();

            new_relation.insert_row(row)
        });

        return new_relation;
    }

    fn symbol(&self) -> String {
        return self.symbol.clone();
    }
}

pub fn build_index<T: IndexBacking>(
    relation: &mut SimpleRelationWithOneIndexBacking<T>,
    column_idx: usize,
) {
    relation.ward.iter().for_each(|row| {
        relation.index.insert_row((
            row[column_idx].clone(),
            relation.ward.get_index_of(row).unwrap(),
        ));
    })
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
            Term::Relation(atom) => return database.get(&atom.symbol).cloned(),
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
                                build_index(&mut left_relation, left_column_idx);
                            },
                            || {
                                build_index(&mut right_relation, right_column_idx);
                            },
                        );

                        let join_result =
                            left_relation.join(&right_relation, left_column_idx, right_column_idx);

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
                                if let Some(relation) = evaluation {
                                    let filtered_relation =
                                        relation.select_equality(column_index, idx);

                                    Some(filtered_relation)
                                } else {
                                    None
                                }
                            }
                            _ => {
                                let evaluation = evaluate(&left_subtree, database, new_symbol);
                                if let Some(relation) = evaluation {
                                    let filtered_relation =
                                        relation.select_value(column_index, selection_target);

                                    Some(filtered_relation)
                                } else {
                                    None
                                }
                            }
                        };
                    }
                    Term::Projection(column_idxs) => {
                        let evaluation = evaluate(&left_subtree, database, new_symbol);
                        return if let Some(relation) = evaluation {
                            let projection = relation.project(column_idxs, new_symbol.to_string());

                            Some(projection)
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
    use crate::models::datalog::{SugaredRule, Ty};
    use crate::models::instance::{Database, SimpleDatabaseWithIndex};
    use crate::models::relational_algebra::{
        Container, Relation, RelationalExpression, SelectionTypedValue,
        SimpleRelationWithOneIndexBacking,
    };

    #[test]
    fn select_value_test() {
        let mut relation: SimpleRelationWithOneIndexBacking<BTreeIndex> =
            SimpleRelationWithOneIndexBacking::new("X".to_string());
        let relation_data = vec![(true, 1), (true, 4), (false, 4)];
        relation_data.into_iter().for_each(|tuple| {
            relation.insert_row(Box::new([
                tuple.0.to_typed_value(),
                tuple.1.to_typed_value(),
            ]));
        });

        let expected_selection_data = vec![(true, 4), (false, 4)];
        let mut expected_selection = SimpleRelationWithOneIndexBacking::new("X".to_string());
        expected_selection_data.into_iter().for_each(|tuple| {
            expected_selection.insert_row(Box::new([
                tuple.0.to_typed_value(),
                tuple.1.to_typed_value(),
            ]));
        });

        let actual_selection = relation.select_value(1, SelectionTypedValue::UInt(4));
        assert_eq!(expected_selection, actual_selection);
    }

    #[test]
    fn select_equality_test() {
        let mut relation: SimpleRelationWithOneIndexBacking<BTreeIndex> =
            SimpleRelationWithOneIndexBacking::new("four".to_string());
        let rel_data = vec![(true, 1, 3), (true, 4, 4), (false, 4, 4)];
        rel_data.into_iter().for_each(|tuple| {
            relation.insert_row(Box::new([
                tuple.0.to_typed_value(),
                tuple.1.to_typed_value(),
                tuple.2.to_typed_value(),
            ]));
        });

        let expected_selection_data = vec![(true, 4, 4), (false, 4, 4)];
        let mut expected_selection = SimpleRelationWithOneIndexBacking::new("four".to_string());
        expected_selection_data.into_iter().for_each(|tuple| {
            expected_selection.insert_row(Box::new([
                tuple.0.to_typed_value(),
                tuple.1.to_typed_value(),
                tuple.2.to_typed_value(),
            ]));
        });

        let actual_selection = relation.select_equality(1, 2);
        assert_eq!(expected_selection, actual_selection);
    }

    use crate::misc::string_interning::Interner;
    use crate::models::index::BTreeIndex;
    use crate::reasoning::algorithms::relational_algebra::build_index;
    use itertools::Itertools;

    #[test]
    fn product_test() {
        let mut left_relation: SimpleRelationWithOneIndexBacking<BTreeIndex> =
            SimpleRelationWithOneIndexBacking::new("X".to_string());
        let left_data = vec![
            (1001, "Arlis"),
            (1002, "Robert"),
            (1003, "Rego"),
            (1004, "Michael"),
            (1005, "Rucy"),
        ];
        left_data.clone().into_iter().for_each(|tuple| {
            left_relation.insert_row(Box::new([
                tuple.0.to_typed_value(),
                tuple.1.to_typed_value(),
            ]));
        });

        let mut right_relation = SimpleRelationWithOneIndexBacking::new("Y".to_string());
        let right_data = vec![
            (1001, "Bulbasaur"),
            (1002, "Charmander"),
            (1003, "Squirtle"),
        ];
        right_data.clone().into_iter().for_each(|tuple| {
            right_relation.insert_row(Box::new([
                tuple.0.to_typed_value(),
                tuple.1.to_typed_value(),
            ]))
        });

        let mut expected_product = SimpleRelationWithOneIndexBacking::new("XY".to_string());

        left_data
            .into_iter()
            .cartesian_product(right_data.into_iter())
            .for_each(|tuple| {
                expected_product.insert_row(Box::new([
                    tuple.0 .0.to_typed_value(),
                    tuple.0 .1.to_typed_value(),
                    tuple.1 .0.to_typed_value(),
                    tuple.1 .1.to_typed_value(),
                ]));
            });

        let actual_product = left_relation.product(&right_relation);
        assert_eq!(expected_product, actual_product);
    }

    #[test]
    fn join_test() {
        let mut left_relation: SimpleRelationWithOneIndexBacking<BTreeIndex> =
            SimpleRelationWithOneIndexBacking::new("X".to_string());
        let left_data = vec![
            (1001, "Arlis"),
            (1002, "Robert"),
            (1003, "Rego"),
            (1004, "Michael"),
            (1005, "Rucy"),
        ];
        left_data.into_iter().for_each(|tuple| {
            left_relation.insert_row(Box::new([
                tuple.0.to_typed_value(),
                tuple.1.to_typed_value(),
            ]));
        });

        let mut right_relation = SimpleRelationWithOneIndexBacking::new("Y".to_string());
        let right_data = vec![
            (1001, "Bulbasaur"),
            (1002, "Charmander"),
            (1003, "Squirtle"),
        ];
        right_data.into_iter().for_each(|tuple| {
            right_relation.insert_row(Box::new([
                tuple.0.to_typed_value(),
                tuple.1.to_typed_value(),
            ]));
        });

        let mut expected_join = SimpleRelationWithOneIndexBacking::new("XY".to_string());
        let expected_join_data = vec![
            (1001, "Arlis", 1001, "Bulbasaur"),
            (1002, "Robert", 1002, "Charmander"),
            (1003, "Rego", 1003, "Squirtle"),
        ];
        expected_join_data.clone().into_iter().for_each(|tuple| {
            expected_join.insert_row(Box::new([
                tuple.0.to_typed_value(),
                tuple.1.to_typed_value(),
                tuple.2.to_typed_value(),
                tuple.3.to_typed_value(),
            ]))
        });

        build_index(&mut left_relation, 0);
        build_index(&mut right_relation, 0);

        let actual_join = left_relation.join(&right_relation, 0, 0);
        assert_eq!(expected_join, actual_join);
    }

    #[test]
    fn evaluate_test() {
        let rule =
            "mysticalAncestor(?x, ?z) <- [child(?x, ?y), child(?y, ?z), subClassOf(?y, demiGod)]";

        let expression = RelationalExpression::from(&SugaredRule::from(rule));

        let mut interner = Interner::default();
        let child_id = interner.rodeo.get_or_intern("child");
        let sub_class_of_id = interner.rodeo.get_or_intern("subClassOf");

        let mut instance: SimpleDatabaseWithIndex<BTreeIndex> =
            SimpleDatabaseWithIndex::new(interner);
        vec![
            ("adam", "jumala"),
            ("vanasarvik", "jumala"),
            ("eve", "adam"),
            ("jumala", "cthulu"),
        ]
        .into_iter()
        .for_each(|tuple| {
            instance.insert_at(
                child_id.into_inner().get(),
                Box::new([tuple.0.to_typed_value(), tuple.1.to_typed_value()]),
            )
        });

        vec![
            ("adam", "human"),
            ("vanasarvik", "demiGod"),
            ("eve", "human"),
            ("jumala", "demiGod"),
            ("cthulu", "demiGod"),
        ]
        .into_iter()
        .for_each(|tuple| {
            instance.insert_at(
                sub_class_of_id.into_inner().get(),
                Box::new([tuple.0.to_typed_value(), tuple.1.to_typed_value()]),
            )
        });

        let mut expected_relation = SimpleRelationWithOneIndexBacking::new("ancestor".to_string());
        let expected_relation_data = vec![("adam", "cthulu"), ("vanasarvik", "cthulu")];
        expected_relation_data
            .clone()
            .into_iter()
            .for_each(|tuple| {
                expected_relation.insert_row(Box::new([
                    tuple.0.to_typed_value(),
                    tuple.1.to_typed_value(),
                ]))
            });

        let actual_relation = instance.evaluate(&expression, "ancestor").unwrap();

        assert_eq!(expected_relation, actual_relation);
    }
}
