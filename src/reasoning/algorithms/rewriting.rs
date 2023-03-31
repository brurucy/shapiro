use crate::data_structures::substitutions::Substitutions;
use crate::misc::helpers::terms_to_row;
use crate::misc::joins::nested_loop_join;
use crate::models::datalog::{Atom, Rule, Term};
use crate::models::instance::{HashSetDatabase, IndexedHashSetBacking};
use ahash::HashMap;
use itertools::Itertools;
use rayon::prelude::*;
use std::num::NonZeroU32;

pub fn unify(left: &Atom, right: &Atom) -> Option<Substitutions> {
    let mut substitution: Substitutions = Default::default();

    let left_and_right = left.terms.iter().zip(right.terms.iter());

    for (left_term, right_term) in left_and_right {
        match (left_term, right_term) {
            (Term::Constant(left_constant), Term::Constant(right_constant)) => {
                if left_constant != right_constant {
                    return None;
                }
            }
            (Term::Variable(left_variable), Term::Constant(right_constant)) => {
                if let Some(constant) = substitution.get(*left_variable) {
                    if constant != right_constant {
                        return None;
                    }
                } else {
                    substitution.insert((*left_variable, right_constant.clone()));
                }
            }
            _ => {}
        }
    }

    return Some(substitution);
}

pub fn attempt_to_rewrite(rewrite: &Substitutions, atom: &Atom) -> Atom {
    return Atom {
        terms: atom
            .terms
            .iter()
            .map(|term| {
                if let Term::Variable(identifier) = term {
                    if let Some(constant) = rewrite.get(*identifier) {
                        return Term::Constant(constant.clone());
                    }
                }
                return term.clone();
            })
            .collect(),
        relation_id: atom.relation_id,
        positive: atom.positive,
    };
}

pub fn is_ground(atom: &Atom) -> bool {
    for term in atom.terms.iter() {
        match term {
            Term::Variable(_) => {
                return false;
            }
            _ => {}
        };
    }
    return true;
}

// pub fn proven(knowledge_base: &Vec<(u32, &IndexedHashSetBacking)>, subs: &Substitutions, current_goal_position: usize, goals: &Vec<Atom>, head: &Atom) -> bool {
//     if current_goal_position == goals.len() {
//         let fresh_atom = attempt_to_rewrite(&subs, &head);
//
//         return is_ground(&fresh_atom)
//     }
//
//     let mut current_goals_x_subs: Vec<(u32, (usize, Atom, Substitutions))> = vec![];
//
//     nested_loop_join(&[(current_goal_position, &goals[current_goal_position])], &[(current_goal_position, subs)], |current_local_atom_id, left_value, subs| {
//         let rewrite_attempt = attempt_to_rewrite(subs, left_value);
//         let current_goal_x_sub = (left_value.relation_id.get(), (*current_local_atom_id, rewrite_attempt, subs.clone()));
//         current_goals_x_subs.push(current_goal_x_sub);
//     });
//
//     let mut subs = Substitutions::default();
//     for (left_relation_id, relation) in knowledge_base {
//         for (right_relation_id, (current_local_atom_id, rewrite_attempt, previous_subs)) in &current_goals_x_subs {
//             if *left_relation_id == *right_relation_id {
//                 for ground_fact in *relation {
//                     let ground_terms = ground_fact
//                         .iter()
//                         .map(|typed_value| {
//                             return Term::Constant(typed_value.clone())
//                         })
//                         .collect();
//
//                     let proposed_atom = Atom { terms: ground_terms, relation_id: NonZeroU32::try_from(*left_relation_id).unwrap(), positive: true };
//
//                     if let Some(new_subs) = unify(&rewrite_attempt, &proposed_atom) {
//                         subs.clone_from(previous_subs);
//                         subs.extend(&new_subs);
//
//                         if proven(knowledge_base, subs.clone(), current_local_atom_id + 1, goals, head) {
//                             return true;
//                         }
//                     }
//                 }
//             }
//         }
//     }
//
//     false
// }

pub fn evaluate_rule(
    knowledge_base: &HashSetDatabase,
    rule: &Rule,
    index: bool,
) -> Option<IndexedHashSetBacking> {
    let mut out: IndexedHashSetBacking = Default::default();

    let head = rule.head.clone();

    let goals: Vec<(usize, &Atom)> = rule.body.iter().enumerate().collect();

    let mut subs_product = vec![(0usize, Substitutions::default())];
    if index {
        for current_atom_id in 0..goals.len() {
            let mut current_goals_x_subs: Vec<(u32, (usize, Atom, Substitutions, Vec<usize>))> =
                vec![];
            subs_product = subs_product
                .into_iter()
                .filter(|(round, _)| *round == current_atom_id)
                .collect();

            nested_loop_join(
                &vec![goals[current_atom_id]],
                &subs_product,
                |current_local_atom_id, left_value, subs| {
                    let rewrite_attempt = attempt_to_rewrite(subs, left_value);
                    let terms_that_are_constant = rewrite_attempt
                        .terms
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, term)| {
                            if let Term::Constant(_) = term {
                                return Some(idx);
                            }

                            return None;
                        })
                        .collect();
                    let current_goal_x_sub = (
                        left_value.relation_id.get(),
                        (
                            *current_local_atom_id,
                            rewrite_attempt,
                            subs.clone(),
                            terms_that_are_constant,
                        ),
                    );
                    current_goals_x_subs.push(current_goal_x_sub.clone());
                },
            );

            let mut cgs_by_rid_ttac: Vec<_> = current_goals_x_subs
                .iter()
                .map(
                    |(
                        relation_id,
                        (current_local_atom_id, rewrite_attempt, subs, terms_that_are_constant),
                    )| {
                        return (
                            (relation_id, terms_that_are_constant),
                            (current_local_atom_id, rewrite_attempt, subs),
                        );
                    },
                )
                .collect();
            cgs_by_rid_ttac.sort();

            let unique_column_combinations: Vec<_> = cgs_by_rid_ttac
                .iter()
                .map(|(key, _)| key)
                .unique()
                .collect();

            let index: HashMap<_, _> = unique_column_combinations
                .into_iter()
                .filter_map(|(relation_id, column_combination)| {
                    if let Some(storage) = knowledge_base.storage.get(relation_id) {
                        let mut hm: HashMap<_, _> = Default::default();
                        for (projected_row, row) in storage.iter().map(|row| {
                            let mut projected_row = vec![];
                            column_combination.iter().for_each(|column_position| {
                                projected_row.push(row[*column_position].clone());
                            });

                            (projected_row, row)
                        }) {
                            hm.entry(projected_row).or_insert(vec![]).push(row);
                        }

                        return Some(((*relation_id, column_combination.clone()), hm));
                    }

                    return None;
                })
                .collect();

            cgs_by_rid_ttac.iter().for_each(
                |(key, (current_local_atom_id, rewrite_attempt, previous_subs))| {
                    if let Some(odd_index) = index.get(&key) {
                        let projected_rewrite_attempt: Vec<_> = rewrite_attempt
                            .terms
                            .iter()
                            .filter_map(|term| {
                                if let Term::Constant(inner) = term {
                                    return Some(inner.clone());
                                }
                                return None;
                            })
                            .collect();

                        if let Some(target_row_set) = odd_index.get(&projected_rewrite_attempt) {
                            target_row_set.iter().for_each(|ground_fact| {
                                let mut local_proposed_atom: Atom = Default::default();
                                local_proposed_atom.terms.extend(ground_fact.iter().map(
                                    |typed_value| {
                                        return Term::Constant(typed_value.clone());
                                    },
                                ));
                                local_proposed_atom.relation_id =
                                    NonZeroU32::try_from(*key.0).unwrap();
                                local_proposed_atom.positive = true;

                                if let Some(new_subs) = unify(rewrite_attempt, &local_proposed_atom)
                                {
                                    let mut extended_subs = (*previous_subs).clone();
                                    extended_subs.extend(new_subs);

                                    subs_product.push((**current_local_atom_id + 1, extended_subs));
                                };
                            });
                        };
                    }
                },
            );
        }
    } else {
        let borrowed_knowledge_base: Vec<_> = knowledge_base
            .storage
            .iter()
            .map(|(relation_id, row_set)| (*relation_id, row_set))
            .collect();

        for current_atom_id in 0..goals.len() {
            let mut current_goals_x_subs: Vec<(u32, (usize, Atom, Substitutions))> = vec![];
            subs_product = subs_product
                .into_iter()
                .filter(|(round, _)| *round == current_atom_id)
                .collect();

            nested_loop_join(
                &vec![goals[current_atom_id]],
                &subs_product,
                |current_local_atom_id, left_value, subs| {
                    let rewrite_attempt = attempt_to_rewrite(subs, left_value);
                    let current_goal_x_sub = (
                        left_value.relation_id.get(),
                        (*current_local_atom_id, rewrite_attempt, subs.clone()),
                    );
                    current_goals_x_subs.push(current_goal_x_sub.clone());
                },
            );

            nested_loop_join(
                &borrowed_knowledge_base,
                &current_goals_x_subs,
                |key, relation, (current_local_atom_id, rewrite_attempt, previous_subs)| {
                    relation.iter().for_each(|ground_fact| {
                        let ground_terms = ground_fact
                            .iter()
                            .map(|typed_value| return Term::Constant(typed_value.clone()))
                            .collect();

                        let proposed_atom = Atom {
                            terms: ground_terms,
                            relation_id: NonZeroU32::try_from(*key).unwrap(),
                            positive: true,
                        };

                        if let Some(new_subs) = unify(rewrite_attempt, &proposed_atom) {
                            let mut extended_subs = previous_subs.clone();
                            extended_subs.extend(new_subs);

                            subs_product.push((current_local_atom_id + 1, extended_subs));
                        }
                    })
                },
            );
        }
    }

    subs_product
        .into_iter()
        .filter(|(local_atom_id, _)| *local_atom_id == goals.len())
        .for_each(|(_local_atom_id, subs)| {
            let fresh_atom = attempt_to_rewrite(&subs, &head);
            if is_ground(&fresh_atom) {
                out.insert(terms_to_row(fresh_atom.terms));
            }
        });

    if out.is_empty() {
        return None;
    }

    return Some(out);
}

#[cfg(test)]
mod tests {
    use crate::misc::helpers::terms_to_row;
    use crate::misc::string_interning::Interner;
    use crate::models::datalog::{Atom, SugaredRule, Ty};
    use crate::models::index::VecIndex;
    use crate::models::instance::{Database, HashSetDatabase, IndexedHashSetBacking};
    use crate::models::reasoner::{BottomUpEvaluator, Dynamic};
    use crate::reasoning::algorithms::rewriting::evaluate_rule;
    use crate::reasoning::reasoners::chibi::ChibiDatalog;
    use crate::reasoning::reasoners::relational::RelationalDatalog;

    #[test]
    fn test_pathological_case() {
        let mut chibi = ChibiDatalog::new(false, false, true);
        let mut relational = RelationalDatalog::<VecIndex>::new(false, false);

        let program = vec![
            SugaredRule::from("+reach(?x, ?y) <- [-reach(?x, ?y), edge(?x, ?y)]"),
            SugaredRule::from("+reach(?x, ?z) <- [-reach(?x, ?z), edge(?x, ?y), reach(?y, ?z)]"),
        ];

        vec![
            ("a", "b"),
            ("a", "c"),
            ("b", "d"),
            ("b", "e"),
            ("d", "g"),
            ("c", "f"),
            ("e", "d"),
            ("f", "g"),
            ("f", "h"),
        ]
        .into_iter()
        .for_each(|(source, destination)| {
            chibi.insert("edge", vec![Box::new(source), Box::new(destination)]);
            relational.insert("edge", vec![Box::new(source), Box::new(destination)])
        });

        vec![
            ("a", "b"),
            ("a", "c"),
            ("b", "d"),
            ("b", "e"),
            ("d", "g"),
            ("c", "f"),
            ("e", "d"),
            ("f", "g"),
            ("f", "h"),
            ("a", "d"),
            ("a", "e"),
            ("c", "g"),
            ("c", "h"),
            ("e", "h"),
        ]
        .into_iter()
        .for_each(|(source, destination)| {
            chibi.insert("reach", vec![Box::new(source), Box::new(destination)]);
            relational.insert("reach", vec![Box::new(source), Box::new(destination)])
        });

        vec![
            ("a", "h"),
            ("b", "g"),
            ("b", "h"),
            ("e", "f"),
            ("e", "g"),
            ("e", "h"),
            ("a", "f"),
            ("b", "f"),
            ("a", "g"),
        ]
        .into_iter()
        .for_each(|(source, destination)| {
            chibi.insert("-reach", vec![Box::new(source), Box::new(destination)]);
            relational.insert("-reach", vec![Box::new(source), Box::new(destination)]);
        });

        let actual_evaluation = chibi.evaluate_program_bottom_up(&program);
        let expected_evaluation = relational.evaluate_program_bottom_up(&program);

        assert_eq!(expected_evaluation, actual_evaluation);
    }

    #[test]
    fn test_evaluate_rule() {
        let mut interner: Interner = Default::default();

        let sugared_rule =
            SugaredRule::from("ancestor(?X, ?Z) <- [ancestor(?X, ?Y), ancestor(?Y, ?Z)]");
        let rule = interner.intern_rule(&sugared_rule);

        let mut fact_store: HashSetDatabase = Default::default();

        let fact_0 = Atom::from_str_with_interner("ancestor(adam, jumala)", &mut interner);
        let fact_1 = Atom::from_str_with_interner("ancestor(vanasarvik, jumala)", &mut interner);
        let fact_2 = Atom::from_str_with_interner("ancestor(eve, adam)", &mut interner);
        let fact_3 = Atom::from_str_with_interner("ancestor(jumala, cthulu)", &mut interner);

        vec![fact_0, fact_1, fact_2, fact_3]
            .into_iter()
            .for_each(|atom| {
                fact_store.insert_at(atom.relation_id.get(), terms_to_row(atom.terms))
            });

        let mut expected_output: IndexedHashSetBacking = Default::default();
        vec![
            Box::new([
                Box::new("adam").to_typed_value(),
                Box::new("cthulu").to_typed_value(),
            ]),
            Box::new([
                Box::new("vanasarvik").to_typed_value(),
                Box::new("cthulu").to_typed_value(),
            ]),
            Box::new([
                Box::new("eve").to_typed_value(),
                Box::new("jumala").to_typed_value(),
            ]),
        ]
        .into_iter()
        .for_each(|row| {
            expected_output.insert(row);
        });

        let actual_evaluation = evaluate_rule(&fact_store, &rule, false).unwrap();

        assert_eq!(expected_output, actual_evaluation)
    }
}
