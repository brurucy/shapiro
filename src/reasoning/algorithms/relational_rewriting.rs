use std::num::NonZeroU32;
use ahash::HashSet;
use crate::data_structures::substitutions::Substitutions;
use crate::misc::helpers::terms_to_row;
use crate::models::datalog::{Atom, Program, Rule, Term};
use crate::models::instance::{Database, HashSetDatabase, IndexedHashSetBacking};
use crate::reasoning::algorithms::evaluation::Set;
use crate::reasoning::algorithms::rewriting::{attempt_to_rewrite, is_ground, make_substitutions};

pub fn nested_loop_join<'a, K: 'a, V: 'a, T: 'a, Left: 'a, Right: 'a>(
    left_iter: &'a Left,
    right_iter: &'a Right,
    mut f: impl FnMut(K, V, T),
) where
    &'a Left: 'a + IntoIterator<Item = &'a (K, V)>,
    &'a Right: 'a + IntoIterator<Item = &'a (K, T)>,
    K: PartialEq + Clone,
    V: Clone,
    T: Clone
{
    left_iter.into_iter().for_each(|(left_key, left_value)| {
        right_iter.into_iter().for_each(|(right_key, right_value)| {
            if left_key == right_key {
                // :( wasteful
                f(left_key.clone(), left_value.clone(), right_value.clone())
            }
        })
    })
}

pub fn evaluate_rule(knowledge_base: &HashSetDatabase, rule: &Rule) -> IndexedHashSetBacking {
    let mut out: IndexedHashSetBacking = Default::default();

    let head = rule
        .head
        .clone();

    let goals: Vec<(usize, Atom)> = rule
        .body
        .clone()
        .into_iter()
        .enumerate()
        .collect();

    let mut subs_product: HashSet<_> = vec![(0usize, Substitutions::default())].into_iter().collect();

    for _ in 0..goals.len() {
        let mut current_goals_x_subs: Vec<(u32, (usize, Atom, Substitutions))> = vec![];

        nested_loop_join(&goals, &subs_product, |current_local_atom_id, left_value, subs| {
            let rewrite_attempt = attempt_to_rewrite(&subs, &left_value);
            if !is_ground(&rewrite_attempt) {
                current_goals_x_subs.push((left_value.relation_id.get(), (current_local_atom_id, rewrite_attempt, subs.clone())));
            }
        });

        let mut new_substitutions = vec![];

        let meh: Vec<_> = knowledge_base.storage.clone().into_iter().collect();

        nested_loop_join(&meh, &current_goals_x_subs, |key: u32, fact_set: IndexedHashSetBacking, (current_local_atom_id, rewrite_attempt, previous_subs): (usize, Atom, Substitutions)| {
            fact_set
                .iter()
                .for_each(|ground_fact| {
                    let ground_terms = ground_fact
                        .iter()
                        .map(|row| {
                            return Term::Constant(row.clone())
                        })
                        .collect();

                    let proposed_atom = Atom{ terms: ground_terms, relation_id: NonZeroU32::try_from(key).unwrap(), positive: true };

                    if let Some (new_subs) = make_substitutions(&rewrite_attempt, &proposed_atom) {
                        let mut new_sub = previous_subs.clone();
                        new_sub.inner.extend(new_subs.inner);

                        new_substitutions.push(((current_local_atom_id + 1), new_sub));
                    }
                })
        });

        new_substitutions
            .into_iter()
            .for_each(|(local_atom_id, subs)| { subs_product.insert((local_atom_id, subs)); });
    };



    out
}

// This is a clear-cut implementation of the algorithm used in the differential engine.
// pub fn evaluate_program(knowledge_base: &HashSetDatabase, program: &Program) -> HashSetDatabase {
//     let mut out: HashSetDatabase = Default::default();
//     out.storage = knowledge_base.storage.clone();
//
//     let indexed_rules: Vec<_> = program
//         .iter()
//         .enumerate()
//         .collect();
//
//     let goals: Vec<((usize, usize), Atom)> = indexed_rules
//         .iter()
//         .flat_map(|(rule_id, rule)| {
//             rule
//                 .body
//                 .iter()
//                 .enumerate()
//                 .map(|(local_atom_id, atom)| ((rule_id.clone(), local_atom_id), atom.clone()))
//                 .collect::<Vec<_>>()
//         })
//         .collect();
//
//     let heads: Vec<_> = indexed_rules
//         .iter()
//         .map(|(rule_id, rule)| (rule_id.clone(), rule.head.clone()))
//         .collect();
//
//     let mut subs_product: HashSet<_> = heads
//         .iter()
//         .map(|(rule_id, _head)| ((rule_id.clone(), 0usize), Substitutions::default()))
//         .collect();
//
//     for _ in 0..goals.len() {
//         let mut current_goals_x_subs: Vec<(u32, ((usize, usize), Atom, Substitutions))> = vec![];
//
//         nested_loop_join(goals.clone(), subs_product.clone(), |(rule_id, current_local_atom_id), left_value, subs| {
//             let rewrite_attempt = attempt_to_rewrite(&subs, &left_value);
//             if !is_ground(&rewrite_attempt) {
//                 current_goals_x_subs.push((left_value.relation_id.get(), ((rule_id, current_local_atom_id), rewrite_attempt, subs)));
//             }
//         });
//
//         let mut new_substitutions = vec![];
//
//         nested_loop_join(out.storage.clone(), current_goals_x_subs, |key: u32, fact_set: IndexedHashSetBacking, ((rule_id, current_local_atom_id), rewrite_attempt, subs): ((usize, usize), Atom, Substitutions)| {
//             fact_set
//                 .iter()
//                 .for_each(|ground_fact| {
//                     let ground_terms = ground_fact
//                         .iter()
//                         .map(|row| {
//                             return Term::Constant(row.clone())
//                         })
//                         .collect();
//
//                     let proposed_atom = Atom{ terms: ground_terms, relation_id: NonZeroU32::try_from(key).unwrap(), positive: true };
//
//                     if let Some (new_subs) = make_substitutions(&rewrite_attempt, &proposed_atom) {
//                         let mut previous_sub = subs.clone();
//                         previous_sub.inner.extend(new_subs.inner);
//
//                         new_substitutions.push(((rule_id, current_local_atom_id + 1), previous_sub));
//                     }
//                 })
//         });
//
//         new_substitutions
//             .into_iter()
//             .for_each(|((rule_id, local_atom_id), subs)| { subs_product.insert(((rule_id, local_atom_id), subs)); });
//     };
//
//     nested_loop_join(heads, subs_product.into_iter().map(|((rule_id, _local_atom_id), subs)| (rule_id, subs)).collect::<Vec<_>>(), |_rule_id, rule_head, subs| {
//         let fresh_atom = attempt_to_rewrite(&subs, &rule_head);
//         if is_ground(&fresh_atom) {
//             out.insert_at(fresh_atom.relation_id.get(), terms_to_row(fresh_atom.terms.clone()));
//         }
//     });
//
//     return out.difference(knowledge_base)
// }

#[cfg(test)]
mod tests {
    use crate::misc::helpers::terms_to_row;
    use crate::misc::string_interning::Interner;
    use crate::models::datalog::{Atom, SugaredRule, Ty};
    use crate::models::instance::{Database, HashSetDatabase, IndexedHashSetBacking};
    use crate::reasoning::algorithms::relational_rewriting::evaluate_program;

    #[test]
    fn test_evaluate_program() {
        let mut interner: Interner = Default::default();

        let sugared_program = vec![
            SugaredRule::from("ancestor(?X, ?Y) <- [parent(?X, ?Y)]"),
            SugaredRule::from("ancestor(?X, ?Z) <- [ancestor(?X, ?Y), ancestor(?Y, ?Z)]"),
        ];
        let program = sugared_program
            .into_iter()
            .map(|rule| interner.intern_rule_weak(&rule))
            .collect();

        let mut fact_store: HashSetDatabase = Default::default();

        let fact_0 = Atom::from_str_with_interner("parent(adam, jumala)", &mut interner);
        let fact_1 = Atom::from_str_with_interner("parent(vanasarvik, jumala)", &mut interner);
        let fact_2 = Atom::from_str_with_interner("parent(eve, adam)", &mut interner);
        let fact_3 = Atom::from_str_with_interner("parent(jumala, cthulu)", &mut interner);

        vec![fact_0, fact_1, fact_2, fact_3]
            .into_iter()
            .for_each(|atom| {
                fact_store.insert_at(atom.relation_id.get(), terms_to_row(atom.terms.clone()))
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

        let relation_id = interner.rodeo.get("ancestor").unwrap().into_inner().get();
        let actual_evaluation = evaluate_program(&fact_store, &program)
            .storage
            .get(&relation_id)
            .unwrap()
            .clone();

        assert_eq!(expected_output, actual_evaluation)
    }
}