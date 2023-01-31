use crate::data_structures::substitutions::Substitutions;
use crate::misc::helpers::terms_to_row;
use crate::models::datalog::{Atom, Rule, Term};
use crate::models::instance::{Database, HashSetBacking, HashSetDatabase};

pub fn make_substitutions(left: &Atom, right: &Atom) -> Option<Substitutions> {
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
                    if constant.clone() != *right_constant {
                        return None;
                    }
                } else {
                    substitution.insert((left_variable.clone(), right_constant.clone()));
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
            .clone()
            .into_iter()
            .map(|term| {
                if let Term::Variable(identifier) = term.clone() {
                    if let Some(constant) = rewrite.get(identifier) {
                        return Term::Constant(constant.clone());
                    }
                }
                return term;
            })
            .collect(),
        relation_id: atom.relation_id,
        positive: atom.clone().positive,
    };
}

pub fn generate_all_substitutions(
    knowledge_base: &HashSetDatabase,
    target_atom: &Atom,
) -> Vec<Substitutions> {
    if let Some(rel) = knowledge_base.storage.get(&target_atom.relation_id.get()) {
        return rel
            .into_iter()
            .filter_map(|row| {
                let term_vec = row
                    .into_iter()
                    .map(|row_element| Term::Constant(row_element.clone()))
                    .collect();

                return make_substitutions(
                    target_atom,
                    &Atom {
                        terms: term_vec,
                        relation_id: target_atom.relation_id,
                        positive: true,
                    },
                );
            })
            .collect();
    }

    return vec![];
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

pub fn accumulate_substitutions(
    knowledge_base: &HashSetDatabase,
    target_atom: &Atom,
    input_substitutions: Vec<Substitutions>,
) -> Vec<Substitutions> {
    return input_substitutions
        .into_iter()
        .fold(vec![], |mut acc, substitution| {
            let rewrite_attempt = &attempt_to_rewrite(&substitution, target_atom);
            if !is_ground(rewrite_attempt) {
                let mut new_substitutions: Vec<Substitutions> =
                    generate_all_substitutions(knowledge_base, rewrite_attempt)
                        .into_iter()
                        .map(|inner_sub| {
                            let mut outer_sub = substitution.clone();
                            outer_sub.extend(&inner_sub);
                            return outer_sub;
                        })
                        .collect();
                acc.append(&mut new_substitutions)
            }
            acc
        });
}

pub fn accumulate_body_substitutions(
    knowledge_base: &HashSetDatabase,
    body: &Vec<Atom>,
) -> Vec<Substitutions>
{
    return body
        .into_iter()
        .fold(vec![Default::default()], |acc, item| {
            accumulate_substitutions(knowledge_base, item, acc)
        });
}

pub fn ground_head(head: &Atom, substitutions: Vec<Substitutions>) -> Option<HashSetBacking> {
    let mut output_instance: HashSetDatabase = Default::default();

    substitutions.into_iter().for_each(|substitutions| {
        let rewrite_attempt = attempt_to_rewrite(&substitutions, head);
        output_instance.insert_at(rewrite_attempt.relation_id.get(), terms_to_row(rewrite_attempt.terms));
    });

    let mut out = None;
    output_instance
        .storage
        .into_iter()
        .for_each(|(relation_id, relation)| {
            if relation_id == head.relation_id.get() {
                out = Some(relation);
            }
        });

    return out;
}

pub fn evaluate_rule(knowledge_base: &HashSetDatabase, rule: &Rule) -> Option<HashSetBacking> {
    return ground_head(
        &rule.head,
        accumulate_body_substitutions(knowledge_base, &rule.body),
    );
}

#[cfg(test)]
mod tests {
    use crate::data_structures::substitutions::Substitutions;
    use crate::misc::helpers::terms_to_row;
    use crate::misc::string_interning::Interner;
    use crate::models::datalog::{Atom, SugaredRule, Ty, TypedValue};
    use crate::models::instance::{Database, HashSetBacking, HashSetDatabase};
    use crate::reasoning::algorithms::rewriting::{accumulate_body_substitutions, accumulate_substitutions, attempt_to_rewrite, evaluate_rule, ground_head, is_ground, make_substitutions};

    use super::generate_all_substitutions;

    #[test]
    fn test_make_substitutions() {
        let mut interner: Interner = Default::default();

        let rule_atom_0 = Atom::from_str_with_interner("edge(?X, ?Y)", &mut interner);
        let data_0 = Atom::from_str_with_interner("edge(a,b)", &mut interner);

        let sub = make_substitutions(&rule_atom_0, &data_0);

        let expected_sub: Substitutions = Substitutions {
            inner: vec![
                (0, TypedValue::Str("a".to_string())),
                (1, TypedValue::Str("b".to_string())),
            ],
        };

        assert_eq!(sub.unwrap(), expected_sub);
    }

    #[test]
    fn test_attempt_to_rewrite() {
        let mut interner: Interner = Default::default();

        let rule_atom_0 = Atom::from_str_with_interner("edge(?X, ?Y)", &mut interner);
        let data_0 = Atom::from_str_with_interner("edge(a,b)", &mut interner);

        if let Some(sub) = make_substitutions(&rule_atom_0, &data_0) {
            assert_eq!(data_0, attempt_to_rewrite(&sub, &rule_atom_0))
        } else {
            panic!()
        };
    }

    #[test]
    fn test_ground_atom() {
        let mut interner: Interner = Default::default();
        let mut fact_store: HashSetDatabase = Default::default();

        let rule_atom_0 = Atom::from_str_with_interner("edge(?X, ?Y)", &mut interner);

        let fact_0 = Atom::from_str_with_interner("edge(a, b)", &mut interner);
        let fact_1 = Atom::from_str_with_interner("edge(b, c)", &mut interner);

        fact_store.insert_at(fact_0.relation_id.get(), terms_to_row(fact_0.terms));
        fact_store.insert_at(fact_1.relation_id.get(), terms_to_row(fact_1.terms));

        let subs = generate_all_substitutions(&fact_store, &rule_atom_0);
        assert_eq!(
            subs,
            vec![
                Substitutions {
                    inner: vec![
                        (0, TypedValue::Str("b".to_string())),
                        (1, TypedValue::Str("c".to_string())),
                    ]
                },
                Substitutions {
                    inner: vec![
                        (0, TypedValue::Str("a".to_string())),
                        (1, TypedValue::Str("b".to_string())),
                    ]
                },
            ]
        )
    }

    #[test]
    fn test_is_ground() {
        let mut interner: Interner = Default::default();

        let rule_atom_0 = Atom::from_str_with_interner("T(?X, ?Y, PLlab)", &mut interner);
        let data_0 = Atom::from_str_with_interner("T(student, takesClassesFrom, PLlab)", &mut interner);

        assert_eq!(is_ground(&rule_atom_0), false);
        assert_eq!(is_ground(&data_0), true)
    }

    #[test]
    fn test_accumulate_substitutions() {
        let mut interner: Interner = Default::default();
        let mut fact_store: HashSetDatabase = Default::default();

        let rule_atom_0 = Atom::from_str_with_interner("T(?X, ?Y, PLlab)", &mut interner);
        let fact_0 = Atom::from_str_with_interner("T(student, takesClassesFrom, PLlab)", &mut interner);
        let fact_1 = Atom::from_str_with_interner("T(professor, worksAt, PLlab)", &mut interner);

        fact_store.insert_at(fact_0.relation_id.get(), terms_to_row(fact_0.terms));
        fact_store.insert_at(fact_1.relation_id.get(), terms_to_row(fact_1.terms));

        let partial_subs = vec![
            Substitutions {
                inner: vec![(0, TypedValue::Str("student".to_string()))],
            },
            Substitutions {
                inner: vec![(0, TypedValue::Str("professor".to_string()))],
            },
        ];

        let subs = accumulate_substitutions(&fact_store, &rule_atom_0, partial_subs);
        assert_eq!(
            subs,
            vec![
                Substitutions {
                    inner: vec![
                        (0, TypedValue::Str("student".to_string())),
                        (1, TypedValue::Str("takesClassesFrom".to_string())),
                    ]
                },
                Substitutions {
                    inner: vec![
                        (0, TypedValue::Str("professor".to_string())),
                        (1, TypedValue::Str("worksAt".to_string())),
                    ]
                },
            ]
        )
    }

    #[test]
    fn test_accumulate_body_substitutions() {
        let mut interner: Interner = Default::default();

        let rule = SugaredRule::from("ancestor(?X, ?Z) <- [ancestor(?X, ?Y), ancestor(?Y, ?Z)]");
        let _rule_head = interner.intern_atom_weak(&rule.head);
        let rule_body = rule
            .body
            .iter()
            .map(|atom| interner.intern_atom_weak(atom))
            .collect();

        let mut fact_store: HashSetDatabase = Default::default();

        let fact_0 = Atom::from_str_with_interner("ancestor(adam, jumala)", &mut interner);
        let fact_1 = Atom::from_str_with_interner("ancestor(vanasarvik, jumala)", &mut interner);
        let fact_2 = Atom::from_str_with_interner("ancestor(eve, adam)", &mut interner);
        let fact_3 = Atom::from_str_with_interner("ancestor(jumala, cthulu)", &mut interner);

        vec![fact_0,
             fact_1,
             fact_2,
             fact_3]
            .iter()
            .for_each(|atom| {
                fact_store.insert_at(atom.relation_id.get(), terms_to_row(atom.terms.clone()))
            });


        let fitting_substitutions = vec![
            Substitutions {
                inner: vec![
                    (0, TypedValue::Str("adam".to_string())),
                    (1, TypedValue::Str("cthulu".to_string())),
                    (2, TypedValue::Str("jumala".to_string())),
                ],
            },
            Substitutions {
                inner: vec![
                    (0, TypedValue::Str("vanasarvik".to_string())),
                    (1, TypedValue::Str("cthulu".to_string())),
                    (2, TypedValue::Str("jumala".to_string())),
                ],
            },
            Substitutions {
                inner: vec![
                    (0, TypedValue::Str("eve".to_string())),
                    (1, TypedValue::Str("jumala".to_string())),
                    (2, TypedValue::Str("adam".to_string())),
                ],
            },
        ];
        let all_substitutions = accumulate_body_substitutions(&fact_store, &rule_body);
        assert_eq!(all_substitutions, fitting_substitutions);
    }

    #[test]
    fn test_ground_head() {
        let mut interner: Interner = Default::default();

        let rule = SugaredRule::from("ancestor(?X, ?Z) <- [ancestor(?X, ?Y), ancestor(?Y, ?Z)]");
        let rule_head = interner.intern_atom_weak(&rule.head);

        let fitting_substitutions = vec![
            Substitutions {
                inner: vec![
                    (0, TypedValue::Str("adam".to_string())),
                    (1, TypedValue::Str("cthulu".to_string())),
                    (2, TypedValue::Str("jumala".to_string())),
                ],
            },
            Substitutions {
                inner: vec![
                    (0, TypedValue::Str("vanasarvik".to_string())),
                    (1, TypedValue::Str("cthulu".to_string())),
                    (2, TypedValue::Str("jumala".to_string())),
                ],
            },
            Substitutions {
                inner: vec![
                    (0, TypedValue::Str("eve".to_string())),
                    (1, TypedValue::Str("jumala".to_string())),
                    (2, TypedValue::Str("adam".to_string())),
                ],
            },
        ];

        let groundingtons = ground_head(&rule_head, fitting_substitutions).unwrap();
        let mut expected_output: HashSetBacking = Default::default();
        vec![
            Box::new([Box::new("adam").to_typed_value(), Box::new("cthulu").to_typed_value()]),
            Box::new([Box::new("vanasarvik").to_typed_value(), Box::new("cthulu").to_typed_value()]),
            Box::new([Box::new("eve").to_typed_value(), Box::new("jumala").to_typed_value()]),
        ]
            .into_iter()
            .for_each(|row| { expected_output.insert(row); });

        assert_eq!(groundingtons, expected_output);
    }

    #[test]
    fn test_evaluate_rule() {
        let mut interner: Interner = Default::default();

        let sugared_rule = SugaredRule::from("ancestor(?X, ?Z) <- [ancestor(?X, ?Y), ancestor(?Y, ?Z)]");
        let rule = interner.intern_rule_weak(&sugared_rule);

        let mut fact_store: HashSetDatabase = Default::default();

        let fact_0 = Atom::from_str_with_interner("ancestor(adam, jumala)", &mut interner);
        let fact_1 = Atom::from_str_with_interner("ancestor(vanasarvik, jumala)", &mut interner);
        let fact_2 = Atom::from_str_with_interner("ancestor(eve, adam)", &mut interner);
        let fact_3 = Atom::from_str_with_interner("ancestor(jumala, cthulu)", &mut interner);

        vec![fact_0,
             fact_1,
             fact_2,
             fact_3]
            .iter()
            .for_each(|atom| {
                fact_store.insert_at(atom.relation_id.get(), terms_to_row(atom.terms.clone()))
            });

        let mut expected_output: HashSetBacking = Default::default();
        vec![
            Box::new([Box::new("adam").to_typed_value(), Box::new("cthulu").to_typed_value()]),
            Box::new([Box::new("vanasarvik").to_typed_value(), Box::new("cthulu").to_typed_value()]),
            Box::new([Box::new("eve").to_typed_value(), Box::new("jumala").to_typed_value()]),
        ]
            .into_iter()
            .for_each(|row| { expected_output.insert(row); });

        let evaluation = evaluate_rule(&fact_store, &rule).unwrap();

        assert_eq!(expected_output, evaluation)
    }
}
