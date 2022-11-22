use rayon::prelude::*;
use crate::data_structures::substitutions::Substitutions;
use crate::models::datalog::{Atom, Body, Rule, Sign, Term};
use crate::models::index::IndexBacking;
use crate::models::instance::Instance;
use crate::models::relational_algebra::Relation;

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
        symbol: atom.clone().symbol,
        sign: atom.clone().sign,
    };
}

pub fn generate_all_substitutions<T>(
    knowledge_base: &Instance<T>,
    target_atom: &Atom,
) -> Vec<Substitutions>
    where T: IndexBacking {
    let relation = knowledge_base.view(&target_atom.symbol);

    return relation
        .into_par_iter()
        .filter_map(|row| {
            let term_vec = row
                .into_iter()
                .map(|row_element| Term::Constant(row_element.clone()))
                .collect();

            return make_substitutions(
                target_atom,
                &Atom {
                    terms: term_vec,
                    symbol: target_atom.symbol.to_string(),
                    sign: Sign::Positive,
                },
            );
        })
        .collect();
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

pub fn accumulate_substitutions<T>(
    knowledge_base: &Instance<T>,
    target_atom: &Atom,
    input_substitutions: Vec<Substitutions>,
) -> Vec<Substitutions>
    where T: IndexBacking {
    return input_substitutions
        .iter()
        .fold(vec![], |mut acc, substitution| {
            let rewrite_attempt = &attempt_to_rewrite(substitution, target_atom);
            if !is_ground(rewrite_attempt) {
                let mut new_substitutions: Vec<Substitutions> =
                    generate_all_substitutions(knowledge_base, rewrite_attempt)
                        .iter()
                        .map(|inner_sub| {
                            let mut outer_sub = substitution.clone();
                            let inner_sub_cl = inner_sub;
                            outer_sub.extend(inner_sub_cl);
                            return outer_sub;
                        })
                        .collect();
                acc.append(&mut new_substitutions)
            }
            acc
        });
}

pub fn accumulate_body_substitutions<T>(knowledge_base: &Instance<T>, body: Body) -> Vec<Substitutions>
    where T: IndexBacking {
    return body
        .into_iter()
        .fold(vec![Default::default()], |acc, item| {
            accumulate_substitutions(knowledge_base, &item, acc)
        });
}

pub fn ground_head<T>(head: &Atom, substitutions: Vec<Substitutions>) -> Option<Relation<T>>
    where T: IndexBacking {
    let mut output_instance = Instance::new(false);

    substitutions.into_iter().for_each(|substitutions| {
        let rewrite_attempt = attempt_to_rewrite(&substitutions, head);
        output_instance.insert_atom(&rewrite_attempt);
    });

    if let Some(relation) = output_instance.database.get(&head.symbol) {
        return Some(relation.clone());
    }
    return None;
}

pub fn evaluate_rule<T>(knowledge_base: &Instance<T>, rule: &Rule) -> Option<Relation<T>>
    where T: IndexBacking {
    return ground_head(
        &rule.head,
        accumulate_body_substitutions(knowledge_base, rule.clone().body),
    );
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{Atom, Rule, TypedValue};
    use crate::models::instance::Instance;
    use crate::data_structures::substitutions::Substitutions;
    use crate::models::index::BTreeIndex;
    use crate::reasoning::algorithms::rewriting::{accumulate_body_substitutions, accumulate_substitutions, attempt_to_rewrite, is_ground, make_substitutions};

    use super::generate_all_substitutions;

    #[test]
    fn test_make_substitution() {
        let rule_atom_0 = Atom::from("edge(?X, ?Y)");
        let data_0 = Atom::from("edge(a,b)");

        if let Some(sub) = make_substitutions(&rule_atom_0, &data_0) {
            let expected_sub: Substitutions = Substitutions {
                inner: vec![
                    (0, TypedValue::Str("a".to_string())),
                    (1, TypedValue::Str("b".to_string())),
                ]
            };
            assert_eq!(sub, expected_sub);
        } else {
            panic!()
        };
    }

    #[test]
    fn test_attempt_to_substitute() {
        let rule_atom_0 = Atom::from("edge(?X, ?Y)");
        let data_0 = Atom::from("edge(a,b)");

        if let Some(sub) = make_substitutions(&rule_atom_0, &data_0) {
            assert_eq!(data_0, attempt_to_rewrite(&sub, &rule_atom_0))
        } else {
            panic!()
        };
    }

    #[test]
    fn test_ground_atom() {
        let mut fact_store: Instance<BTreeIndex> = Instance::new(false);
        let rule_atom_0 = Atom::from("edge(?X, ?Y)");
        fact_store.insert_atom(&Atom::from("edge(a, b)"));
        fact_store.insert_atom(&Atom::from("edge(b, c)"));

        let subs = generate_all_substitutions(&fact_store, &rule_atom_0);
        assert_eq!(
            subs,
            vec![
                Substitutions {
                    inner:
                    vec![
                        (0, TypedValue::Str("a".to_string())),
                        (1, TypedValue::Str("b".to_string())),
                    ]
                },
                Substitutions {
                    inner:
                    vec![
                        (0, TypedValue::Str("b".to_string())),
                        (1, TypedValue::Str("c".to_string())),
                    ]
                },
            ]
        )
    }

    #[test]
    fn test_is_ground() {
        let rule_atom_0 = Atom::from("T(?X, ?Y, PLlab)");
        let data_0 = Atom::from("T(student, takesClassesFrom, PLlab)");

        assert_eq!(is_ground(&rule_atom_0), false);
        assert_eq!(is_ground(&data_0), true)
    }

    #[test]
    fn test_extend_substitutions() {
        let rule_atom_0 = Atom::from("T(?X, ?Y, PLlab)");
        let mut fact_store: Instance<BTreeIndex> = Instance::new(false);
        fact_store.insert_atom(&Atom::from("T(student, takesClassesFrom, PLlab)"));
        fact_store.insert_atom(&Atom::from("T(professor, worksAt, PLlab)"));

        let partial_subs = vec![
            Substitutions {
                inner: vec![(0, TypedValue::Str("student".to_string()))]
            },
            Substitutions {
                inner: vec![(0, TypedValue::Str("professor".to_string()))]
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
    fn test_explode_body_substitutions() {
        let rule = Rule::from("ancestor(?X, ?Z) <- [ancestor(?X, ?Y), ancestor(?Y, ?Z)]");
        let rule_body = rule.body;

        let mut fact_store: Instance<BTreeIndex> = Instance::new(false);

        fact_store.insert_atom(&Atom::from("ancestor(adam, jumala)"));
        fact_store.insert_atom(&Atom::from("ancestor(vanasarvik, jumala)"));
        fact_store.insert_atom(&Atom::from("ancestor(eve, adam)"));
        fact_store.insert_atom(&Atom::from("ancestor(jumala, cthulu)"));


        let fitting_substitutions = vec![
            Substitutions {
                inner: vec![
                    (0, TypedValue::Str("adam".to_string())),
                    (1, TypedValue::Str("cthulu".to_string())),
                    (2, TypedValue::Str("jumala".to_string())),
                ]
            },
            Substitutions {
                inner: vec![
                    (0, TypedValue::Str("vanasarvik".to_string())),
                    (1, TypedValue::Str("cthulu".to_string())),
                    (2, TypedValue::Str("jumala".to_string())),
                ]
            },
            Substitutions {
                inner: vec![
                    (0, TypedValue::Str("eve".to_string())),
                    (1, TypedValue::Str("jumala".to_string())),
                    (2, TypedValue::Str("adam".to_string())),
                ]
            },
        ];
        let all_substitutions = accumulate_body_substitutions(&fact_store, rule_body);
        assert_eq!(all_substitutions, fitting_substitutions);
    }
}
