use crate::models::{
    datalog::{Atom, Body, BottomUpEvaluator, Rule, Sign, Substitutions, Term},
    instance::Instance,
    relational_algebra::{Column, Relation},
};
use std::collections::HashMap;

// This is an implementation of the simplest datalog algorithm, INFER1
pub fn make_substitutions(left: &Atom, right: &Atom) -> Option<Substitutions> {
    let mut substitution: Substitutions = HashMap::new();

    let left_and_right = left.terms.iter().zip(right.terms.iter());

    for (left_term, right_term) in left_and_right {
        match (left_term, right_term) {
            (Term::Constant(left_constant), Term::Constant(right_constant)) => {
                if left_constant != right_constant {
                    return None;
                }
            }
            (Term::Variable(left_variable), Term::Constant(right_constant)) => {
                if let Some(constant) = substitution.get(left_variable.as_str()) {
                    if constant.clone() != *right_constant {
                        return None;
                    }
                } else {
                    substitution.insert(left_variable.clone(), right_constant.clone());
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
                    if let Some(constant) = rewrite.get(&identifier) {
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

pub fn generate_all_substitutions(
    knowledge_base: &Instance,
    target_atom: &Atom,
) -> Vec<Substitutions> {
    let relation = knowledge_base.view(&target_atom.symbol);

    return relation
        .into_iter()
        .map(|row| {
            let term_vec = row
                .into_iter()
                .map(|row_element| Term::Constant(row_element))
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
        .filter(|substitution| substitution.clone() != None)
        .map(|some_fact| some_fact.unwrap())
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

pub fn accumulate_substitutions(
    knowledge_base: &Instance,
    target_atom: &Atom,
    input_substitutions: Vec<Substitutions>,
) -> Vec<Substitutions> {
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
                            let inner_sub_cl = inner_sub.clone();
                            outer_sub.extend(inner_sub_cl);
                            return outer_sub;
                        })
                        .collect();
                acc.append(&mut new_substitutions)
            }
            acc
        });
}

pub fn accumulate_body_substitutions(knowledge_base: &Instance, body: Body) -> Vec<Substitutions> {
    return body
        .into_iter()
        .fold(vec![HashMap::default()], |acc, item| {
            accumulate_substitutions(knowledge_base, &item, acc)
        });
}

pub fn ground_head(head: &Atom, substitutions: Vec<Substitutions>) -> Option<Relation> {
    let mut output_instance = Instance::new();

    substitutions.into_iter().for_each(|substitutions| {
        let rewrite_attempt = attempt_to_rewrite(&substitutions, head);
        output_instance.insert_atom(&rewrite_attempt);
    });

    if let Some(relation) = output_instance.database.get(&head.symbol) {
        return Some(relation.clone());
    }
    return None;
}

pub fn evaluate_rule(knowledge_base: &Instance, rule: &Rule) -> Option<Relation> {
    return ground_head(
        &rule.head,
        accumulate_body_substitutions(knowledge_base, rule.clone().body),
    );
}

pub fn evaluate_program(knowledge_base: &Instance, program: Vec<Rule>) -> Instance {
    let mut previous_delta = Instance::new();
    let mut current_delta = Instance::new();
    let mut output = Instance::new();

    loop {
        previous_delta = current_delta.clone();
        let mut edb_plus_previous_delta = knowledge_base.clone();
        previous_delta
            .database
            .clone()
            .into_iter()
            .for_each(|relation| {
                relation
                    .1
                    .into_iter()
                    .for_each(|row| edb_plus_previous_delta.insert_typed(&relation.0, row))
            });
        current_delta = Instance::new();
        program.clone().into_iter().for_each(|rule| {
            if let Some(rule_evaluation) = evaluate_rule(&edb_plus_previous_delta, &rule) {
                rule_evaluation.clone().into_iter().for_each(|row| {
                    current_delta.insert_typed(&rule_evaluation.symbol, row.clone());
                    output.insert_typed(&rule_evaluation.symbol, row);
                })
            }
        });

        if previous_delta == current_delta {
            break;
        }
    }

    return output;
}

pub struct ChibiDatalog {
    pub fact_store: Instance,
}

impl BottomUpEvaluator for ChibiDatalog {
    fn evaluate_program_bottom_up(&self, program: Vec<Rule>) -> Instance {
        return evaluate_program(&self.fact_store, program);
    }
}

impl Default for ChibiDatalog {
    fn default() -> Self {
        ChibiDatalog {
            fact_store: Instance::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::implementations::datalog_positive_simple::{
        accumulate_body_substitutions, accumulate_substitutions, attempt_to_rewrite,
        evaluate_program, is_ground, make_substitutions,
    };
    use crate::models::datalog::{Atom, Rule, Sign, Substitutions, Term, TypedValue};
    use crate::models::instance::Instance;
    use crate::parsers::datalog::{parse_atom, parse_rule};
    use std::collections::{HashMap, HashSet};

    use super::generate_all_substitutions;

    #[test]
    fn test_make_substitution() {
        let rule_atom_0 = Atom::from("edge(?X, ?Y)");
        let data_0 = Atom::from("edge(a,b)");

        if let Some(sub) = make_substitutions(&rule_atom_0, &data_0) {
            let expected_sub: Substitutions = vec![
                ("?X".to_string(), TypedValue::Str("a".to_string())),
                ("?Y".to_string(), TypedValue::Str("b".to_string())),
            ]
            .into_iter()
            .collect();
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
        let mut fact_store = Instance::new();
        let rule_atom_0 = Atom::from("edge(?X, ?Y)");
        fact_store.insert_typed(
            "edge",
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("b".to_string()),
            ],
        );
        fact_store.insert_typed(
            "edge",
            vec![
                TypedValue::Str("b".to_string()),
                TypedValue::Str("c".to_string()),
            ],
        );

        let subs = generate_all_substitutions(&fact_store, &rule_atom_0);
        assert_eq!(
            subs,
            vec![
                vec![
                    ("?X".to_string(), TypedValue::Str("a".to_string())),
                    ("?Y".to_string(), TypedValue::Str("b".to_string()))
                ]
                .into_iter()
                .collect::<HashMap<String, TypedValue>>(),
                vec![
                    ("?X".to_string(), TypedValue::Str("b".to_string())),
                    ("?Y".to_string(), TypedValue::Str("c".to_string()))
                ]
                .into_iter()
                .collect::<HashMap<String, TypedValue>>(),
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
        let mut fact_store = Instance::new();
        fact_store.insert_typed(
            "T",
            vec![
                TypedValue::Str("student".to_string()),
                TypedValue::Str("takesClassesFrom".to_string()),
                TypedValue::Str("PLlab".to_string()),
            ],
        );
        fact_store.insert_typed(
            "T",
            vec![
                TypedValue::Str("professor".to_string()),
                TypedValue::Str("worksAt".to_string()),
                TypedValue::Str("PLlab".to_string()),
            ],
        );

        let partial_subs = vec![
            vec![("?X".to_string(), TypedValue::Str("student".to_string()))]
                .into_iter()
                .collect::<HashMap<String, TypedValue>>(),
            vec![("?X".to_string(), TypedValue::Str("professor".to_string()))]
                .into_iter()
                .collect::<HashMap<String, TypedValue>>(),
        ];

        let subs = accumulate_substitutions(&fact_store, &rule_atom_0, partial_subs);
        assert_eq!(
            subs,
            vec![
                vec![
                    ("?X".to_string(), TypedValue::Str("student".to_string())),
                    ("?Y".to_string(), TypedValue::Str("takesClassesFrom".to_string())),
                ]
                .into_iter()
                .collect::<HashMap<String, TypedValue>>(),
                vec![
                    ("?X".to_string(), TypedValue::Str("professor".to_string())),
                    ("?Y".to_string(), TypedValue::Str("worksAt".to_string())),
                ]
                .into_iter()
                .collect::<HashMap<String, TypedValue>>(),
            ]
        )
    }

    #[test]
    fn test_explode_body_substitutions() {
        let rule_atom_0 = Atom::from("ancestor(?X, ?Y)");
        let rule_atom_1 = Atom::from("ancestor(?Y, ?Z)");
        let rule_body = vec![rule_atom_0, rule_atom_1];

        let mut fact_store = Instance::new();
        fact_store.insert_typed(
            "ancestor",
            vec![
                TypedValue::Str("adam".to_string()),
                TypedValue::Str("jumala".to_string()),
            ],
        );
        fact_store.insert_typed(
            "ancestor",
            vec![
                TypedValue::Str("vanasarvik".to_string()),
                TypedValue::Str("jumala".to_string()),
            ],
        );
        fact_store.insert_typed(
            "ancestor",
            vec![
                TypedValue::Str("eve".to_string()),
                TypedValue::Str("adam".to_string()),
            ],
        );
        fact_store.insert_typed(
            "ancestor",
            vec![
                TypedValue::Str("jumala".to_string()),
                TypedValue::Str("cthulu".to_string()),
            ],
        );

        let fitting_substitutions = vec![
            vec![
                ("?X".to_string(), TypedValue::Str("adam".to_string())),
                ("?Y".to_string(), TypedValue::Str("jumala".to_string())),
                ("?Z".to_string(), TypedValue::Str("cthulu".to_string())),
            ]
            .into_iter()
            .collect::<HashMap<String, TypedValue>>(),
            vec![
                ("?X".to_string(), TypedValue::Str("vanasarvik".to_string())),
                ("?Y".to_string(), TypedValue::Str("jumala".to_string())),
                ("?Z".to_string(), TypedValue::Str("cthulu".to_string())),
            ]
            .into_iter()
            .collect::<HashMap<String, TypedValue>>(),
            vec![
                ("?X".to_string(), TypedValue::Str("eve".to_string())),
                ("?Y".to_string(), TypedValue::Str("adam".to_string())),
                ("?Z".to_string(), TypedValue::Str("jumala".to_string())),
            ]
            .into_iter()
            .collect::<HashMap<String, TypedValue>>(),
        ];
        let all_substitutions = accumulate_body_substitutions(&fact_store, rule_body);
        assert_eq!(all_substitutions, fitting_substitutions);
    }

    #[test]
    fn test_evaluate_program() {
        let rule_0 = Rule::from("ancestor(?X, ?Z) <- [ancestor(?X, ?Y), ancestor(?Y, ?Z)]");

        let mut fact_store = Instance::new();
        fact_store.insert_typed(
            "ancestor",
            vec![
                TypedValue::Str("adam".to_string()),
                TypedValue::Str("jumala".to_string()),
            ],
        );
        fact_store.insert_typed(
            "ancestor",
            vec![
                TypedValue::Str("vanasarvik".to_string()),
                TypedValue::Str("jumala".to_string()),
            ],
        );
        fact_store.insert_typed(
            "ancestor",
            vec![
                TypedValue::Str("eve".to_string()),
                TypedValue::Str("adam".to_string()),
            ],
        );
        fact_store.insert_typed(
            "ancestor",
            vec![
                TypedValue::Str("jumala".to_string()),
                TypedValue::Str("cthulu".to_string()),
            ],
        );

        let evaluation: HashSet<Vec<TypedValue>> = evaluate_program(&fact_store, vec![rule_0])
            .database
            .into_iter()
            .flat_map(|(k, v)| v)
            .collect();
        let expected_evaluation: HashSet<Vec<TypedValue>> = vec![
            Atom::from("ancestor(adam, cthulu)"),
            Atom::from("ancestor(vanasarvik, cthulu)"),
            Atom::from("ancestor(eve, jumala)"),
            Atom::from("ancestor(eve, cthulu)"),
        ]
        .into_iter()
        .map(|atom| {
            atom.terms
                .into_iter()
                .map(|term| match term {
                    Term::Constant(constant) => return constant,
                    Term::Variable(_) => unreachable!(),
                })
                .collect()
        })
        .collect();

        assert_eq!(evaluation, expected_evaluation)
    }
}
