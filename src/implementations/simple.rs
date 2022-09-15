use crate::models::datalog::{Atom, Body, BottomUpEvaluator, Rule, Substitutions, Term};
use std::collections::{HashMap, HashSet};

pub fn make_substitutions(left: &Atom, right: &Atom) -> Option<Substitutions> {
    let mut substitution: Substitutions = HashMap::new();

    if left.symbol != right.symbol {
        return None;
    }

    if left.terms.len() != right.terms.len() {
        return None;
    }

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

pub fn ground_atom<I>(knowledge_base: I, target_atom: &Atom) -> Vec<Substitutions>
where
    I: IntoIterator<Item = Atom>,
{
    return knowledge_base
        .into_iter()
        .map(|fact| {
            return make_substitutions(target_atom, &fact);
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

pub fn extend_substitutions<I>(
    knowledge_base: I,
    target_atom: &Atom,
    input_substitutions: Vec<Substitutions>,
) -> Vec<Substitutions>
where
    I: IntoIterator<Item = Atom> + Clone,
{
    return input_substitutions
        .iter()
        .fold(vec![], |mut acc, substitution| {
            let rewrite_attempt = &attempt_to_rewrite(substitution, target_atom);
            if !is_ground(rewrite_attempt) {
                let mut new_substitutions: Vec<Substitutions> =
                    ground_atom(knowledge_base.clone(), rewrite_attempt)
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

pub fn explode_body_substitutions<I>(knowledge_base: I, body: Body) -> Vec<Substitutions>
where
    I: IntoIterator<Item = Atom> + Clone,
{
    return body
        .into_iter()
        .fold(vec![HashMap::default()], |acc, item| {
            extend_substitutions(knowledge_base.clone(), &item, acc)
        });
}

pub fn ground_head(head: &Atom, substitutions: Vec<Substitutions>) -> Vec<Atom> {
    return substitutions.into_iter().fold(vec![], |mut acc, sub| {
        acc.push(attempt_to_rewrite(&sub, head));
        acc
    });
}

pub fn evaluate_rule<I>(knowledge_base: I, rule: &Rule) -> Vec<Atom>
where
    I: IntoIterator<Item = Atom> + Clone,
{
    return ground_head(
        &rule.head,
        explode_body_substitutions(knowledge_base, rule.clone().body),
    );
}

pub fn consolidate<I>(knowledge_base: I) -> I
where
    I: IntoIterator<Item = Atom> + Clone + FromIterator<Atom>,
{
    let current_dedup: HashSet<Atom> = knowledge_base.into_iter().collect();
    current_dedup.into_iter().collect()
}

pub fn evaluate_program<I>(knowledge_base: I, program: Vec<Rule>) -> I
where
    I: IntoIterator<Item = Atom> + Clone + Default + Eq + FromIterator<Atom> + Extend<Atom>,
{
    let edb = knowledge_base.clone();
    let mut previous_delta: I = I::default();
    let mut current_delta: I = I::default();
    let mut output: I = I::default();

    loop {
        previous_delta = current_delta.clone();
        current_delta = program
            .clone()
            .into_iter()
            // This is so ugly
            .flat_map(|rule| {
                evaluate_rule(
                    edb.clone()
                        .into_iter()
                        .chain(current_delta.clone().into_iter())
                        .collect::<I>(),
                    &rule,
                )
            })
            .collect();
        if previous_delta == current_delta {
            break;
        }
        output.extend(current_delta.clone())
    }

    return output;
}

pub struct ChibiDatalog<I>
where
    I: IntoIterator<Item = Atom> + Clone + Default + Eq + FromIterator<Atom> + Extend<Atom>,
{
    pub fact_store: I,
}

impl<I: IntoIterator<Item = Atom> + Clone + Default + Eq + FromIterator<Atom> + Extend<Atom>>
    BottomUpEvaluator<I> for ChibiDatalog<I>
{
    fn evaluate_program_bottom_up(&self, program: Vec<Rule>) -> I {
        return evaluate_program(self.fact_store.clone(), program);
    }
}

impl<I: IntoIterator<Item = Atom> + Clone + Default + Eq + FromIterator<Atom> + Extend<Atom>>
    Default for ChibiDatalog<I>
{
    fn default() -> Self {
        ChibiDatalog {
            fact_store: I::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::implementations::simple::{
        attempt_to_rewrite, evaluate_program, explode_body_substitutions, extend_substitutions,
        ground_atom, is_ground, make_substitutions,
    };
    use crate::models::datalog::{Atom, Rule, Sign, Substitutions, Term, TypedValue};
    use crate::parsers::datalog::{parse_atom, parse_rule};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_make_substitution() {
        let rule_atom_0 = parse_atom("edge(?X, ?Y)");
        let data_0 = parse_atom("edge(a,b)");

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
        let rule_atom_0 = parse_atom("edge(?X, ?Y)");
        let data_0 = parse_atom("edge(a,b)");

        if let Some(sub) = make_substitutions(&rule_atom_0, &data_0) {
            assert_eq!(data_0, attempt_to_rewrite(&sub, &rule_atom_0))
        } else {
            panic!()
        };
    }

    #[test]
    fn test_ground_atom() {
        let rule_atom_0 = parse_atom("edge(?X, ?Y)");
        let data_0 = parse_atom("edge(a,b)");
        let data_1 = parse_atom("edge(b,c)");

        let fact_store = vec![data_0, data_1];

        let subs = ground_atom(fact_store, &rule_atom_0);
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
        let rule_atom_0 = parse_atom("T(?X, ?Y, PLlab)");
        let data_0 = parse_atom("T(student, takesClassesFrom, PLlab)");

        assert_eq!(is_ground(&rule_atom_0), false);
        assert_eq!(is_ground(&data_0), true)
    }

    #[test]
    fn test_extend_substitutions() {
        let rule_atom_0 = parse_atom("T(?X, ?Y, PLlab)");
        let data_0 = parse_atom("T(student, takesClassesFrom, PLlab)");
        let data_1 = parse_atom("T(professor, worksAt, PLlab)");

        let fact_store = vec![data_0, data_1];

        let partial_subs = vec![
            vec![("?X".to_string(), TypedValue::Str("student".to_string()))]
                .into_iter()
                .collect::<HashMap<String, TypedValue>>(),
            vec![("?X".to_string(), TypedValue::Str("professor".to_string()))]
                .into_iter()
                .collect::<HashMap<String, TypedValue>>(),
        ];

        let subs = extend_substitutions(fact_store, &rule_atom_0, partial_subs);
        assert_eq!(
            subs,
            vec![
                vec![
                    ("?X".to_string(), TypedValue::Str("student".to_string())),
                    (
                        "?Y".to_string(),
                        TypedValue::Str("takesClassesFrom".to_string())
                    ),
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
        let rule_atom_0 = parse_atom("ancestor(?X, ?Y)");
        let rule_atom_1 = parse_atom("ancestor(?Y, ?Z)");
        let rule_body = vec![rule_atom_0, rule_atom_1];

        let data_0 = parse_atom("ancestor(adam, jumala)");
        let data_1 = parse_atom("ancestor(vanasarvik, jumala)");
        let data_2 = parse_atom("ancestor(eve, adam)");
        let data_3 = parse_atom("ancestor(jumala, cthulu)");
        let fact_store = vec![data_0, data_1, data_2, data_3];

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
        let all_substitutions = explode_body_substitutions(fact_store, rule_body);
        assert_eq!(all_substitutions, fitting_substitutions);
    }

    #[test]
    fn test_evaluate_program() {
        let rule_0 = parse_rule("ancestor(?X, ?Z) <- [ancestor(?X, ?Y), ancestor(?Y, ?Z)]");

        let data_0 = parse_atom("ancestor(adam, jumala)");
        let data_1 = parse_atom("ancestor(vanasarvik, jumala)");
        let data_2 = parse_atom("ancestor(eve, adam)");
        let data_3 = parse_atom("ancestor(jumala, cthulu)");
        let fact_store = vec![data_0, data_1, data_2, data_3];

        let evaluation: HashSet<Atom> = evaluate_program(fact_store, vec![rule_0])
            .into_iter()
            .collect();
        let expected_evaluation: HashSet<Atom> = vec![
            parse_atom("ancestor(adam, cthulu)"),
            parse_atom("ancestor(vanasarvik, cthulu)"),
            parse_atom("ancestor(eve, jumala)"),
            parse_atom("ancestor(eve, cthulu)"),
        ]
        .into_iter()
        .collect();
        assert_eq!(evaluation, expected_evaluation)
    }
}
