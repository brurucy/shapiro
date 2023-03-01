use crate::data_structures::substitutions::Substitutions;
use crate::misc::helpers::terms_to_row;
use crate::models::datalog::{Atom, Rule, Term};
use crate::models::instance::{Database, HashSetDatabase, IndexedHashSetBacking};

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
                    if constant != *right_constant {
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
                        return Term::Constant(constant);
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

                return unify(
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
) -> Vec<Substitutions> {
    return body
        .into_iter()
        .fold(vec![Default::default()], |acc, item| {
            accumulate_substitutions(knowledge_base, item, acc)
        });
}

pub fn ground_head(head: &Atom, substitutions: Vec<Substitutions>) -> Option<IndexedHashSetBacking> {
    let mut output_instance: HashSetDatabase = Default::default();

    substitutions.into_iter().for_each(|substitutions| {
        let rewrite_attempt = attempt_to_rewrite(&substitutions, head);
        output_instance.insert_at(
            rewrite_attempt.relation_id.get(),
            terms_to_row(rewrite_attempt.terms),
        );
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

pub fn evaluate_rule(knowledge_base: &HashSetDatabase, rule: &Rule) -> Option<IndexedHashSetBacking> {
    return ground_head(
        &rule.head,
        accumulate_body_substitutions(knowledge_base, &rule.body),
    );
}
