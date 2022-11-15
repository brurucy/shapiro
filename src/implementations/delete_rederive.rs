use std::fmt::format;
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use crate::data_structures::hashmap::IndexedHashMap;
use crate::implementations::evaluation::Evaluation;
use crate::models::datalog::{BottomUpEvaluator, Dynamic, Rule, Ty, TypedDynamic, TypedValue};
use crate::models::index::IndexBacking;
use crate::models::instance::Instance;

const OVERDELETION_PREFIX: &'static str = "-";
const REDERIVATION_PREFIX: &'static str = "+";

pub fn make_overdeletion_program(program: &Vec<Rule>) -> Vec<Rule> {
    let mut overdeletion_program = vec![];

    program
        .iter()
        .for_each(|rule| {
            let new_symbol = format!("{}{}", OVERDELETION_PREFIX, rule.head.symbol);
            let mut new_head = rule.head.clone();
            new_head.symbol = new_symbol;
            rule
                .body
                .iter()
                .enumerate()
                .for_each(|(idx, body_atom)| {
                    let mut new_rule = rule.clone();
                    new_rule.head = new_head.clone();
                    new_rule.body = new_rule.body;
                    new_rule.body[idx].symbol = format!("{}{}", OVERDELETION_PREFIX, new_rule.body[idx].symbol);
                    overdeletion_program.push(new_rule);
                })
        });

    overdeletion_program
}

pub fn make_alternative_derivation_program(program: &Vec<Rule>) -> Vec<Rule> {
    let mut alternative_derivation_program = vec![];

    program
        .iter()
        .for_each(|rule| {
            let alt_symbol = format!("{}{}", REDERIVATION_PREFIX, rule.head.symbol);
            let del_symbol = format!("{}{}", OVERDELETION_PREFIX, rule.head.symbol);

            let mut alt_rule = rule.clone();
            alt_rule.head.symbol = alt_symbol;

            let mut new_del_atom = alt_rule.head.clone();
            new_del_atom.symbol = del_symbol;

            alt_rule.body.insert(0, new_del_atom);
            alternative_derivation_program.push(alt_rule)
        });

    alternative_derivation_program
}

pub fn delete_rederive<K : IndexBacking, T: TypedDynamic + Dynamic<K> + BottomUpEvaluator<K> + Clone>(instance: T, program: &Vec<Rule>, updates: Vec<(&str, Vec<Box<dyn Ty>>)>) -> Instance<K> {
    let mut temp_instance = instance.clone();
    let mut touched_relations = HashSet::new();
    updates
        .into_iter()
        .for_each(|(symbol, update)| {
            touched_relations.insert(symbol);
            let new_symbol = format!("{}{}", OVERDELETION_PREFIX, symbol);
            temp_instance
                .insert(&new_symbol, update)
        });
    // Stage 1 - overdeletion
    let delete = make_overdeletion_program(program);
    let ods = temp_instance.evaluate_program_bottom_up(delete);
    ods
        .database
        .into_iter()
        .for_each(|(symbol, relation)| {
            relation
                .ward
                .into_iter()
                .for_each(|(data, active)| {
                    temp_instance.insert_typed(&symbol, data)
                })
        });

    // Stage 2 - rederivation
    let rederive = make_alternative_derivation_program(program);
    let alts = temp_instance.evaluate_program_bottom_up(rederive);
    alts
        .database
        .into_iter()
        .for_each(|(symbol, relation)| {
            relation
                .ward
                .into_iter()
                .for_each(|(data, active)| {
                    temp_instance.insert_typed(&symbol, data)
                })
        });

    // Stage 3 - diffing
    let ti = temp_instance.get_instance();
    let mut output_instance = Instance::new(false);
    touched_relations
        .into_iter()
        .for_each(|rel| {
            let del_sym = format!("{}{}", OVERDELETION_PREFIX, rel);
            let alt_sym = format!("{}{}", REDERIVATION_PREFIX, rel);
            let del_rel = ti.database.get(&del_sym);
            let alt_rel = ti.database.get(&alt_sym);

            if let Some(overdeletions) = del_rel {
                let mut alt = IndexedHashMap::new();
                if let Some(alternative_derivations) = alt_rel {
                    alt = alternative_derivations.ward.clone()
                }
                overdeletions
                    .ward
                    .iter()
                    .for_each(|(row, _active)| {
                        if !alt.contains_key(row) {
                            output_instance.insert_typed(rel, row.clone())
                        }
                    })
            }
        });
    
    return output_instance
}

#[cfg(test)]
mod tests {
    use crate::implementations::delete_rederive::{make_alternative_derivation_program, make_overdeletion_program, OVERDELETION_PREFIX, REDERIVATION_PREFIX};
    use crate::models::datalog::Rule;

    #[test]
    fn test_make_overdeletion_program() {
        let program = vec![
            Rule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            Rule::from("reach(?x, ?z) <- [reach(?x, ?y), edge(?y, ?z)]")
        ];

        let actual_overdeletion_program = make_overdeletion_program(&program);
        let exp_overdeletion_program = vec![
            Rule::from(&*format!("{}reach(?x, ?y) <- [{}edge(?x, ?y)]", OVERDELETION_PREFIX, OVERDELETION_PREFIX)),
            Rule::from(&*format!("{}reach(?x, ?z) <- [{}reach(?x, ?y), edge(?y, ?z)]", OVERDELETION_PREFIX, OVERDELETION_PREFIX)),
            Rule::from(&*format!("{}reach(?x, ?z) <- [reach(?x, ?y), {}edge(?y, ?z)]", OVERDELETION_PREFIX, OVERDELETION_PREFIX))
        ];

        assert_eq!(exp_overdeletion_program, actual_overdeletion_program)
    }

    #[test]
    fn test_make_alternative_derivation_program() {
        let program = vec![
            Rule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            Rule::from("reach(?x, ?z) <- [reach(?x, ?y), edge(?y, ?z)]")
        ];

        let actual_alt_program = make_alternative_derivation_program(&program);
        let exp_alt_program = vec![
            Rule::from(&*format!("{}reach(?x, ?y) <- [{}reach(?x, ?y), edge(?x, ?y)]", REDERIVATION_PREFIX, OVERDELETION_PREFIX)),
            Rule::from(&*format!("{}reach(?x, ?z) <- [{}reach(?x, ?z), reach(?x, ?y), edge(?y, ?z)]", REDERIVATION_PREFIX, OVERDELETION_PREFIX)),
        ];

        assert_eq!(exp_alt_program, actual_alt_program)
    }
}