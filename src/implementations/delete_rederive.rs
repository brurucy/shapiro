use crate::models::datalog::{Rule, Ty, TypedValue};
use crate::models::instance::Instance;

pub fn make_overdeletion_program(program: &Vec<Rule>) -> Vec<Rule> {
    let mut overdeletion_program = vec![];

    program
        .iter()
        .for_each(|rule| {
            let new_symbol = format!("del{}", rule.head.symbol);
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
                    new_rule.body[idx].symbol = format!("del{}", new_rule.body[idx].symbol);
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
            let alt_symbol = format!("alt{}", rule.head.symbol);
            let del_symbol = format!("del{}", rule.head.symbol);

            let mut alt_rule = rule.clone();
            alt_rule.head.symbol = alt_symbol;

            let mut new_del_atom = alt_rule.head.clone();
            new_del_atom.symbol = del_symbol;

            alt_rule.body.insert(0, new_del_atom);
            alternative_derivation_program.push(alt_rule)
        });

    alternative_derivation_program
}

pub fn derive_rederive(program: &Vec<Rule>, updates: Vec<(&str, Box<[TypedValue]>)>) {
    todo!()
}