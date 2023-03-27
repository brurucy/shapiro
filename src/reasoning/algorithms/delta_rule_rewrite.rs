use crate::models::datalog::SugaredRule;
use ahash::HashSet;
use crate::models::instance::Database;

pub const DELTA_PREFIX: &'static str = "Δ";

pub fn make_sne_programs(program: &Vec<SugaredRule>) -> (Vec<SugaredRule>, Vec<SugaredRule>) {
    let idb_relations = program
        .iter()
        .map(|rule| rule.head.symbol.clone())
        .collect::<HashSet<_>>();
    // Nonrecursive program
    let mut nonrecursive_program = vec![];
    program.iter().for_each(|rule| {
        if !rule
            .body
            .iter()
            .any(|body_atom| idb_relations.contains(&body_atom.symbol))
        {
            nonrecursive_program.push(rule.clone());
        }
    });

    // Delta program
    let mut delta_program = vec![];

    program.iter().for_each(|rule| {
        rule.body.iter().enumerate().for_each(|(idx, body_atom)| {
            if idb_relations.contains(&body_atom.symbol) {
                let mut new_rule = rule.clone();
                new_rule.body = new_rule.body;
                new_rule.body[idx].symbol = format!("{}{}", DELTA_PREFIX, body_atom.symbol);
                let delta_atom = new_rule.body[idx].clone();
                new_rule.body.remove(idx);
                new_rule.body.insert(0, delta_atom);

                delta_program.push(new_rule);
            }
        })
    });

    (nonrecursive_program, delta_program)
}

pub fn deltaify_idb(program: &Vec<SugaredRule>) -> Vec<SugaredRule> {
    let idb_relations = program
        .iter()
        .map(|rule| rule.head.clone())
        .collect::<HashSet<_>>();

    return idb_relations
        .into_iter()
        .map(|rule_head| {
            let delta_string = format!("{}{}", DELTA_PREFIX, rule_head.symbol);

            let mut delta_rule = SugaredRule::default();
            let mut delta_rule_head = rule_head.clone();
            delta_rule_head.symbol = delta_string;
            delta_rule.head = delta_rule_head;
            delta_rule.body.push(rule_head);

            return delta_rule;
        })
        .collect();
}
