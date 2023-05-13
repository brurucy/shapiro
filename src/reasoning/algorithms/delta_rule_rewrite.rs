use crate::models::datalog::SugaredRule;
use ahash::HashSet;

pub const DELTA_PREFIX: &'static str = "Δ";

// Suitable for initial materialization only.
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
// For small updates.
pub fn make_update_sne_programs(program: &Vec<SugaredRule>) -> (Vec<SugaredRule>, Vec<SugaredRule>) {
    let idb_relations = program
        .iter()
        .map(|rule| rule.head.symbol.clone())
        .collect::<HashSet<_>>();
    // Delta Nonrecursive program
    let mut delta_nonrecursive_program = vec![];
    program.iter().for_each(|rule| {
        if !rule
            .body
            .iter()
            .any(|body_atom| idb_relations.contains(&body_atom.symbol))
        {
            rule.body.iter().enumerate().for_each(|(idx, body_atom)| {
                let mut new_rule = rule.clone();
                new_rule.body = new_rule.body;
                new_rule.body[idx].symbol = format!("{}{}", DELTA_PREFIX, body_atom.symbol);

                let delta_atom = new_rule.body[idx].clone();
                new_rule.body.remove(idx);
                new_rule.body.insert(0, delta_atom);

                delta_nonrecursive_program.push(rule.clone());
            });
        }
    });
    // Delta program
    let mut delta_program = vec![];
    program.iter().for_each(|rule| {
        rule.body.iter().enumerate().for_each(|(idb_body_atom_idx, body_atom)| {
            if idb_relations.contains(&body_atom.symbol) {
                let mut new_rule = rule.clone();
                new_rule.body = new_rule.body;
                new_rule.body[idb_body_atom_idx].symbol = format!("{}{}", DELTA_PREFIX, body_atom.symbol);

                let delta_atom = new_rule.body[idb_body_atom_idx].clone();
                new_rule.body.remove(idb_body_atom_idx);
                new_rule.body.insert(0, delta_atom);

                let edb_body_atoms: Vec<_> = new_rule
                    .body
                    .iter()
                    .enumerate()
                    .filter(|(_position, new_rule_body_atom)| !idb_relations.contains(&body_atom.symbol))
                    .collect();

                if edb_body_atoms.len() > 0 {
                    edb_body_atoms
                        .iter()
                        .for_each(|(edb_body_atom_idx, new_rule_body_atom)| {
                            let mut delta_edb_atom = new_rule_body_atom.clone().clone();
                            delta_edb_atom.symbol = format!("{}{}", DELTA_PREFIX, body_atom.symbol);

                            let mut new_edb_rule = new_rule.clone();
                            new_edb_rule.body.remove(*edb_body_atom_idx);
                            new_edb_rule.body.insert(0, delta_edb_atom);

                            delta_program.push(new_edb_rule);
                        });
                } else {
                    delta_program.push(new_rule);
                }
            }
        })
    });

    (delta_nonrecursive_program, delta_program)
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
