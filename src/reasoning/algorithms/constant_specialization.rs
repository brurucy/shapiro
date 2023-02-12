// This technique merely tries to emulate the relational operation "constant pushdown", by creating
// sub-relations from all constants in each atom.

use ahash::HashSet;
use crate::models::datalog::{SugaredAtom, SugaredProgram, SugaredRule, Term};

fn get_constants(sugared_atom: &SugaredAtom) -> Vec<(usize, Term)> {
    sugared_atom
        .terms
        .iter()
        .enumerate()
        .filter_map(|(position, term)| {
            match term {
                Term::Variable(_) => None,
                constant => Some((position, constant.clone()))
            }
        })
        .collect()
}

fn specialize_atom(sugared_atom: &SugaredAtom) -> Option<(SugaredAtom, SugaredRule)> {
    let constants = get_constants(sugared_atom);
    let mut specialized_sugared_atom = sugared_atom.clone();

    if !constants.is_empty() && constants.len() != sugared_atom.terms.len() {
        let mut new_symbol = sugared_atom.symbol.to_string();

        constants
            .into_iter()
            .for_each(|(position, constant)| {
                new_symbol = format!("{}_{}", new_symbol, constant);
                specialized_sugared_atom.terms.remove(position);
            });

        specialized_sugared_atom.symbol = new_symbol;

        let selection_rule = SugaredRule {
            head: specialized_sugared_atom.clone(),
            body: vec![sugared_atom.clone()],
        };

        return Some((specialized_sugared_atom, selection_rule))
    }

    return None
}

pub fn specialize_to_constants(sugared_program: &SugaredProgram) -> SugaredProgram {
    let mut specialized_program: HashSet<_> = Default::default();

    sugared_program
        .iter()
        .for_each(|sugared_rule| {
            let mut new_rule = sugared_rule.clone();

            if let Some((specialized_head, selection_rule)) = specialize_atom(&sugared_rule.head) {
                new_rule.head = specialized_head;
                specialized_program.insert(selection_rule);
            }

            sugared_rule
                .body
                .iter()
                .enumerate()
                .for_each(|(body_atom_position, body_atom)| {
                    if let Some((specialized_body_atom, selection_rule)) = specialize_atom(body_atom) {
                        new_rule.body[body_atom_position] = specialized_body_atom;
                        specialized_program.insert(selection_rule);
                    }
                });

            specialized_program.insert(new_rule);
        });

    return specialized_program.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use ahash::HashSet;
    use crate::models::datalog::{SugaredAtom, SugaredRule};
    use crate::reasoning::algorithms::constant_specialization::{specialize_atom, specialize_to_constants};

    #[test]
    fn test_specialize_atom() {
        let input_atom = SugaredAtom::from("T(?y, rdf:type, ?x)");

        let expected_specialized_atom = SugaredAtom::from("T_rdf:type(?y, ?x)");
        let expected_selection_rule = SugaredRule::from("T_rdf:type(?y, ?x) <- [T(?y, rdf:type, ?x)]");

        let (actual_specialized_atom, actual_selection_rule) = specialize_atom(&input_atom).unwrap();

        assert_eq!(expected_specialized_atom, actual_specialized_atom);
        assert_eq!(expected_selection_rule, actual_selection_rule);
    }

    #[test]
    fn test_specialize_to_constants() {
        let input_program = vec![
            SugaredRule::from("T(?y, rdf:type, ?x) <- [T(?a, rdfs:domain, ?x), T(?y, ?a, ?z)]"),
            SugaredRule::from("T(?z, rdf:type, ?x) <- [T(?a, rdfs:range, ?x), T(?y, ?a, ?z)]"),
            SugaredRule::from("T(?x, rdfs:subPropertyOf, ?z) <- [T(?x, rdfs:subPropertyOf, ?y), T(?y, rdfs:subPropertyOf, ?z)]"),
            SugaredRule::from("T(?x, rdfs:subClassOf, ?z) <- [T(?x, rdfs:subClassOf, ?y), T(?y, rdfs:subClassOf, ?z)]"),
            SugaredRule::from("T(?z, rdf:type, ?y) <- [T(?x, rdfs:subClassOf, ?y), T(?z, rdf:type, ?x)]"),
            SugaredRule::from("T(?x, ?b, ?y) <- [T(?a, rdfs:subPropertyOf, ?b), T(?x, ?a, ?y)]"),
        ];

        let expected_output_program: HashSet<_> = vec![
            SugaredRule::from("T_rdfs:domain(?a, ?x) <- [T(?a, rdfs:domain, ?x)]"),
            SugaredRule::from("T_rdfs:range(?a, ?x) <- [T(?a, rdfs:range, ?x)]"),
            SugaredRule::from("T_rdf:type(?y, ?x) <- [T(?y, rdf:type, ?x)]"),
            SugaredRule::from("T_rdfs:subPropertyOf(?x, ?z) <- [T(?x, rdfs:subPropertyOf, ?z)]"),
            SugaredRule::from("T_rdfs:subClassOf(?x, ?z) <- [T(?x, rdfs:subClassOf, ?z)]"),

            SugaredRule::from("T_rdf:type(?y, ?x) <- [T_rdfs:domain(?a, ?x), T(?y, ?a, ?z)]"),
            SugaredRule::from("T_rdf:type(?z, ?x) <- [T_rdfs:range(?a, ?x), T(?y, ?a, ?z)]"),
            SugaredRule::from("T_rdfs:subPropertyOf(?x, ?z) <- [T_rdfs:subPropertyOf(?x, ?y), T_rdfs:subPropertyOf(?y, ?z)]"),
            SugaredRule::from("T_rdfs:subClassOf(?x, ?z) <- [T_rdfs:subClassOf(?x, ?y), T_rdfs:subClassOf(?y, ?z)]"),
            SugaredRule::from("T_rdf:type(?z, ?y) <- [T_rdfs:subClassOf(?x, ?y), T_rdf:type(?z, ?x)]"),
            SugaredRule::from("T(?x, ?b, ?y) <- [T_rdfs:subPropertyOf(?a, ?b), T(?x, ?a, ?y)]"),
        ].into_iter().collect();

        let actual_output_program = specialize_to_constants(&input_program).into_iter().collect::<HashSet<_>>();

        expected_output_program
            .iter()
            .for_each(|rule| println!("{}", rule));

        println!("");

        actual_output_program
            .iter()
            .for_each(|rule| println!("{}", rule));

        assert_eq!(expected_output_program, actual_output_program)
    }
}