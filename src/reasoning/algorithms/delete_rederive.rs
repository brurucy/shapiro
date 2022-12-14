use crate::models::datalog::Rule;
use crate::models::index::IndexBacking;
use crate::models::reasoner::{BottomUpEvaluator, Dynamic, DynamicTyped, Flusher, RelationDropper};
use crate::models::relational_algebra::Row;
use ahash::{HashSet, HashSetExt};

const OVERDELETION_PREFIX: &'static str = "-";
const REDERIVATION_PREFIX: &'static str = "+";

pub fn make_overdeletion_program(program: &Vec<Rule>) -> Vec<Rule> {
    let mut overdeletion_program = vec![];

    program.iter().for_each(|rule| {
        let new_symbol = format!("{}{}", OVERDELETION_PREFIX, rule.head.symbol);
        let mut new_head = rule.head.clone();
        new_head.symbol = new_symbol;
        rule.body.iter().enumerate().for_each(|(idx, body_atom)| {
            let mut new_rule = rule.clone();
            new_rule.head = new_head.clone();
            new_rule.body = new_rule.body;
            new_rule.body[idx].symbol = format!("{}{}", OVERDELETION_PREFIX, body_atom.symbol);
            overdeletion_program.push(new_rule);
        })
    });

    overdeletion_program
}

pub fn make_alternative_derivation_program(program: &Vec<Rule>) -> Vec<Rule> {
    let mut alternative_derivation_program = vec![];

    program.iter().for_each(|rule| {
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

pub fn delete_rederive<K, T>(instance: &mut T, program: &Vec<Rule>, updates: Vec<(&str, Row)>)
where
    K: IndexBacking,
    T: DynamicTyped + Dynamic + Flusher + BottomUpEvaluator<K> + RelationDropper,
{
    let mut relations_to_be_flushed: HashSet<String> = HashSet::new();
    let mut relations_to_be_dropped: HashSet<String> = HashSet::new();
    updates.clone().into_iter().for_each(|(symbol, update)| {
        let del_sym = format!("{}{}", OVERDELETION_PREFIX, symbol);
        instance.insert_typed(&del_sym, update);
        relations_to_be_dropped.insert(del_sym.to_string());
        relations_to_be_flushed.insert(symbol.to_string());
    });
    // Stage 1 - overdeletion
    let delete = make_overdeletion_program(program);
    let ods = instance.evaluate_program_bottom_up(delete);
    ods.database.iter().for_each(|(del_sym, relation)| {
        let sym = del_sym.strip_prefix(OVERDELETION_PREFIX).unwrap();
        relation.ward.iter().for_each(|(data, _active)| {
            instance.insert_typed(&del_sym, data.clone());
            relations_to_be_dropped.insert(del_sym.to_string());
            instance.delete_typed(sym, data.clone());
            relations_to_be_flushed.insert(sym.to_string());
        });
    });

    updates
        .iter()
        .for_each(|(sym, row)| instance.delete_typed(sym, row.clone()));

    relations_to_be_flushed.iter().for_each(|sym| {
        instance.flush(sym);
    });

    // Stage 2 - rederivation
    let rederive = make_alternative_derivation_program(program);

    let alts = instance.evaluate_program_bottom_up(rederive);
    alts.database.iter().for_each(|(alt_sym, relation)| {
        let sym = alt_sym.strip_prefix(REDERIVATION_PREFIX).unwrap();
        relation.ward.iter().for_each(|(data, _active)| {
            instance.insert_typed(&sym, data.clone());
        });
    });

    relations_to_be_dropped.iter().for_each(|del_sym| {
        instance.drop_relation(del_sym);
    })
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{Atom, Rule, Ty};
    use crate::models::reasoner::{Dynamic, Materializer, Queryable};
    use crate::reasoning::algorithms::delete_rederive::{
        delete_rederive, make_alternative_derivation_program, make_overdeletion_program,
        OVERDELETION_PREFIX, REDERIVATION_PREFIX,
    };
    use crate::reasoning::reasoners::chibi::ChibiDatalog;

    #[test]
    fn test_make_overdeletion_program() {
        let program = vec![
            Rule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            Rule::from("reach(?x, ?z) <- [reach(?x, ?y), edge(?y, ?z)]"),
        ];

        let actual_overdeletion_program = make_overdeletion_program(&program);

        let exp_overdeletion_program = vec![
            Rule::from(&*format!(
                "{}reach(?x, ?y) <- [{}edge(?x, ?y)]",
                OVERDELETION_PREFIX, OVERDELETION_PREFIX
            )),
            Rule::from(&*format!(
                "{}reach(?x, ?z) <- [{}reach(?x, ?y), edge(?y, ?z)]",
                OVERDELETION_PREFIX, OVERDELETION_PREFIX
            )),
            Rule::from(&*format!(
                "{}reach(?x, ?z) <- [reach(?x, ?y), {}edge(?y, ?z)]",
                OVERDELETION_PREFIX, OVERDELETION_PREFIX
            )),
        ];

        assert_eq!(exp_overdeletion_program, actual_overdeletion_program)
    }

    #[test]
    fn test_make_alternative_derivation_program() {
        let program = vec![
            Rule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            Rule::from("reach(?x, ?z) <- [reach(?x, ?y), edge(?y, ?z)]"),
        ];

        let actual_alt_program = make_alternative_derivation_program(&program);
        let exp_alt_program = vec![
            Rule::from(&*format!(
                "{}reach(?x, ?y) <- [{}reach(?x, ?y), edge(?x, ?y)]",
                REDERIVATION_PREFIX, OVERDELETION_PREFIX
            )),
            Rule::from(&*format!(
                "{}reach(?x, ?z) <- [{}reach(?x, ?z), reach(?x, ?y), edge(?y, ?z)]",
                REDERIVATION_PREFIX, OVERDELETION_PREFIX
            )),
        ];

        assert_eq!(exp_alt_program, actual_alt_program)
    }

    // https://www.public.asu.edu/~dietrich/publications/AuthorCopyMaintenanceOfRecursiveViews.pdf
    #[test]
    fn test_delete_rederive() {
        let mut chibi: ChibiDatalog = Default::default();

        vec![
            ("a", "b"),
            ("a", "c"),
            ("b", "d"),
            ("b", "e"),
            ("d", "g"),
            ("c", "f"),
            ("e", "d"),
            ("e", "f"),
            ("f", "g"),
            ("f", "h"),
        ]
        .into_iter()
        .for_each(|(source, destination)| {
            chibi.insert("edge", vec![Box::new(source), Box::new(destination)])
        });

        let program = vec![
            Rule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            Rule::from("reach(?x, ?z) <- [reach(?x, ?y), edge(?y, ?z)]"),
        ];

        chibi.materialize(&program);

        let expected_deletion_1 = Atom::from("reach(e, f)");
        let expected_deletion_2 = Atom::from("reach(e, h)");
        let expected_deletion_3 = Atom::from("reach(b, f)");
        let expected_deletion_4 = Atom::from("reach(b, h)");

        assert!(chibi.contains(&expected_deletion_1));
        assert!(chibi.contains(&expected_deletion_2));
        assert!(chibi.contains(&expected_deletion_3));
        assert!(chibi.contains(&expected_deletion_4));

        delete_rederive(
            &mut chibi,
            &program,
            vec![(
                "edge",
                Box::new(["e".to_typed_value(), "f".to_typed_value()]),
            )],
        );

        assert!(!chibi.contains(&expected_deletion_1));
        assert!(!chibi.contains(&expected_deletion_2));
        assert!(!chibi.contains(&expected_deletion_3));
        assert!(!chibi.contains(&expected_deletion_4));
    }
}
