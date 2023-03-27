use crate::models::datalog::SugaredRule;
use crate::models::reasoner::{BottomUpEvaluator, Dynamic, DynamicTyped, RelationDropper};
use crate::models::relational_algebra::Row;
use ahash::{HashSet, HashSetExt};
use std::time::Instant;

const OVERDELETION_PREFIX: &'static str = "-";
const REDERIVATION_PREFIX: &'static str = "+";

pub fn make_overdeletion_program(program: &Vec<SugaredRule>) -> Vec<SugaredRule> {
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

pub fn make_alternative_derivation_program(program: &Vec<SugaredRule>) -> Vec<SugaredRule> {
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

pub type TypedDiff<'a> = (&'a str, Row);

pub fn delete_rederive<'a, T>(
    instance: &mut T,
    program: &'a Vec<SugaredRule>,
    deletions: Vec<TypedDiff<'a>>,
) where
    T: DynamicTyped + Dynamic + BottomUpEvaluator + RelationDropper,
{
    let mut relations_to_be_dropped: HashSet<String> = HashSet::new();
    deletions.iter().for_each(|(sym, deletion)| {
        let del_sym = format!("{}{}", OVERDELETION_PREFIX, sym);
        instance.insert_typed(&del_sym, deletion.clone());
        instance.delete_typed(sym, &deletion);
        relations_to_be_dropped.insert(del_sym);
    });
    // Overdeletion and Rederivation programs
    let overdeletion_program = make_overdeletion_program(program);
    let rederivation_program = make_alternative_derivation_program(program);
    // Stage 1 - intensional overdeletion
    let overdeletions = instance.evaluate_program_bottom_up(&overdeletion_program);

    overdeletions.into_iter().for_each(|(del_sym, row_set)| {
        let sym = del_sym.strip_prefix(OVERDELETION_PREFIX).unwrap();
        row_set.into_iter().for_each(|overdeletion| {
            instance.delete_typed(sym, &overdeletion);
            instance.insert_typed(&del_sym, overdeletion);
        });
        relations_to_be_dropped.insert(del_sym);
    });

    //Stage 2 - intensional rederivation
    let rederivations = instance.evaluate_program_bottom_up(&rederivation_program);

    rederivations.into_iter().for_each(|(alt_sym, row_set)| {
        let sym = alt_sym.strip_prefix(REDERIVATION_PREFIX).unwrap();
        row_set.into_iter().for_each(|row| {
            instance.insert_typed(&sym, row);
        })
    });

    relations_to_be_dropped.into_iter().for_each(|del_sym| {
        instance.drop_relation(&del_sym);
    });
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{SugaredRule, Ty, TypedValue};
    use crate::models::index::VecIndex;
    use crate::models::reasoner::{
        BottomUpEvaluator, Dynamic, DynamicTyped, Materializer, Queryable,
    };
    use crate::models::relational_algebra::Row;
    use crate::reasoning::algorithms::delete_rederive::{
        delete_rederive, make_alternative_derivation_program, make_overdeletion_program,
        OVERDELETION_PREFIX, REDERIVATION_PREFIX,
    };
    use crate::reasoning::reasoners::chibi::ChibiDatalog;
    use crate::reasoning::reasoners::relational::RelationalDatalog;
    use indexmap::IndexSet;
    use rand::distributions::uniform::SampleBorrow;

    #[test]
    fn test_make_overdeletion_program() {
        let program = vec![
            SugaredRule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            SugaredRule::from("reach(?x, ?z) <- [reach(?x, ?y), edge(?y, ?z)]"),
        ];

        let actual_overdeletion_program = make_overdeletion_program(&program);

        let exp_overdeletion_program = vec![
            SugaredRule::from(&*format!(
                "{}reach(?x, ?y) <- [{}edge(?x, ?y)]",
                OVERDELETION_PREFIX, OVERDELETION_PREFIX
            )),
            SugaredRule::from(&*format!(
                "{}reach(?x, ?z) <- [{}reach(?x, ?y), edge(?y, ?z)]",
                OVERDELETION_PREFIX, OVERDELETION_PREFIX
            )),
            SugaredRule::from(&*format!(
                "{}reach(?x, ?z) <- [reach(?x, ?y), {}edge(?y, ?z)]",
                OVERDELETION_PREFIX, OVERDELETION_PREFIX
            )),
        ];

        assert_eq!(exp_overdeletion_program, actual_overdeletion_program)
    }

    #[test]
    fn test_make_alternative_derivation_program() {
        let program = vec![
            SugaredRule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            SugaredRule::from("reach(?x, ?z) <- [reach(?x, ?y), edge(?y, ?z)]"),
        ];

        let actual_alt_program = make_alternative_derivation_program(&program);
        let exp_alt_program = vec![
            SugaredRule::from(&*format!(
                "{}reach(?x, ?y) <- [{}reach(?x, ?y), edge(?x, ?y)]",
                REDERIVATION_PREFIX, OVERDELETION_PREFIX
            )),
            SugaredRule::from(&*format!(
                "{}reach(?x, ?z) <- [{}reach(?x, ?z), reach(?x, ?y), edge(?y, ?z)]",
                REDERIVATION_PREFIX, OVERDELETION_PREFIX
            )),
        ];

        assert_eq!(exp_alt_program, actual_alt_program)
    }

    // https://www.public.asu.edu/~dietrich/publications/AuthorCopyMaintenanceOfRecursiveViews.pdf
    #[test]
    fn test_delete_rederive_logic() {
        let mut chibi = RelationalDatalog::<VecIndex>::new(false, false);

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
            SugaredRule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            SugaredRule::from("reach(?x, ?z) <- [edge(?x, ?y), reach(?y, ?z)]"),
        ];

        chibi.materialize(&program);

        // Overdeletions
        let overdeletion_program = make_overdeletion_program(&program);

        chibi.delete("edge", &vec![Box::new("e"), Box::new("f")]);
        chibi.insert("-edge", vec![Box::new("e"), Box::new("f")]);

        let actual_overdeletions = chibi.evaluate_program_bottom_up(&overdeletion_program);
        let expected_overdeletions = vec![
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("h".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("b".to_string()),
                TypedValue::Str("g".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("b".to_string()),
                TypedValue::Str("h".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("e".to_string()),
                TypedValue::Str("f".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("e".to_string()),
                TypedValue::Str("g".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("e".to_string()),
                TypedValue::Str("h".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("f".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("b".to_string()),
                TypedValue::Str("f".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("g".to_string()),
            ]
            .into_boxed_slice(),
        ]
        .into_iter()
        .collect::<IndexSet<Row>>();

        assert_eq!(
            expected_overdeletions,
            actual_overdeletions.get("-reach").unwrap().clone()
        );

        actual_overdeletions
            .get("-reach")
            .unwrap()
            .into_iter()
            .for_each(|overdeletion| {
                chibi.insert_typed("-reach", overdeletion.clone());
                chibi.delete_typed("reach", overdeletion);
            });

        let rederivation_program = make_alternative_derivation_program(&program);
        let actual_rederivations = chibi.evaluate_program_bottom_up(&rederivation_program);
        let expected_rederivations = vec![
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("h".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("b".to_string()),
                TypedValue::Str("g".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("e".to_string()),
                TypedValue::Str("g".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("f".to_string()),
            ]
            .into_boxed_slice(),
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("g".to_string()),
            ]
            .into_boxed_slice(),
        ]
        .into_iter()
        .collect::<IndexSet<Row>>();

        assert_eq!(
            expected_rederivations,
            actual_rederivations.get("+reach").unwrap().clone()
        );
    }

    // https://www.public.asu.edu/~dietrich/publications/AuthorCopyMaintenanceOfRecursiveViews.pdf
    #[test]
    fn test_delete_rederive() {
        let mut chibi = RelationalDatalog::<VecIndex>::new(false, false);

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
            SugaredRule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            SugaredRule::from("reach(?x, ?z) <- [edge(?x, ?y), reach(?y, ?z)]"),
        ];

        chibi.materialize(&program);

        let expected_deletion_1: Vec<Box<dyn Ty>> = vec![Box::new("e"), Box::new("f")];
        let expected_deletion_2: Vec<Box<dyn Ty>> = vec![Box::new("e"), Box::new("h")];
        let expected_deletion_3: Vec<Box<dyn Ty>> = vec![Box::new("b"), Box::new("f")];
        let expected_deletion_4: Vec<Box<dyn Ty>> = vec![Box::new("b"), Box::new("h")];

        assert!(chibi.contains_row("reach", &expected_deletion_1));
        assert!(chibi.contains_row("reach", &expected_deletion_2));
        assert!(chibi.contains_row("reach", &expected_deletion_3));
        assert!(chibi.contains_row("reach", &expected_deletion_4));

        // chibi
        //     .fact_store
        //     .storage
        //     .iter()
        //     .for_each(|(relation_id, values)| {
        //         values
        //             .iter()
        //             .for_each(|row| println!("{}:{:?}", relation_id, row.clone()))
        //     });

        delete_rederive(
            &mut chibi,
            &program,
            vec![(
                "edge",
                Box::new(["e".to_typed_value(), "f".to_typed_value()]),
            )],
        );

        //println!("post delete rederive");

        // chibi
        //     .fact_store
        //     .storage
        //     .iter()
        //     .for_each(|(relation_id, values)| {
        //         values
        //             .iter()
        //             .for_each(|row| println!("{}:{:?}", relation_id, row.clone()))
        //     });

        assert!(!chibi.contains_row("reach", &expected_deletion_1));
        assert!(!chibi.contains_row("reach", &expected_deletion_2));
        assert!(!chibi.contains_row("reach", &expected_deletion_3));
        assert!(!chibi.contains_row("reach", &expected_deletion_4));
    }
}
