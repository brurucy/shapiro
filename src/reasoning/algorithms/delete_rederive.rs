use crate::data_structures::hashmap::IndexedHashMap;
use crate::models::reasoner::{BottomUpEvaluator, Dynamic, DynamicTyped, Flusher};
use crate::models::datalog::{Rule, Ty};
use crate::models::index::IndexBacking;

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
                    new_rule.body[idx].symbol = format!("{}{}", OVERDELETION_PREFIX, body_atom.symbol);
                    overdeletion_program.push(new_rule);
                })
        },
        );

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

pub fn delete_rederive<K, T>
(
    instance: &mut T,
    program: &Vec<Rule>,
    updates: Vec<(&str, Vec<Box<dyn Ty>>)>
) where
    K : IndexBacking,
    T : DynamicTyped + Dynamic + Flusher + BottomUpEvaluator<K>
{
    updates
        .into_iter()
        .for_each(|(symbol, update)| {
            let del_sym = format!("{}{}", OVERDELETION_PREFIX, symbol);
            instance.insert(&del_sym, update)
        });
    // Stage 1 - overdeletion
    let delete = make_overdeletion_program(program);
    let ods = instance.evaluate_program_bottom_up(delete);
    ods
        .database
        .iter()
        .for_each(|(symbol, relation)| {
            relation
                .ward
                .iter()
                .for_each(|(data, _active)| {
                    instance.insert_typed(&symbol, data.clone())
                })
        });

    // Stage 2 - rederivation
    let rederive = make_alternative_derivation_program(program);
    let alts = instance.evaluate_program_bottom_up(rederive);
    alts
        .database
        .iter()
        .for_each(|(symbol, relation)| {
            relation
                .ward
                .iter()
                .for_each(|(data, _active)| {
                    instance.insert_typed(&symbol, data.clone())
                })
        });

    // Stage 3 - diffing overdeletions from alternative derivations
    ods
        .database
        .into_iter()
        .for_each(|(del_sym, relation)| {
            let sym = del_sym.strip_prefix(OVERDELETION_PREFIX).unwrap();
            let alt_sym = format!("{}{}", REDERIVATION_PREFIX, sym);

            let mut alt = IndexedHashMap::default();
            if let Some(alternative_derivations) = alts.database.get(&alt_sym).cloned() {
                alt = alternative_derivations.ward;
            }

            relation
                .ward
                .into_iter()
                .for_each(|(row, _active)| {
                    if !alt.contains_key(&row) {
                        instance.delete_typed(sym, row.clone());
                    }
                });
            instance.flush(sym)
        });
}

#[cfg(test)]
mod tests {
    use ahash::HashSet;
    use crate::models::datalog::Rule;
    use crate::models::reasoner::{BottomUpEvaluator, Dynamic, DynamicTyped};
    use crate::reasoning::algorithms::delete_rederive::{delete_rederive, make_alternative_derivation_program, make_overdeletion_program, OVERDELETION_PREFIX, REDERIVATION_PREFIX};
    use crate::reasoning::reasoners::chibi::ChibiDatalog;

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

    // https://www.public.asu.edu/~dietrich/publications/AuthorCopyMaintenanceOfRecursiveViews.pdf
    #[test]
    fn test_delete_rederive() {
        let mut chibi = ChibiDatalog::new(true, false);

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
                chibi.insert("edge", vec![
                    Box::new(source),
                    Box::new(destination),
                ])
            });

        let program = vec![
            Rule::from("reach(?x, ?y) <- [edge(?x, ?y)]"),
            Rule::from("reach(?x, ?z) <- [reach(?x, ?y), edge(?y, ?z)]")
        ];

        let materialization = chibi.evaluate_program_bottom_up(program.clone());
        materialization
            .view("reach")
            .iter()
            .for_each(|row| {
                chibi.insert_typed("reach", row.clone())
            });
        let mat_result: HashSet<(String, String)> = materialization
            .view("reach")
            .iter()
            .map(|boxed_slice| {
                let boxed_vec = boxed_slice.to_vec();
                (boxed_vec[0].clone().try_into().unwrap(), boxed_vec[1].clone().try_into().unwrap())
            })
            .collect();

        delete_rederive(&mut chibi, &program, vec![
            ("edge", vec![Box::new("e"), Box::new("f")])
        ]);
        let rederivation_result: HashSet<(String, String)> = chibi
            .fact_store
            .view("reach")
            .iter()
            .map(|boxed_slice| {
                let boxed_vec = boxed_slice.to_vec();
                (boxed_vec[0].clone().try_into().unwrap(), boxed_vec[1].clone().try_into().unwrap())
            })
            .collect();

        let expected_deletion_1 = ("e".to_string(), "f".to_string());
        let expected_deletion_2 = ("e".to_string(), "h".to_string());
        let expected_deletion_3 = ("b".to_string(), "f".to_string());
        let expected_deletion_4 = ("b".to_string(), "h".to_string());

        assert!(mat_result.contains(&expected_deletion_1));
        assert!(!rederivation_result.contains(&expected_deletion_1));

        assert!(mat_result.contains(&expected_deletion_2));
        assert!(!rederivation_result.contains(&expected_deletion_2));

        assert!(mat_result.contains(&expected_deletion_3));
        assert!(!rederivation_result.contains(&expected_deletion_3));

        assert!(mat_result.contains(&expected_deletion_4));
        assert!(!rederivation_result.contains(&expected_deletion_4));
    }
}