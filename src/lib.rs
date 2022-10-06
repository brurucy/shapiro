extern crate core;

pub mod implementations;
pub mod lexers;
pub mod models;
pub mod parsers;
pub mod utils;

pub use implementations::datalog_positive_infer::ChibiDatalog;

#[cfg(test)]
mod tests {
    use crate::models::datalog::{BottomUpEvaluator, Term, TypedValue};
    use std::collections::HashSet;
    use std::ops::Deref;

    #[test]
    fn test_chibi_datalog() {
        use crate::{parsers::datalog::parse_rule, ChibiDatalog};

        let mut reasoner: ChibiDatalog = Default::default();
        reasoner.fact_store.insert(
            "edge",
            vec![
                Box::new("a".to_string()),
                Box::new("b".to_string()),
            ],
        );
        reasoner.fact_store.insert(
            "edge",
            vec![
                Box::new("b".to_string()),
                Box::new("c".to_string()),
            ],
        );
        reasoner.fact_store.insert(
            "edge",
            vec![
                Box::new("b".to_string()),
                Box::new("d".to_string()),
            ],
        );

        let new_tuples: HashSet<Vec<TypedValue>> = reasoner
            .evaluate_program_bottom_up(vec![
                parse_rule("reachable(?x, ?y) <- [edge(?x, ?y)]"),
                parse_rule("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
            ])
            .view("reachable")
            .into_iter()
            .map(|boxed_slice| boxed_slice.deref().into())
            .collect();

        let expected_new_tuples: HashSet<Vec<TypedValue>> = vec![
            // Rule 1 output
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("b".to_string()),
            ],
            vec![
                TypedValue::Str("b".to_string()),
                TypedValue::Str("c".to_string()),
            ],
            vec![
                TypedValue::Str("b".to_string()),
                TypedValue::Str("d".to_string()),
            ],
            // Rule 2 output
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("c".to_string()),
            ],
            vec![
                TypedValue::Str("a".to_string()),
                TypedValue::Str("d".to_string()),
            ],
        ]
        .into_iter()
        .collect();

        assert_eq!(new_tuples, expected_new_tuples)
    }
}
