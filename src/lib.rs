extern crate core;

pub mod implementations;
pub mod lexers;
pub mod models;
pub mod parsers;
pub mod data_structures;

pub use implementations::datalog_positive_infer::ChibiDatalog;

#[cfg(test)]
mod tests {
    use crate::models::datalog::{BottomUpEvaluator, Rule};
    use std::collections::{HashSet};

    #[test]
    fn test_chibi_datalog() {
        use crate::ChibiDatalog;

        // Chibi Datalog is a very simple reasoner, that supports only positive datalog queries
        // with no negation, aggregates and else.
        let mut reasoner: ChibiDatalog = Default::default();
        // Atoms are of arbitrary arity
        reasoner.fact_store.insert("edge", vec![Box::new(1), Box::new(2)]);
        reasoner.fact_store.insert("edge", vec![Box::new(2), Box::new(3)]);
        reasoner.fact_store.insert("edge", vec![Box::new(2), Box::new(4)]);

        let new_tuples: HashSet<(u32, u32)> = reasoner
            .evaluate_program_bottom_up(vec![
                Rule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
                Rule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
            ])
            .view("reachable")
            .into_iter()
            .map(|boxed_slice| {
                // The output is boxed, so there's some wrangling to do
                let boxed_vec = boxed_slice.to_vec();
                (boxed_vec[0].clone().try_into().unwrap(), boxed_vec[1].clone().try_into().unwrap())
            })
            .collect();

        let expected_new_tuples: HashSet<(u32, u32)> = vec![
            // Rule 1 output
            (1, 2),
            (2, 3),
            (2, 4),
            // Rule 2 output
            (1, 3),
            (1, 4)
        ]
        .into_iter()
        .collect();

        assert_eq!(new_tuples, expected_new_tuples)
    }
}
