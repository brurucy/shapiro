extern crate core;

pub mod implementations;
pub mod lexers;
pub mod models;
pub mod parsers;

pub use implementations::simple::ChibiDatalog;

mod test {
    use crate::models::datalog::BottomUpEvaluator;
    use std::collections::HashSet;

    #[test]
    fn test_chibi_datalog() {
        use crate::{
            models::datalog::Atom,
            parsers::datalog::{parse_atom, parse_rule},
            ChibiDatalog,
        };

        let mut reasoner: ChibiDatalog<HashSet<Atom>> = Default::default();

        reasoner.fact_store.insert(parse_atom("edge(a, b)"));
        reasoner.fact_store.insert(parse_atom("edge(b, c)"));
        reasoner.fact_store.insert(parse_atom("edge(b, d)"));

        let mut new_tuples = reasoner.evaluate_program_bottom_up(vec![
            parse_rule("reachable(?x, ?y) <- [edge(?x, ?y)]"),
            parse_rule("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
        ]);

        let expected_new_tuples: HashSet<Atom> = vec![
            // Rule 1 output
            parse_atom("reachable(a, b)"),
            parse_atom("reachable(b, c)"),
            parse_atom("reachable(b, d)"),
            // Rule 2 output
            parse_atom("reachable(a, c)"),
            parse_atom("reachable(a, d)"),
        ]
        .into_iter()
        .collect();

        assert_eq!(new_tuples, expected_new_tuples)
    }
}
