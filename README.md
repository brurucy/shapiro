# Shapiro

Shapiro is a reasoning framework geared towards Datalog.

This was created as a response to the fact that virtually **all** datalog reasoners in Rust are implemented as unwieldy pseudo
DSLs.

Which is a pity, macros are beautiful, but can be extremely unwieldy to use, and cannot be just "dropped in" a project.

Meanwhile, this library provides a usual sql-like interface querying interface to Datalog.

Here's an usage example with the classic ancestor example:

```rust
use std::collections::HashSet;
use shapiro::models::datalog::BottomUpEvaluator;

#[test]
fn test_chibi_datalog() {
    use shapiro::{ChibiDatalog, parsers::datalog::{parse_rule, parse_atom}, models::datalog::Atom};

    let mut reasoner: ChibiDatalog<HashSet<Atom>> = Default::default();

    reasoner.fact_store.insert(parse_atom("edge(a, b)"));
    reasoner.fact_store.insert(parse_atom("edge(b, c)"));
    reasoner.fact_store.insert(parse_atom("edge(b, d)"));

    let mut new_tuples = reasoner.evaluate_program_bottom_up(
        vec![
            parse_rule("reachable(?x, ?y) <- [edge(?x, ?y)]"),
            parse_rule("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]")]
    );

    let expected_new_tuples: HashSet<Atom> = vec![
        // Rule 1 output
        parse_atom("reachable(a, b)"),
        parse_atom("reachable(b, c)"),
        parse_atom("reachable(b, d)"),
        // Rule 2 output
        parse_atom("reachable(a, c)"),
        parse_atom("reachable(a, d)"),
    ].into_iter().collect();

    assert_eq!(new_tuples, expected_new_tuples)
}
```

Atoms are typed, with four different types for terms:

1. `String`
2. `bool`
3. `u32`
4. `f64`

And unbounded arity.