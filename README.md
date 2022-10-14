# Shapiro

Shapiro is a datalog toolbox and zoo.

Here you can find:

1. [x] A very fast in-memory parallel datalog and relational algebra engine that relies on an ordered(not necessarily sorted) 
   container for storage and sorted sets as indexes - Simple Datalog
2. [x] A not-so-fast in-memory parallel datalog engine that uses no indexes and does not rely on order - ChibiDatalog

With more to come.

Here's an example with the classic ancestor query:

```rust
#[cfg(test)]
mod tests {
    use crate::models::datalog::{BottomUpEvaluator, Rule};
    use std::collections::HashSet;

    #[test]
    fn test_chibi_datalog() {
        use crate::ChibiDatalog;

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

```

In case you are interested in performance, clone the repo and just `cargo run`.

It may not be very informative, later on I will include a proper benchmark here, but in order to answer six very not-trivial
queries over hundreds of thousands of triples(the included benchmark), barely half a second passed on my M1 Pro.

### Roadmap

0. [] Using `DashMap` instead of `indexmap`
1. [] Streaming implementation with `timely`
2. [] Negation(stratification is already implemented)
3. [] Head Aggregations
4. [] Body functions
5. [] Head Skolemization
6. [] Program Linearization
7. [] Multiple heads for one body