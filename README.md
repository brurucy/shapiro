# Shapiro

Shapiro is a datalog toolbox and zoo.

Here you can find, at the moment, **two** simple in-memory query engines that support materialized recursive queries.

1. [x] A datalog and relational algebra engine that relies on an ordered(not necessarily sorted) container for
   storage and sorted sets as indexes - Simple Datalog
2. [x] A engine that uses no indexes and does not rely on order - ChibiDatalog
3. [x] A sort of fast in-memory parallel and distributed datalog engine that supports adding and removing rules and data with optimal update times - DifferentialDatalog 

The following snippet showcases `ChibiDatalog` in action.

```rust
#[cfg(test)]
mod tests {
   use crate::models::reasoner::{Dynamic, Materializer, Queryable};
   use crate::models::datalog::{Atom, Rule};
   use crate::models::index::ValueRowId;
   use crate::reasoning::reasoners::chibi::ChibiDatalog;

   #[test]
   fn test_chibi_datalog() {
      // Chibi Datalog is a simple incremental datalog reasoner.
      let mut reasoner: ChibiDatalog = Default::default();
      reasoner.insert("edge", vec![Box::new(1), Box::new(2)]);
      reasoner.insert("edge", vec![Box::new(2), Box::new(3)]);
      reasoner.insert("edge", vec![Box::new(2), Box::new(4)]);
      reasoner.insert("edge", vec![Box::new(4), Box::new(5)]);

      // Queries are structured as datalog programs, collections of rules. The following query
      // has two rules, one of them dictating that every edge is reachable from itself
      // and another that establishes reachability to be transitive. Notice how this rule
      // is recursive.
      let query = vec![
         Rule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
         Rule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
      ];

      // To materialize a query is to ensure that with any updates, the query will remain correct.
      reasoner.materialize(&query);

      // The input graph looks like this:
      // 1 --> 2 --> 3
      //       |
      //         --> 4 --> 5
      // Then, the result of the query would be:
      // (1, 2)
      // (1, 3)
      // (1, 4)
      // (1, 5)
      // (2, 3)
      // (2, 4)
      // (2, 5)
      // (4, 5)
      vec![
         Atom::from("reachable(1, 2)"),
         Atom::from("reachable(1, 3)"),
         Atom::from("reachable(1, 4)"),
         Atom::from("reachable(1, 5)"),
         Atom::from("reachable(2, 3)"),
         Atom::from("reachable(2, 4)"),
         Atom::from("reachable(2, 5)"),
         Atom::from("reachable(4, 5)"),
      ]
              .iter()
              .for_each(|point_query| assert!(reasoner.contains(point_query)));

      // Now, for the incremental aspect. Let's say that we got an update to our graph, removing
      // three edges (1 --> 2), (2 --> 3), (2 --> 4), and adding two (1 --> 3), (3 --> 4):
      // 1 --> 3 --> 4 --> 5
      // Given that the query has been materialized, this update will not re-run it from scratch.
      // Instead, it will be adjusted to the new data, yielding the following:
      // (1, 3)
      // (3, 4)
      // (3, 5)
      // And retracting
      // (1, 2)
      // (2, 3)
      // (2, 4)
      // (2, 5)
      // However, this adjustment isn't differential, that is, the computation isn't
      // necessarily proportional to the size of the change, hence you should avoid updating
      // until you have a batch large enough. Empirically, batches of size 1-10% are alright.
      // Take note that this will not be a problem, at all, unless you are handling relatively
      // large amounts of data (a hundred thousand elements and above) with complex queries.
      reasoner.update(vec![
         (true, ("edge", vec![Box::new(1), Box::new(3)])),
         (true, ("edge", vec![Box::new(3), Box::new(4)])),
         (false, ("edge", vec![Box::new(1), Box::new(2)])),
         (false, ("edge", vec![Box::new(2), Box::new(3)])),
         (false, ("edge", vec![Box::new(2), Box::new(4)])),
      ]);

      vec![
         Atom::from("reachable(1, 3)"),
         Atom::from("reachable(3, 4)"),
         Atom::from("reachable(3, 5)"),
      ]
              .iter()
              .for_each(|point_query| assert!(reasoner.contains(point_query)));

      vec![
         Atom::from("reachable(1, 2)"),
         Atom::from("reachable(2, 3)"),
         Atom::from("reachable(2, 4)"),
         Atom::from("reachable(2, 5)"),
      ]
              .iter()
              .for_each(|point_query| assert!(!reasoner.contains(point_query)));
   }
}

```

In case you are interested in performance, clone the repo and just `cargo run`.

It may not be very informative, later on I will include a proper benchmark here, but in order to answer six very not-trivial
queries over hundreds of thousands of triples(the included benchmark), barely half a second passed on my M1 Pro.

### Roadmap

1. [] - Constant specialization
2. [] - Magic sets
3. [] - Negation(stratification is already implemented)