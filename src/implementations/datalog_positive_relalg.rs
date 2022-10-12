use crate::models::datalog::{BottomUpEvaluator, Rule};
use crate::models::instance::Instance;
use crate::models::relational_algebra::{Map, Relation, RelationalExpression};
use rayon::prelude::*;
use crate::implementations::evaluation::{Evaluation, InstanceEvaluator};
use crate::implementations::rule_graph::{sort_program};
use crate::models::index::{IndexBacking, ValueRowId};

pub struct RelationalAlgebra {
    pub program: Vec<(String, RelationalExpression)>
}

impl RelationalAlgebra {
    fn new(program: &Vec<Rule>) -> Self {
        return RelationalAlgebra {
            program: program.iter().map(|rule| (rule.head.symbol.to_string(), RelationalExpression::from(rule))).collect()
        }
    }
}

impl<T, K> InstanceEvaluator<T, K> for RelationalAlgebra
where T : IndexBacking,
      K : Map
{
    fn evaluate(&self, instance: &Instance<T, K>) -> Vec<Relation<T, K>> {
        return self.program
            .clone()
            .into_iter()
            .filter_map(|(symbol, expression)| {
                println!("evaluating: {}", expression.to_string());
                return instance.evaluate(&expression, &symbol)
            })
            .collect();
    }
}

pub struct ParallelRelationalAlgebra {
    pub program: Vec<(String, RelationalExpression)>
}

impl ParallelRelationalAlgebra {
    fn new(program: &Vec<Rule>) -> Self {
        return ParallelRelationalAlgebra {
            program: program.iter().map(|rule| (rule.head.symbol.to_string(), RelationalExpression::from(rule))).collect()
        }
    }
}

impl<T, K> InstanceEvaluator<T, K> for ParallelRelationalAlgebra
where T : IndexBacking,
      K : Map {
    fn evaluate(&self, instance: &Instance<T, K>) -> Vec<Relation<T, K>> {
        return self.program
            .clone()
            .into_par_iter()
            .filter_map(|(symbol, expression)| {
                println!("evaluating: {}", expression.to_string());
                return instance.evaluate(&expression, &symbol)
            })
            .collect();
    }
}

pub struct SimpleDatalog<T, K>
    where T : IndexBacking,
          K : Map {
    pub fact_store: Instance<T, K>,
    parallel: bool
}

impl<T, K> Default for SimpleDatalog<T, K>
    where T : IndexBacking,
          K : Map {
    fn default() -> Self {
        SimpleDatalog {
            fact_store: Instance::new(false),
            parallel: true
        }
    }
}

impl<T, K> SimpleDatalog<T, K>
where T : IndexBacking,
      K : Map {
    pub fn new(parallel: bool) -> Self {
        return Self {
            parallel,
            ..Default::default()
        }
    }
}

impl<T, K> BottomUpEvaluator<T, K> for SimpleDatalog<T, K>
    where T : IndexBacking,
          K : Map {
    fn evaluate_program_bottom_up(&self, program: Vec<Rule>) -> Instance<T, K> {
        let mut evaluation = Evaluation::new(&self.fact_store, Box::new(RelationalAlgebra::new(&sort_program(&program))));
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRelationalAlgebra::new(&program));
        }
        evaluation.semi_naive();

        return evaluation.output
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{BottomUpEvaluator, Rule, TypedValue};
    use std::collections::{BTreeSet, HashSet};
    use std::ops::Deref;
    use crate::implementations::datalog_positive_relalg::SimpleDatalog;
    use crate::models::index::ValueRowId;

    #[test]
    fn test_simple_datalog() {
        let mut reasoner: SimpleDatalog<BTreeSet<ValueRowId>> = Default::default();
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
                Rule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
                Rule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
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

        assert_eq!(expected_new_tuples, new_tuples)
    }
}


