use rayon::prelude::IntoParallelIterator;
use crate::implementations::evaluation::{Evaluation, InstanceEvaluator};
use crate::implementations::interning::Interner;
use crate::implementations::rule_graph::sort_program;
use crate::models::datalog::{Atom, Rule, Ty, Term};
use crate::models::datalog::Sign::Positive;
use crate::models::index::IndexBacking;
use crate::models::instance::Instance;
use crate::models::reasoner::BottomUpEvaluator;
use crate::models::relational_algebra::{Relation, RelationalExpression};

pub struct RuleToRelationalExpressionConverter {
    pub program: Vec<(String, RelationalExpression)>
}

impl RuleToRelationalExpressionConverter {
    fn new(program: &Vec<Rule>) -> Self {
        return RuleToRelationalExpressionConverter {
            program: program.iter().map(|rule| (rule.head.symbol.to_string(), RelationalExpression::from(rule))).collect()
        }
    }
}

impl<T> InstanceEvaluator<T> for RuleToRelationalExpressionConverter
    where T : IndexBacking,
{
    fn evaluate(&self, instance: &Instance<T>) -> Vec<Relation<T>> {
        return self.program
            .clone()
            .into_iter()
            .fold(Instance::new(false), |mut acc, (symbol, expression)| {
                println!("evaluating: {}", expression.to_string());
                let output = instance.evaluate(&expression, &symbol);
                if let Some(relation) = output {
                    relation
                        .ward
                        .iter()
                        .for_each(|(row, active)| {
                            if *active {
                                acc.insert_typed(&relation.symbol, row.clone());
                            }
                        })
                }
                acc
            })
            .database
            .values()
            .cloned()
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

impl<T> InstanceEvaluator<T> for ParallelRelationalAlgebra
    where T : IndexBacking {
    fn evaluate(&self, instance: &Instance<T>) -> Vec<Relation<T>> {
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

pub struct SimpleDatalog<T>
    where T : IndexBacking {
    pub fact_store: Instance<T>,
    interner: Interner,
    parallel: bool,
    intern: bool
}

impl<T> Default for SimpleDatalog<T>
    where T : IndexBacking {
    fn default() -> Self {
        SimpleDatalog {
            fact_store: Instance::new(false),
            interner: Interner::default(),
            parallel: true,
            intern: true,
        }
    }
}

impl<T> SimpleDatalog<T>
    where T : IndexBacking {
    pub fn new(parallel: bool, intern: bool) -> Self {
        return Self {
            parallel,
            intern,
            ..Default::default()
        }
    }
    pub fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut atom = Atom {
            symbol: table.to_string(),
            terms: row
                .iter()
                .map(|ty| Term::Constant(ty.to_typed_value()))
                .collect(),
            sign: Positive
        };
        if self.intern {
            atom = self.interner.intern_atom(&atom)
        }
        self.fact_store.insert_atom(&atom)
    }
}

impl<T> BottomUpEvaluator<T> for SimpleDatalog<T>
    where T : IndexBacking {
    fn evaluate_program_bottom_up(&mut self, program: Vec<Rule>) -> Instance<T> {
        let mut program = program;
        if self.intern {
            program = program
                .iter()
                .map(|rule| self.interner.intern_rule(rule))
                .collect();
        }
        let mut evaluation = Evaluation::new(&self.fact_store, Box::new(RuleToRelationalExpressionConverter::new(&sort_program(&program))));
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRelationalAlgebra::new(&program));
        }
        evaluation.semi_naive();

        return evaluation.output
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{Rule, TypedValue};
    use std::collections::{BTreeSet, HashSet};
    use std::ops::Deref;
    use crate::models::index::ValueRowId;
    use crate::models::reasoner::BottomUpEvaluator;
    use crate::reasoning::reasoners::simple::SimpleDatalog;

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