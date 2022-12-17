use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::Sign::Positive;
use crate::models::datalog::{Atom, Program, Rule, Term, Ty, TypedValue};
use crate::models::index::IndexBacking;
use crate::models::instance::Instance;
use crate::models::reasoner::{
    BottomUpEvaluator, Diff, Dynamic, DynamicTyped, Flusher, Materializer, Queryable,
    RelationDropper,
};
use crate::models::relational_algebra::{Relation, RelationalExpression, Row};
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::evaluation::{Evaluation, InstanceEvaluator};
use rayon::prelude::*;

pub struct RuleToRelationalExpressionConverter {
    pub program: Vec<(String, RelationalExpression)>,
}

impl RuleToRelationalExpressionConverter {
    fn new(program: &Vec<Rule>) -> Self {
        return RuleToRelationalExpressionConverter {
            program: program
                .iter()
                .map(|rule| {
                    (
                        rule.head.symbol.to_string(),
                        RelationalExpression::from(rule),
                    )
                })
                .collect(),
        };
    }
}

impl<T> InstanceEvaluator<T> for RuleToRelationalExpressionConverter
where
    T: IndexBacking,
{
    fn evaluate(&self, instance: &Instance<T>) -> Vec<Relation<T>> {
        return self
            .program
            .clone()
            .into_iter()
            .fold(Instance::new(false), |mut acc, (symbol, expression)| {
                let output = instance.evaluate(&expression, &symbol);
                if let Some(relation) = output {
                    relation.ward.iter().for_each(|(row, active)| {
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
    pub program: Vec<(String, RelationalExpression)>,
}

impl ParallelRelationalAlgebra {
    fn new(program: &Vec<Rule>) -> Self {
        return ParallelRelationalAlgebra {
            program: program
                .iter()
                .map(|rule| {
                    let head = rule.head.symbol.to_string();
                    let expr = RelationalExpression::from(rule);
                    return (head, expr);
                })
                .collect(),
        };
    }
}

impl<T> InstanceEvaluator<T> for ParallelRelationalAlgebra
where
    T: IndexBacking,
{
    fn evaluate(&self, instance: &Instance<T>) -> Vec<Relation<T>> {
        return self
            .program
            .clone()
            .into_par_iter()
            .filter_map(|(symbol, expression)| return instance.evaluate(&expression, &symbol))
            .collect();
    }
}

pub struct SimpleDatalog<T>
where
    T: IndexBacking,
{
    pub fact_store: Instance<T>,
    interner: Interner,
    parallel: bool,
    intern: bool,
    safe: bool,
    materialization: Program,
}

impl<T> Default for SimpleDatalog<T>
where
    T: IndexBacking,
{
    fn default() -> Self {
        SimpleDatalog {
            fact_store: Instance::new(false),
            interner: Interner::default(),
            parallel: true,
            intern: true,
            safe: true,
            materialization: vec![],
        }
    }
}

impl<T> SimpleDatalog<T>
where
    T: IndexBacking,
{
    pub fn new(parallel: bool, intern: bool) -> Self {
        return Self {
            parallel,
            intern,
            ..Default::default()
        };
    }
    pub fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut atom = Atom {
            symbol: table.to_string(),
            terms: row
                .iter()
                .map(|ty| Term::Constant(ty.to_typed_value()))
                .collect(),
            sign: Positive,
        };
        if self.intern {
            atom = self.interner.intern_atom(&atom)
        }
        self.fact_store.insert_atom(&atom)
    }
}

impl<T: IndexBacking> Dynamic for SimpleDatalog<T> {
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut typed_row: Box<[TypedValue]> = row.iter().map(|ty| ty.to_typed_value()).collect();

        if self.materialization.len() > 0 {
            self.safe = false
        }

        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.fact_store.insert_typed(table, typed_row)
    }

    fn delete(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut typed_row = row.iter().map(|ty| ty.to_typed_value()).collect();

        if self.materialization.len() > 0 {
            self.safe = false
        }

        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.fact_store.delete_typed(table, typed_row)
    }
}

impl<T: IndexBacking> DynamicTyped for SimpleDatalog<T> {
    fn insert_typed(&mut self, table: &str, row: Row) {
        if self.materialization.len() > 0 {
            self.safe = false
        }

        let mut typed_row = row.clone();
        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.fact_store.insert_typed(table, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: Row) {
        if self.materialization.len() > 0 {
            self.safe = false
        }

        let mut typed_row = row.clone();
        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.fact_store.delete_typed(table, typed_row)
    }
}

impl<T: IndexBacking> Flusher for SimpleDatalog<T> {
    fn flush(&mut self, table: &str) {
        if let Some(relation) = self.fact_store.database.get_mut(table) {
            relation.compact();
        }
    }
}

impl<T> BottomUpEvaluator<T> for SimpleDatalog<T>
where
    T: IndexBacking,
{
    fn evaluate_program_bottom_up(&mut self, program: Vec<Rule>) -> Instance<T> {
        let mut program = program;
        if self.intern {
            program = program
                .iter()
                .map(|rule| self.interner.intern_rule(rule))
                .collect();
        }

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(RuleToRelationalExpressionConverter::new(&sort_program(
                &program,
            ))),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRelationalAlgebra::new(&program));
        }
        evaluation.semi_naive();

        return evaluation.output;
    }
}

impl<T: IndexBacking> Materializer for SimpleDatalog<T> {
    fn materialize(&mut self, program: &Program) {
        program.iter().for_each(|rule| {
            let mut possibly_interned_rule = rule.clone();
            if self.intern {
                possibly_interned_rule = self.interner.intern_rule(&possibly_interned_rule);
            }
            self.materialization.push(possibly_interned_rule);
        });

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(RuleToRelationalExpressionConverter::new(&sort_program(
                &self.materialization,
            ))),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRelationalAlgebra::new(&self.materialization));
        }

        evaluation.semi_naive();

        evaluation
            .output
            .database
            .iter()
            .for_each(|(symbol, relation)| {
                relation.ward.iter().for_each(|(row, _active)| {
                    self.insert_typed(symbol, row.clone());
                });
            });

        self.safe = true;
    }

    // Update first processes deletions, then additions.
    fn update(&mut self, changes: Vec<Diff>) {
        let mut additions: Vec<(&str, Row)> = vec![];
        let mut retractions: Vec<(&str, Row)> = vec![];

        changes.iter().for_each(|(sign, (sym, value))| {
            let mut typed_row: Row = value
                .into_iter()
                .map(|untyped_value| untyped_value.to_typed_value())
                .collect();

            if self.intern {
                typed_row = self.interner.intern_typed_values(typed_row);
            }

            if *sign {
                additions.push((sym, typed_row));
            } else {
                retractions.push((sym, typed_row));
            }
        });

        if retractions.len() > 0 {
            delete_rederive(self, &self.materialization.clone(), retractions)
        }

        if additions.len() > 0 {
            additions.iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row.clone());
            });
            let mut evaluation = Evaluation::new(
                &self.fact_store,
                Box::new(RuleToRelationalExpressionConverter::new(&sort_program(
                    &self.materialization,
                ))),
            );
            if self.parallel {
                evaluation.evaluator =
                    Box::new(ParallelRelationalAlgebra::new(&self.materialization));
            }

            evaluation.semi_naive();

            evaluation
                .output
                .database
                .iter()
                .for_each(|(symbol, relation)| {
                    relation.ward.iter().for_each(|(row, _active)| {
                        self.insert_typed(symbol, row.clone());
                    });
                });
        }
        self.safe = true;
    }

    fn triple_count(&self) -> usize {
        return self
            .fact_store
            .database
            .iter()
            .map(|(sym, rel)| return rel.ward.len())
            .sum();
    }
}

impl<T: IndexBacking> Queryable for SimpleDatalog<T> {
    fn contains(&mut self, atom: &Atom) -> bool {
        let rel = self.fact_store.view(&atom.symbol);
        let mut boolean_query = atom
            .terms
            .iter()
            .map(|term| term.clone().into())
            .collect::<Vec<TypedValue>>()
            .into_boxed_slice();
        if self.intern {
            boolean_query = self.interner.intern_typed_values(boolean_query);
        }

        return rel.contains(&boolean_query);
    }
}

impl<T: IndexBacking> RelationDropper for SimpleDatalog<T> {
    fn drop_relation(&mut self, table: &str) {
        self.fact_store.database.remove(table);
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{Rule, Ty, TypedValue};
    use crate::models::index::BTreeIndex;
    use crate::models::reasoner::BottomUpEvaluator;
    use crate::reasoning::reasoners::simple::SimpleDatalog;
    use std::collections::HashSet;
    use std::ops::Deref;

    #[test]
    fn test_simple_datalog() {
        let mut reasoner: SimpleDatalog<BTreeIndex> = Default::default();
        reasoner.fact_store.insert_typed(
            "edge",
            Box::new(["a".to_typed_value(), "b".to_typed_value()]),
        );
        reasoner.fact_store.insert_typed(
            "edge",
            Box::new(["b".to_typed_value(), "c".to_typed_value()]),
        );
        reasoner.fact_store.insert_typed(
            "edge",
            Box::new(["b".to_typed_value(), "d".to_typed_value()]),
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
