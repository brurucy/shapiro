use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Atom, Program, SugaredProgram, SugaredRule, Ty, TypedValue};
use crate::models::index::IndexBacking;
use crate::models::instance::SimpleDatabaseWithIndex;
use crate::models::reasoner::{
    BottomUpEvaluator, Diff, Dynamic, DynamicTyped, Flusher, Materializer, Queryable,
    RelationDropper,
};
use crate::models::relational_algebra::{SimpleRelationWithOneIndexBacking, RelationalExpression, Row};
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::evaluation::{Evaluation, InstanceEvaluator};
use rayon::prelude::*;

pub struct RuleToRelationalExpressionConverter<'a> {
    pub program: Vec<(&'a str, RelationalExpression)>,
}

impl<'a> RuleToRelationalExpressionConverter<'a> {
    fn new(program: &Vec<SugaredRule>) -> Self {
        return RuleToRelationalExpressionConverter {
            program: program
                .iter()
                .map(|rule| {
                    (
                        &rule.head.symbol[..],
                        RelationalExpression::from(rule),
                    )
                })
                .collect(),
        };
    }
}

impl<'a, T> InstanceEvaluator<T> for RuleToRelationalExpressionConverter<'a>
where
    T: IndexBacking,
{
    fn evaluate(&self, instance: &SimpleDatabaseWithIndex<T>) -> Vec<SimpleRelationWithOneIndexBacking<T>> {
        return self
            .program
            .clone()
            .into_iter()
            .fold(SimpleDatabaseWithIndex::new(), |mut acc, (symbol, expression)| {
                let output = instance.evaluate(&expression, &symbol);
                if let Some(relation) = output {
                    relation.ward.iter().for_each(|(row, active)| {
                        if *active {
                            acc.insert_typed(&relation.relation_id, row.clone());
                        }
                    })
                }
                acc
            })
            .storage
            .values()
            .cloned()
            .collect();
    }
}

pub struct ParallelRelationalAlgebra {
    pub program: Vec<(String, RelationalExpression)>,
}

impl ParallelRelationalAlgebra {
    fn new(program: &Vec<SugaredRule>) -> Self {
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
    fn evaluate(&self, instance: &SimpleDatabaseWithIndex<T>) -> Vec<SimpleRelationWithOneIndexBacking<T>> {
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
    pub storage: SimpleDatabaseWithIndex<T>,
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
            storage: SimpleDatabaseWithIndex::new(),
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

        self.storage.insert_typed(table, typed_row)
    }

    fn delete(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut typed_row = row.iter().map(|ty| ty.to_typed_value()).collect();

        if self.materialization.len() > 0 {
            self.safe = false
        }

        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.storage.delete_typed(table, typed_row)
    }
}

impl<T: IndexBacking> DynamicTyped for SimpleDatalog<T> {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let mut typed_row = row.clone();
        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.storage.insert_typed(table, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: Row) {
        let mut typed_row = row.clone();
        if self.intern {
            typed_row = self.interner.intern_typed_values(typed_row);
        }

        self.storage.delete_typed(table, typed_row)
    }
}

impl<T: IndexBacking> Flusher for SimpleDatalog<T> {
    fn flush(&mut self, table: &str) {
        if let Some(relation) = self.storage.storage.get_mut(table) {
            relation.compact();
        }
    }
}

impl<T> BottomUpEvaluator<T> for SimpleDatalog<T>
where
    T: IndexBacking,
{
    fn evaluate_program_bottom_up(&mut self, program: SugaredProgram) -> SimpleDatabaseWithIndex<T> {
        let interned_program = &sort_program(&program)
            .iter()
            .map(|sugared_rule| self.interner.intern_rule(sugared_rule))
            .collect();

        let mut evaluation = Evaluation::new(
            &self.storage,
            Box::new(RuleToRelationalExpressionConverter::new(interned_program)),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRelationalAlgebra::new(interned_program));
        }
        evaluation.semi_naive();

        return evaluation.output;
    }
}

impl<T: IndexBacking> Materializer for SimpleDatalog<T> {
    fn materialize(&mut self, program: &Program) {
        program.iter().for_each(|rule| {
            self.materialization.push(rule);
        });

        let mut evaluation = Evaluation::new(
            &self.storage,
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
            let typed_row: Row = value
                .into_iter()
                .map(|untyped_value| untyped_value.to_typed_value())
                .collect();

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
                &self.storage,
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
            .storage
            .storage
            .iter()
            .map(|(_sym, rel)| return rel.ward.len())
            .sum();
    }
}

impl<T: IndexBacking> Queryable for SimpleDatalog<T> {
    fn contains(&mut self, atom: &Atom) -> bool {
        let rel = self.storage.view(&atom.relation_id);
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
        self.storage.storage.remove(table);
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{SugaredRule, Ty, TypedValue};
    use crate::models::index::BTreeIndex;
    use crate::models::reasoner::BottomUpEvaluator;
    use crate::reasoning::reasoners::simple::SimpleDatalog;
    use std::collections::HashSet;
    use std::ops::Deref;

    #[test]
    fn test_simple_datalog() {
        let mut reasoner: SimpleDatalog<BTreeIndex> = Default::default();
        reasoner.storage.insert_typed(
            "edge",
            Box::new(["a".to_typed_value(), "b".to_typed_value()]),
        );
        reasoner.storage.insert_typed(
            "edge",
            Box::new(["b".to_typed_value(), "c".to_typed_value()]),
        );
        reasoner.storage.insert_typed(
            "edge",
            Box::new(["b".to_typed_value(), "d".to_typed_value()]),
        );

        let new_tuples: HashSet<Vec<TypedValue>> = reasoner
            .evaluate_program_bottom_up(vec![
                SugaredRule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
                SugaredRule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
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
