use lasso::Spur;
use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{SugaredAtom, SugaredProgram, SugaredRule, Ty, TypedValue};
use crate::models::index::IndexBacking;
use crate::models::instance::{Database, SimpleDatabaseWithIndex};
use crate::models::reasoner::{BottomUpEvaluator, Diff, Dynamic, DynamicTyped, Flusher, Materializer, Queryable, RelationDropper};
use crate::models::relational_algebra::{RelationalExpression, Row};
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::evaluation::{Evaluation, InstanceEvaluator};
use rayon::prelude::*;

pub struct RuleToRelationalExpressionConverter<'a> {
    pub program: Vec<(&'a str, u32, RelationalExpression)>,
}

impl<'a> RuleToRelationalExpressionConverter<'a> {
    fn new(program: Vec<(u32, SugaredRule)>) -> Self {
        return RuleToRelationalExpressionConverter {
            program: program
                .clone()
                .into_iter()
                .map(|(relation_id, rule)| {
                    (
                        &rule.head.symbol[..],
                        relation_id,
                        RelationalExpression::from(&rule),
                    )
                })
                .collect(),
        };
    }
}

impl<'a, T> InstanceEvaluator<SimpleDatabaseWithIndex<T>> for RuleToRelationalExpressionConverter<'a>
where
    T: IndexBacking + PartialEq,
{
    fn evaluate(&self, instance: &SimpleDatabaseWithIndex<T>) -> SimpleDatabaseWithIndex<T> {
        let mut out: SimpleDatabaseWithIndex<T> = Default::default();

        self
            .program
            .iter()
            .for_each(|(sym, relation_id, expr)| {
                if let Some(eval) = instance.evaluate(&expr, sym) {
                    if let None = out.storage.get(*sym) {
                        out.create_relation(sym.to_string(), *relation_id)
                    }

                    eval
                        .ward
                        .into_iter()
                        .for_each(|(row, active)| {
                            if active {
                                out.insert_at(*relation_id, row)
                            }
                        })
                }

            });

        return out
    }
}

pub struct ParallelRelationalAlgebra<'a> {
    pub program: Vec<(&'a str, u32, RelationalExpression)>,
}

impl<'a> ParallelRelationalAlgebra<'a> {
    fn new(program: &'a Vec<(u32, SugaredRule)>) -> Self {
        return ParallelRelationalAlgebra {
            program: program
                .iter()
                .map(|(relation_id, rule)| {
                    (
                        &(rule.head.symbol)[..],
                        *relation_id,
                        RelationalExpression::from(rule),
                    )
                })
                .collect(),
        };
    }
}

impl<'a, T> InstanceEvaluator<SimpleDatabaseWithIndex<T>> for ParallelRelationalAlgebra<'a>
where
    T: IndexBacking + PartialEq,
{
    fn evaluate(&self, instance: &SimpleDatabaseWithIndex<T>) -> SimpleDatabaseWithIndex<T> {
        let mut out: SimpleDatabaseWithIndex<T> = Default::default();

        self
            .program
            .par_iter()
            .filter_map(|(sym, relation_id, expr)| {
                if let Some(eval) = instance.evaluate(expr, sym) {
                    return Some((sym.clone(), relation_id.clone(), eval))
                }
                None
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|(sym, relation_id, fresh_relation)| {
                if let None = out.storage.get(sym) {
                    out.create_relation(sym.to_string(), relation_id)
                }

                fresh_relation
                    .ward
                    .into_iter()
                    .for_each(|(row, active)| {
                        if active {
                            out.insert_at(relation_id, row)
                        }
                    })
            });

        return out
    }
}

pub struct SimpleDatalog<T>
where
    T: IndexBacking + PartialEq,
{
    pub fact_store: SimpleDatabaseWithIndex<T>,
    interner: Interner,
    parallel: bool,
    sugared_program: SugaredProgram,
}

impl<T> Default for SimpleDatalog<T>
where
    T: IndexBacking + PartialEq,
{
    fn default() -> Self {
        SimpleDatalog {
            fact_store: SimpleDatabaseWithIndex::default(),
            interner: Interner::default(),
            parallel: true,
            sugared_program: vec![]
        }
    }
}

impl<T> SimpleDatalog<T>
where
    T: IndexBacking + PartialEq,
{
    pub fn new(parallel: bool) -> Self {
        return Self {
            parallel,
            ..Default::default()
        };
    }
}

impl<T: IndexBacking + PartialEq> Dynamic for SimpleDatalog<T> {
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut typed_row: Box<[TypedValue]> = row.iter().map(|ty| ty.to_typed_value()).collect();
        typed_row = self.interner.intern_typed_values(typed_row);

        self.insert_typed(table, typed_row)
    }

    fn delete(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        let mut typed_row = row.iter().map(|ty| ty.to_typed_value()).collect();
        typed_row = self.interner.intern_typed_values(typed_row);

        self.insert_typed(table, typed_row)
    }
}

impl<T: IndexBacking + PartialEq> DynamicTyped for SimpleDatalog<T> {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let typed_row = self.interner.intern_typed_values(row);
        let relation_id = self.interner.rodeo.get_or_intern(table).into_inner().get();

        self.fact_store.insert_at(relation_id, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: Row) {
        let typed_row = self.interner.intern_typed_values(row);
        let relation_id = self.interner.rodeo.get_or_intern(table).into_inner().get();

        self.fact_store.delete_at(relation_id, typed_row)
    }
}

impl<T: IndexBacking + PartialEq> Flusher for SimpleDatalog<T> {
    fn flush(&mut self, table: &str) {
        if let Some(relation) = self.fact_store.storage.get_mut(table) {
            relation.compact();
        }
    }
}

impl<'a, T: IndexBacking + PartialEq> BottomUpEvaluator<'a> for SimpleDatalog<T> {
    fn evaluate_program_bottom_up(&mut self, program: SugaredProgram) -> Vec<(&'a str, Row)> {
        let sugared_program = &sort_program(&program);

        let interned_sugared_program: Vec<(u32, SugaredRule)> = sugared_program
            .iter()
            .map(|rule| (
                self.interner.rodeo.get_or_intern(&rule.head.symbol[..]).into_inner().get(),
                self.interner.intern_sugared_rule(rule)
            ))
            .collect();

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(RuleToRelationalExpressionConverter::new(&interned_sugared_program.clone())),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRelationalAlgebra::new(&interned_sugared_program.clone()));
        }
        evaluation.semi_naive();

        return evaluation
            .output
            .storage
            .into_iter()
            .flat_map(|(sym, relation)| {
                relation
                    .ward
                    .into_iter()
                    .map(|row| (&sym[..], row.0))
            })
            .collect()
    }
}

impl<T: IndexBacking + PartialEq> Materializer for SimpleDatalog<T> {
    fn materialize(&mut self, program: &SugaredProgram) {
        let sugared_program = sort_program(&program);

        let interned_sugared_program = sugared_program
            .into_iter()
            .map(|rule| self.interner.intern_sugared_rule(&rule))
            .collect::<Vec<_>>();

        self.sugared_program = interned_sugared_program.clone();

        let relation_id_rule = interned_sugared_program
            .into_iter()
            .map(|rule| {
                let relation_id = self.interner.rodeo.get_or_intern(rule.head.symbol.clone()).into_inner().get();

                return (relation_id, rule)
            })
            .collect();

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(RuleToRelationalExpressionConverter::new(&relation_id_rule)));
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRelationalAlgebra::new(&relation_id_rule));
        }

        evaluation.semi_naive();

        evaluation
            .output
            .storage
            .iter()
            .for_each(|(symbol, relation)| {
                relation.ward.iter().for_each(|(row, _active)| {
                   self.insert_typed(symbol, row.clone());
                });
            });

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
            delete_rederive(self, &self.sugared_program, retractions)
        }

        let interned_sugared_program = self.sugared_program
            .iter()
            .map(|rule| self.interner.intern_sugared_rule(rule))
            .collect::<Vec<_>>();

        self.sugared_program = interned_sugared_program.clone();

        let relation_id_rule = interned_sugared_program
            .into_iter()
            .map(|rule| {
                let relation_id = self.interner.rodeo.get_or_intern(rule.head.symbol.clone()).into_inner().get();

                return (relation_id, rule)
            })
            .collect();

        if additions.len() > 0 {
            additions.iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row.clone());
            });
            let mut evaluation = Evaluation::new(
                &self.fact_store,
                Box::new(RuleToRelationalExpressionConverter::new(&relation_id_rule))
            );
            if self.parallel {
                evaluation.evaluator =
                    Box::new(ParallelRelationalAlgebra::new(&relation_id_rule));
            }

            evaluation.semi_naive();

            evaluation
                .output
                .storage
                .iter()
                .for_each(|(symbol, relation)| {
                    relation.ward.iter().for_each(|(row, _active)| {
                        self.insert_typed(symbol, row.clone());
                    });
                });
        }
    }

    fn triple_count(&self) -> usize {
        return self
            .fact_store
            .storage
            .iter()
            .map(|(_sym, rel)| return rel.ward.len())
            .sum();
    }
}

impl<T: IndexBacking + PartialEq> Queryable for SimpleDatalog<T> {
    fn contains(&mut self, atom: &SugaredAtom) -> bool {
        if let Some(rel) = self.fact_store.storage.get(&atom.symbol) {
            let mut boolean_query = atom
                .terms
                .iter()
                .map(|term| term.clone().into())
                .collect::<Vec<TypedValue>>()
                .into_boxed_slice();
            boolean_query = self.interner.intern_typed_values(boolean_query);

            return rel.ward.contains_key(&boolean_query);
        }

        return false
    }
}

impl<T : IndexBacking + PartialEq> RelationDropper for SimpleDatalog<T> {
    fn drop_relation(&mut self, table: &str) {
        self.fact_store.storage.remove(table);
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{SugaredRule, Ty, TypedValue};
    use crate::models::index::BTreeIndex;
    use crate::models::reasoner::{BottomUpEvaluator, Dynamic};
    use crate::reasoning::reasoners::simple::SimpleDatalog;
    use std::collections::HashSet;

    #[test]
    fn test_simple_datalog() {
        let mut reasoner: SimpleDatalog<BTreeIndex> = Default::default();
        reasoner.insert(
            "edge",
            Box::new(["a".to_typed_value(), "b".to_typed_value()]),
        );
        reasoner.insert(
            "edge",
            Box::new(["b".to_typed_value(), "c".to_typed_value()]),
        );
        reasoner.insert(
            "edge",
            Box::new(["b".to_typed_value(), "d".to_typed_value()]),
        );

        let new_tuples: HashSet<Vec<TypedValue>> = reasoner
            .evaluate_program_bottom_up(vec![
                SugaredRule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
                SugaredRule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
            ])
            .into_iter()
            .filter(|(sym, _)| *sym == "reachable")
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
