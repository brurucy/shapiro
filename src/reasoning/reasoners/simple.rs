use crate::misc::rule_graph::sort_program;
use crate::models::datalog::{SugaredAtom, SugaredProgram, SugaredRule, Ty, TypedValue};
use crate::models::index::IndexBacking;
use crate::models::instance::{Database, SimpleDatabaseWithIndex};
use crate::models::reasoner::{BottomUpEvaluator, Diff, Dynamic, DynamicTyped, EvaluationResult, Flusher, Materializer, Queryable, RelationDropper};
use crate::models::relational_algebra::{RelationalExpression, Row};
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::evaluation::{Evaluation, InstanceEvaluator};
use rayon::prelude::*;
use crate::misc::helpers::ty_to_row;
use crate::misc::string_interning::Interner;

pub struct RelationalAlgebra {
    pub program: Vec<(String, u32, RelationalExpression)>,
}

impl RelationalAlgebra {
    fn new(program: &Vec<(u32, SugaredRule)>) -> Self {
        return RelationalAlgebra {
            program: program
                .iter()
                .map(|(relation_id, rule)| {
                    (
                        rule.head.symbol.clone(),
                        *relation_id,
                        RelationalExpression::from(rule),
                    )
                })
                .collect(),
        };
    }
}

impl<T> InstanceEvaluator<SimpleDatabaseWithIndex<T>> for RelationalAlgebra
where
    T: IndexBacking + PartialEq,
{
    fn evaluate(&self, instance: &SimpleDatabaseWithIndex<T>) -> SimpleDatabaseWithIndex<T> {
        let mut out: SimpleDatabaseWithIndex<T> = SimpleDatabaseWithIndex::new(instance.symbol_interner.clone());

        self
            .program
            .iter()
            .for_each(|(sym, relation_id, expr)| {
                if let Some(eval) = instance.evaluate(&expr, sym) {
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

pub struct ParallelRelationalAlgebra {
    pub program: Vec<(String, u32, RelationalExpression)>,
}

impl ParallelRelationalAlgebra {
    fn new(program: &Vec<(u32, SugaredRule)>) -> Self {
        return ParallelRelationalAlgebra {
            program: program
                .into_iter()
                .map(|(relation_id, rule)| {
                    (
                        rule.head.symbol.clone(),
                        *relation_id,
                        RelationalExpression::from(rule),
                    )
                })
                .collect(),
        };
    }
}

impl<T> InstanceEvaluator<SimpleDatabaseWithIndex<T>> for ParallelRelationalAlgebra
where
    T: IndexBacking + PartialEq,
{
    fn evaluate(&self, instance: &SimpleDatabaseWithIndex<T>) -> SimpleDatabaseWithIndex<T> {
        let mut out: SimpleDatabaseWithIndex<T> = SimpleDatabaseWithIndex::new(instance.symbol_interner.clone());

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
            .for_each(|(sym, _relation_id, fresh_relation)| {
                let local_relation_id = out.symbol_interner.rodeo.get_or_intern(sym.to_string()).into_inner().get();

                fresh_relation
                    .ward
                    .into_iter()
                    .for_each(|(row, active)| {
                        if active {
                            out.insert_at(local_relation_id, row)
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
    pub row_interner: Interner,
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
            row_interner: Default::default(),
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
        self.insert_typed(table,ty_to_row(row))
    }

    fn delete(&mut self, table: &str, row: &Vec<Box<dyn Ty>>) {
        self.delete_typed(table, &row.iter().map(|ty| ty.to_typed_value()).collect())
    }
}

impl<T: IndexBacking + PartialEq> DynamicTyped for SimpleDatalog<T> {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let typed_row = self.row_interner.intern_typed_values(&row);
        let relation_id = self.fact_store.symbol_interner.rodeo.get_or_intern(table).into_inner().get();

        self.fact_store.insert_at(relation_id, row)
    }
    fn delete_typed(&mut self, table: &str, row: &Row) {
        let typed_row = self.row_interner.intern_typed_values(row);
        let relation_id = self.fact_store.symbol_interner.rodeo.get_or_intern(table).into_inner().get();

        self.fact_store.delete_at(relation_id, row)
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
    fn evaluate_program_bottom_up(&mut self, program: &SugaredProgram) -> EvaluationResult {
        let sugared_program = &sort_program(&program);

        let interned_sugared_program: Vec<(u32, SugaredRule)> = sugared_program
            .iter()
            .map(|rule| (
                self.fact_store.symbol_interner.rodeo.get_or_intern(&rule.head.symbol[..]).into_inner().get(),
                self.row_interner.intern_sugared_rule(rule)
            ))
            .collect();

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(RelationalAlgebra::new(&interned_sugared_program)),
        );
        if self.parallel {
            evaluation.evaluator = Box::new(ParallelRelationalAlgebra::new(&interned_sugared_program));
        }
        evaluation.semi_naive();

        return evaluation
            .output
            .storage
            .into_iter()
            .fold(Default::default(), |mut acc: EvaluationResult, (sym, row_active_set)| {
                let row_set = row_active_set
                    .ward
                    .into_iter()
                    .map(|(k, _v)| k)
                    .collect();

                acc.insert(sym, row_set);
                acc
            })
    }
}

impl<T: IndexBacking + PartialEq> Materializer for SimpleDatalog<T> {
    fn materialize(&mut self, program: &SugaredProgram) {
        let sugared_program = sort_program(&program);

        let interned_sugared_program = sugared_program
            .into_iter()
            .map(|rule| self.row_interner.intern_sugared_rule(&rule))
            .collect::<Vec<_>>();

        self.sugared_program = interned_sugared_program.clone();

        let relation_id_rule: Vec<_> = interned_sugared_program
            .into_iter()
            .map(|rule| {
                let relation_id = self.fact_store.symbol_interner.rodeo.get_or_intern(rule.head.symbol.clone()).into_inner().get();

                return (relation_id, rule)
            })
            .collect();

        let mut evaluation = Evaluation::new(
            &self.fact_store,
            Box::new(RelationalAlgebra::new(&relation_id_rule)));
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

        let sugared_program = self.sugared_program.clone();

        if retractions.len() > 0 {
            delete_rederive(self, &sugared_program, retractions)
        }

        let interned_sugared_program = self.sugared_program
            .iter()
            .map(|rule| self.fact_store.symbol_interner.intern_sugared_rule(rule))
            .collect::<Vec<_>>();

        self.sugared_program = interned_sugared_program.clone();

        let relation_id_rule: Vec<_> = interned_sugared_program
            .into_iter()
            .map(|rule| {
                let relation_id = self.fact_store.symbol_interner.rodeo.get_or_intern(rule.head.symbol.clone()).into_inner().get();

                return (relation_id, rule)
            })
            .collect();

        if additions.len() > 0 {
            additions.iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row.clone());
            });
            let mut evaluation = Evaluation::new(
                &self.fact_store,
                Box::new(RelationalAlgebra::new(&relation_id_rule))
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
            boolean_query = self.row_interner.intern_typed_values(&boolean_query);

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
    use ahash::HashSet;
    use crate::models::datalog::{SugaredRule,TypedValue};
    use crate::models::index::BTreeIndex;
    use crate::models::reasoner::{BottomUpEvaluator, Dynamic};
    use crate::reasoning::reasoners::simple::SimpleDatalog;
    use crate::models::relational_algebra::Row;

    #[test]
    fn test_simple_datalog() {
        let mut reasoner: SimpleDatalog<BTreeIndex> = Default::default();
        reasoner.insert(
            "edge",
            vec![Box::new("a"), Box::new("b")],
        );
        reasoner.insert(
            "edge",
            vec![Box::new("b"), Box::new("c")],
        );
        reasoner.insert(
            "edge",
            vec![Box::new("b"), Box::new("d")],
        );

        let new_tuples: HashSet<Row> = reasoner
            .evaluate_program_bottom_up(&vec![
                SugaredRule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
                SugaredRule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
            ])
            .get("reachable")
            .unwrap()
            .clone();

        let mut expected_new_tuples: HashSet<Row> = Default::default();

        vec![
            // Rule 1 output
            Box::new([
                TypedValue::Str("a".to_string()),
                TypedValue::Str("b".to_string()),
            ]),
            Box::new([
                TypedValue::Str("b".to_string()),
                TypedValue::Str("c".to_string()),
            ]),
            Box::new([
                TypedValue::Str("b".to_string()),
                TypedValue::Str("d".to_string()),
            ]),
            // Rule 2 output
            Box::new([
                TypedValue::Str("a".to_string()),
                TypedValue::Str("c".to_string()),
            ]),
            Box::new([
                TypedValue::Str("a".to_string()),
                TypedValue::Str("d".to_string()),
            ]),
        ]
            .into_iter()
            .for_each(|row| {
                expected_new_tuples.insert(row);
            });

        assert_eq!(expected_new_tuples, new_tuples)
    }
}
