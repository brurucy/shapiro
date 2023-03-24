use lasso::Spur;
use crate::misc::helpers::{idempotent_program_strong_intern, idempotent_program_weak_intern, ty_to_row};
use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{SugaredProgram, Ty};
use crate::models::index::IndexBacking;
use crate::models::instance::{Database, SimpleDatabaseWithIndex};
use crate::models::reasoner::{
    BottomUpEvaluator, Diff, Dynamic, DynamicTyped, EvaluationResult, Materializer, Queryable,
    RelationDropper,
};
use crate::models::relational_algebra::{RelationalExpression, Row};
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::evaluation::{Evaluation, IncrementalEvaluation, InstanceEvaluator};
use rayon::prelude::*;
use crate::reasoning::algorithms::delta_rule_rewrite::{deltaify_idb, make_sne_programs};

pub struct RelationalAlgebra {
    pub program: Vec<(String, RelationalExpression)>,
}

impl RelationalAlgebra {
    fn new(program: &SugaredProgram) -> Self {
        return RelationalAlgebra {
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

impl<T> InstanceEvaluator<SimpleDatabaseWithIndex<T>> for RelationalAlgebra
where
    T: IndexBacking + PartialEq,
{
    fn evaluate(&self, instance: SimpleDatabaseWithIndex<T>) -> SimpleDatabaseWithIndex<T> {
        let mut out: SimpleDatabaseWithIndex<T> = SimpleDatabaseWithIndex::new(Interner::default());

        self.program.iter().for_each(|(sym, expr)| {
            if let Some(eval) = instance.evaluate(&expr, sym) {
                let local_relation_id = out
                    .symbol_interner
                    .rodeo
                    .get_or_intern(sym)
                    .into_inner()
                    .get();

                eval.ward
                    .into_iter()
                    .for_each(|row| out.insert_at(local_relation_id, row))
            }
        });

        return out;
    }
}

pub struct ParallelRelationalAlgebra {
    pub program: Vec<(String, RelationalExpression)>,
}

impl ParallelRelationalAlgebra {
    fn new(program: &SugaredProgram) -> Self {
        return ParallelRelationalAlgebra {
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

impl<T> InstanceEvaluator<SimpleDatabaseWithIndex<T>> for ParallelRelationalAlgebra
where
    T: IndexBacking + PartialEq,
{
    fn evaluate(&self, instance: SimpleDatabaseWithIndex<T>) -> SimpleDatabaseWithIndex<T> {
        let mut out: SimpleDatabaseWithIndex<T> = SimpleDatabaseWithIndex::new(Interner::default());

        self.program
            .par_iter()
            .filter_map(|(sym, expr)| {
                if let Some(eval) = instance.evaluate(expr, sym) {
                    return Some((sym, eval));
                }
                None
            })
            .collect::<Vec<_>>()
            .into_iter()
            .for_each(|(sym, fresh_relation)| {
                let local_relation_id = out
                    .symbol_interner
                    .rodeo
                    .get_or_intern(sym)
                    .into_inner()
                    .get();

                fresh_relation
                    .ward
                    .into_iter()
                    .for_each(|row| out.insert_at(local_relation_id, row))
            });

        return out;
    }
}

pub struct RelationalDatalog<T>
where
    T: IndexBacking + PartialEq,
{
    pub fact_store: SimpleDatabaseWithIndex<T>,
    pub row_interner: Interner,
    parallel: bool,
    intern: bool,
    sugared_program: SugaredProgram,
}

impl<T> Default for RelationalDatalog<T>
where
    T: IndexBacking + PartialEq,
{
    fn default() -> Self {
        RelationalDatalog {
            fact_store: SimpleDatabaseWithIndex::default(),
            row_interner: Default::default(),
            parallel: true,
            intern: true,
            sugared_program: Default::default(),
        }
    }
}

impl<T> RelationalDatalog<T>
where
    T: IndexBacking + PartialEq,
{
    pub fn new(parallel: bool, intern: bool) -> Self {
        return Self {
            parallel,
            intern,
            ..Default::default()
        };
    }
}

impl<T: IndexBacking + PartialEq> Dynamic for RelationalDatalog<T> {
    fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        self.insert_typed(table, ty_to_row(&row))
    }

    fn delete(&mut self, table: &str, row: &Vec<Box<dyn Ty>>) {
        self.delete_typed(table, &ty_to_row(row))
    }
}

impl<T: IndexBacking + PartialEq> RelationalDatalog<T> {
    fn idempotent_intern(&mut self, table: &str, row: Row) -> (u32, Row) {
        let typed_row = if self.intern {
            self.row_interner.intern_row(row)
        } else {
            row
        };
        let relation_id = self
            .fact_store
            .symbol_interner
            .rodeo
            .get_or_intern(table)
            .into_inner()
            .get();

        return (relation_id, typed_row);
    }
    fn idempotent_program_weak_intern(&mut self, program: &SugaredProgram) -> SugaredProgram {
        return idempotent_program_weak_intern(&mut self.row_interner, self.intern, program);
    }
    // fn new_evaluation(
    //     &self,
    //     sugared_program: &SugaredProgram,
    // ) -> Evaluation<SimpleDatabaseWithIndex<T>> {
    //     return Evaluation::new(
    //         &self.fact_store,
    //         if self.parallel {
    //             Box::new(ParallelRelationalAlgebra::new(sugared_program))
    //         } else {
    //             Box::new(RelationalAlgebra::new(sugared_program))
    //         },
    //     );
    // }
    // fn update_materialization(&mut self) {
    //     let mut evaluation = self.new_evaluation(&self.sugared_program);
    //
    //     evaluation.semi_naive();
    //
    //     evaluation
    //         .output
    //         .storage
    //         .into_iter()
    //         .for_each(|(symbol, relation)| {
    //             relation.ward.into_iter().for_each(|row| {
    //                 self.insert_typed(&symbol, row);
    //             });
    //         });
    // }
    fn new_evaluation(&self, [delta, nonrecursive, recursive]: [Box<ParallelRelationalAlgebra>; 3]) -> IncrementalEvaluation<SimpleDatabaseWithIndex<T>> {
        return IncrementalEvaluation::new(delta, nonrecursive, recursive)
    }
    fn update_materialization(&mut self) {
        let deltaifier = deltaify_idb(&self.sugared_program);
        let (nonrecursive, recursive) = make_sne_programs(&self.sugared_program);

        let programs = [deltaifier, nonrecursive, recursive];
        let instance_evaluators = programs.map(|program| {
            return Box::new(ParallelRelationalAlgebra::new(&program))
        });

        let mut evaluation = self.new_evaluation(instance_evaluators);

        evaluation.semi_naive(&self.fact_store);

        evaluation
            .output
            .storage
            .into_iter()
            .for_each(|(symbol, relation)| {
                relation.ward.into_iter().for_each(|row| {
                    self.insert_typed(&symbol, row);
                });
            });
    }
}

impl<T: IndexBacking + PartialEq> DynamicTyped for RelationalDatalog<T> {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let (relation_id, typed_row) = self.idempotent_intern(table, row);

        self.fact_store.insert_at(relation_id, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: &Row) {
        let (relation_id, typed_row) = self.idempotent_intern(table, row.clone());

        self.fact_store.delete_at(relation_id, &typed_row)
    }
}

impl<T: IndexBacking + PartialEq> BottomUpEvaluator for RelationalDatalog<T> {
    fn evaluate_program_bottom_up(&mut self, program: &SugaredProgram) -> EvaluationResult {
        // let interned_sugared_program = self.idempotent_program_weak_intern(program);
        //
        // let mut evaluation = self.new_evaluation(&interned_sugared_program);
        //
        // evaluation.semi_naive();
        //
        // return evaluation.output.storage.into_iter().fold(
        //     Default::default(),
        //     |mut acc: EvaluationResult, (sym, row)| {
        //         acc.insert(sym, row.ward);
        //         acc
        //     },
        // );
        let deltaifier = deltaify_idb(program);
        let (nonrecursive, recursive) = make_sne_programs(program);

        let programs = [deltaifier, nonrecursive, recursive];

        let instance_evaluators = programs.map(|program| {
            return Box::new(ParallelRelationalAlgebra::new(&program));
        });

        let mut evaluation = self.new_evaluation(instance_evaluators);

        evaluation.semi_naive(&self.fact_store);

        return evaluation.output.storage.into_iter().fold(
            Default::default(),
            |mut acc: EvaluationResult, (sym, row)| {
                acc.insert(sym, row.ward);
                acc
            },
        );
    }
}

impl<T: IndexBacking + PartialEq> Materializer for RelationalDatalog<T> {
    fn materialize(&mut self, program: &SugaredProgram) {
        self.idempotent_program_weak_intern(program)
            .into_iter()
            .for_each(|sugared_rule| {
                self.sugared_program.push(sugared_rule);
            });

        self.sugared_program = sort_program(&self.sugared_program);

        self.update_materialization();
    }

    // Update first processes deletions, then additions.
    fn update(&mut self, changes: Vec<Diff>) {
        let mut additions: Vec<(&str, Row)> = vec![];
        let mut retractions: Vec<(&str, Row)> = vec![];

        changes.iter().for_each(|(sign, (sym, value))| {
            let typed_row: Row = ty_to_row(value);

            if *sign {
                additions.push((sym, typed_row));
            } else {
                retractions.push((sym, typed_row));
            }
        });

        if retractions.len() > 0 {
            delete_rederive(self, &self.sugared_program.clone(), retractions)
        }

        if additions.len() > 0 {
            additions.into_iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row);
            });

            self.update_materialization();
        }

        //self.update_materialization();
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

impl<T: IndexBacking + PartialEq> Queryable for RelationalDatalog<T> {
    fn contains_row(&self, table: &str, row: &Vec<Box<dyn Ty>>) -> bool {
        if let Some(relation) = self.fact_store.storage.get(table) {
            let mut typed_row = Some(ty_to_row(row));
            if self.intern {
                typed_row = self.row_interner.try_intern_row(&typed_row.unwrap());
                if typed_row == None {
                    return false;
                }
            }
            return relation.ward.contains(&typed_row.unwrap());
        }

        return false;
    }
}

impl<T: IndexBacking + PartialEq> RelationDropper for RelationalDatalog<T> {
    fn drop_relation(&mut self, table: &str) {
        self.fact_store.storage.remove(table);
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{SugaredRule, TypedValue};
    use crate::models::index::BTreeIndex;
    use crate::models::reasoner::{BottomUpEvaluator, Dynamic, Materializer, Queryable};
    use crate::models::relational_algebra::Row;
    use crate::reasoning::reasoners::relational::RelationalDatalog;
    use indexmap::IndexSet;

    #[test]
    fn test_relational_operations() {
        let mut reasoner: RelationalDatalog<BTreeIndex> = RelationalDatalog::new(false, false);

        assert!(!reasoner.contains_row("edge", &vec![Box::new("a"), Box::new("b")]));
        assert!(!reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("c")]));
        assert!(!reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("d")]));

        assert_eq!(reasoner.triple_count(), 0);

        reasoner.insert("edge", vec![Box::new("a"), Box::new("b")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("c")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("d")]);

        assert_eq!(reasoner.triple_count(), 3);

        assert!(reasoner.contains_row("edge", &vec![Box::new("a"), Box::new("b")]));
        assert!(reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("c")]));
        assert!(reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("d")]));

        reasoner.delete("edge", &vec![Box::new("a"), Box::new("b")]);
        reasoner.delete("edge", &vec![Box::new("b"), Box::new("c")]);
        reasoner.delete("edge", &vec![Box::new("b"), Box::new("d")]);

        assert_eq!(reasoner.triple_count(), 0);

        assert!(!reasoner.contains_row("edge", &vec![Box::new("a"), Box::new("b")]));
        assert!(!reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("c")]));
        assert!(!reasoner.contains_row("edge", &vec![Box::new("b"), Box::new("d")]));
    }

    #[test]
    fn test_relational_datalog() {
        let mut reasoner: RelationalDatalog<BTreeIndex> = RelationalDatalog::new(false, false);
        reasoner.insert("edge", vec![Box::new("a"), Box::new("b")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("c")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("d")]);

        let new_tuples = reasoner
            .evaluate_program_bottom_up(&vec![
                SugaredRule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
                SugaredRule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
            ])
            .get("reachable")
            .unwrap()
            .clone();

        let mut expected_new_tuples: IndexSet<Row> = Default::default();

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
