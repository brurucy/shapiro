use crate::misc::helpers::{idempotent_program_weak_intern, ty_to_row};
use crate::misc::string_interning::Interner;
use crate::models::datalog::SugaredProgram;
use crate::models::index::IndexBacking;
use crate::models::instance::{Database, SimpleDatabaseWithIndex};
use crate::models::reasoner::{
    BottomUpEvaluator, Diff, Dynamic, DynamicTyped, EvaluationResult, Materializer, Queryable,
    RelationDropper, UntypedRow,
};
use crate::models::relational_algebra::{RelationalExpression, Row};
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::delta_rule_rewrite::{
    deltaify_idb, make_sne_programs, make_update_sne_programs, DELTA_PREFIX,
};
use crate::reasoning::algorithms::evaluation::{
    ImmediateConsequenceOperator, IncrementalEvaluation,
};
use colored::Colorize;
use rayon::prelude::*;
use std::time::Instant;

pub fn evaluate_rules_sequentially<T: IndexBacking>(
    sugared_program: &Vec<(String, RelationalExpression)>,
    instance: &SimpleDatabaseWithIndex<T>,
) -> SimpleDatabaseWithIndex<T> {
    let mut out: SimpleDatabaseWithIndex<T> = SimpleDatabaseWithIndex::new(Interner::default());

    sugared_program.iter().for_each(|(sym, expr)| {
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

pub fn evaluate_rules_in_parallel<T: IndexBacking>(
    sugared_program: &Vec<(String, RelationalExpression)>,
    instance: &SimpleDatabaseWithIndex<T>,
) -> SimpleDatabaseWithIndex<T> {
    let mut out: SimpleDatabaseWithIndex<T> = SimpleDatabaseWithIndex::new(Interner::default());

    sugared_program
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

pub fn key_program_by_symbol(
    sugared_program: &SugaredProgram,
) -> Vec<(String, RelationalExpression)> {
    return sugared_program
        .iter()
        .map(|rule| {
            (
                rule.head.symbol.to_string(),
                RelationalExpression::from(rule),
            )
        })
        .collect();
}

pub fn deltaify_idb_by_renaming<T: IndexBacking>(
    deltaify_idb_program: &SugaredProgram,
    fact_store: &SimpleDatabaseWithIndex<T>,
) -> SimpleDatabaseWithIndex<T> {
    let mut out = fact_store.clone();
    deltaify_idb_program.iter().for_each(|rule| {
        if let Some(relation) = fact_store.storage.get(&(rule.body[0].symbol)) {
            out.storage
                .insert(rule.head.symbol.clone(), relation.clone());
        }
    });

    return out;
}

pub struct RelationalAlgebra {
    pub nonrecursive_program: Vec<(String, RelationalExpression)>,
    pub recursive_program: Vec<(String, RelationalExpression)>,
    pub deltaifying_program: SugaredProgram,
}

impl RelationalAlgebra {
    fn new(
        nonrecursive_program: &SugaredProgram,
        recursive_program: &SugaredProgram,
        deltaifying_program: &SugaredProgram,
    ) -> Self {
        return RelationalAlgebra {
            nonrecursive_program: key_program_by_symbol(nonrecursive_program),
            recursive_program: key_program_by_symbol(recursive_program),
            deltaifying_program: deltaifying_program.clone(),
        };
    }
}

impl<T: IndexBacking> ImmediateConsequenceOperator<SimpleDatabaseWithIndex<T>>
    for RelationalAlgebra
{
    fn deltaify_idb(&self, fact_store: &SimpleDatabaseWithIndex<T>) -> SimpleDatabaseWithIndex<T> {
        return deltaify_idb_by_renaming(&self.deltaifying_program, fact_store);
    }

    fn nonrecursive_program(
        &self,
        fact_store: &SimpleDatabaseWithIndex<T>,
    ) -> SimpleDatabaseWithIndex<T> {
        return evaluate_rules_sequentially(&self.nonrecursive_program, fact_store);
    }

    fn recursive_program(
        &self,
        fact_store: &SimpleDatabaseWithIndex<T>,
    ) -> SimpleDatabaseWithIndex<T> {
        return evaluate_rules_sequentially(&self.recursive_program, fact_store);
    }
}

pub struct ParallelRelationalAlgebra {
    pub nonrecursive_program: Vec<(String, RelationalExpression)>,
    pub recursive_program: Vec<(String, RelationalExpression)>,
    pub deltaifying_program: SugaredProgram,
}

impl ParallelRelationalAlgebra {
    fn new(
        nonrecursive_program: &SugaredProgram,
        recursive_program: &SugaredProgram,
        deltaifying_program: &SugaredProgram,
    ) -> Self {
        return ParallelRelationalAlgebra {
            nonrecursive_program: key_program_by_symbol(nonrecursive_program),
            recursive_program: key_program_by_symbol(recursive_program),
            deltaifying_program: deltaifying_program.clone(),
        };
    }
}

impl<T: IndexBacking> ImmediateConsequenceOperator<SimpleDatabaseWithIndex<T>>
    for ParallelRelationalAlgebra
{
    fn deltaify_idb(&self, fact_store: &SimpleDatabaseWithIndex<T>) -> SimpleDatabaseWithIndex<T> {
        return deltaify_idb_by_renaming(&self.deltaifying_program, fact_store);
    }

    fn nonrecursive_program(
        &self,
        fact_store: &SimpleDatabaseWithIndex<T>,
    ) -> SimpleDatabaseWithIndex<T> {
        return evaluate_rules_in_parallel(&self.nonrecursive_program, fact_store);
    }

    fn recursive_program(
        &self,
        fact_store: &SimpleDatabaseWithIndex<T>,
    ) -> SimpleDatabaseWithIndex<T> {
        return evaluate_rules_in_parallel(&self.recursive_program, fact_store);
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
    dred: bool,
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
            dred: false,
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
    fn insert(&mut self, table: &str, row: UntypedRow) {
        self.insert_typed(table, ty_to_row(&row))
    }

    fn delete(&mut self, table: &str, row: &UntypedRow) {
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
    fn new_evaluation(
        &self,
        immediate_consequence_operator: Box<
            dyn ImmediateConsequenceOperator<SimpleDatabaseWithIndex<T>>,
        >,
    ) -> IncrementalEvaluation<SimpleDatabaseWithIndex<T>> {
        return IncrementalEvaluation::new(immediate_consequence_operator);
    }
    fn update_materialization(&mut self) {
        let evaluation = self.evaluate_program_bottom_up(&self.sugared_program.clone());

        evaluation.into_iter().for_each(|(symbol, relation)| {
            relation.into_iter().for_each(|row| {
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
        let deltaifier = deltaify_idb(program);
        let (nonrecursive, recursive) = if !self.dred {
            make_update_sne_programs(program)
        } else {
            make_sne_programs(program)
        };

        let programs: Vec<_> = [nonrecursive, recursive, deltaifier]
            .into_iter()
            .map(|sugared_program| {
                return idempotent_program_weak_intern(
                    &mut self.row_interner,
                    self.intern,
                    &sugared_program,
                );
            })
            .collect();

        let im_op = Box::new(ParallelRelationalAlgebra::new(
            &programs[0],
            &programs[1],
            &programs[2],
        ));
        let mut evaluation = self.new_evaluation(im_op);
        if !self.parallel {
            evaluation.immediate_consequence_operator = Box::new(RelationalAlgebra::new(
                &programs[0],
                &programs[1],
                &programs[2],
            ));
        }

        let now = Instant::now();
        evaluation.semi_naive(&self.fact_store);
        println!(
            "{{{}: {}}}",
            "inferencetime",
            now.elapsed().as_millis().to_string()
        );

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

        self.update_materialization();
    }

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
            self.dred = true;
            delete_rederive(self, &self.sugared_program.clone(), retractions);
            self.dred = false;
        }

        if additions.len() > 0 {
            additions.iter().for_each(|(sym, row)| {
                self.insert_typed(&format!("{}{}", DELTA_PREFIX, sym), row.clone());
            });

            self.update_materialization();

            additions.into_iter().for_each(|(sym, row)| {
                self.delete_typed(&format!("{}{}", DELTA_PREFIX, sym), &row);
                self.insert_typed(sym, row.clone());
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

    fn dump(&self) {
        todo!()
    }
}

impl<T: IndexBacking + PartialEq> Queryable for RelationalDatalog<T> {
    fn contains_row(&self, table: &str, row: &UntypedRow) -> bool {
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
