use std::time::Instant;
use colored::Colorize;
use crate::misc::helpers::{
    idempotent_intern, idempotent_program_strong_intern, idempotent_program_weak_intern, ty_to_row,
};
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, SugaredProgram, SugaredRule, Ty};
use crate::models::instance::{Database, HashSetDatabase};
use crate::models::reasoner::{BottomUpEvaluator, Diff, Dynamic, DynamicTyped, EvaluationResult, Materializer, Queryable, RelationDropper, UntypedRow};
use crate::models::relational_algebra::Row;
use crate::reasoning::algorithms::delete_rederive::delete_rederive;
use crate::reasoning::algorithms::delta_rule_rewrite::{deltaify_idb, make_sne_programs};
use crate::reasoning::algorithms::evaluation::{IncrementalEvaluation, ImmediateConsequenceOperator};
use crate::reasoning::algorithms::rewriting::evaluate_rule;
use lasso::{Key, Spur};
use rayon::prelude::*;

pub fn evaluate_rules_sequentially(program: &Program, instance: &HashSetDatabase) -> HashSetDatabase {
    let mut out: HashSetDatabase = Default::default();

    program
        .iter()
        .for_each(|rule| {
            if let Some(eval) = evaluate_rule(&instance, &rule) {
                eval.into_iter().for_each(|row| out.insert_at(rule.head.relation_id.get(), row))
            }
        });

    return out;
}

pub fn evaluate_rules_in_parallel(program: &Program, instance: &HashSetDatabase) -> HashSetDatabase {
    let mut out: HashSetDatabase = Default::default();

    program
        .par_iter()
        .filter_map(|rule| {
            if let Some(eval) = evaluate_rule(instance, &rule) {
                return Some((rule.head.relation_id.get(), eval));
            }

            return None;
        })
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|(relation_id, eval)| {
            eval.into_iter()
                .for_each(|row| out.insert_at(relation_id, row))
        });

    return out;
}

pub struct Rewriting {
    pub nonrecursive_program: Program,
    pub recursive_program: Program,
    pub deltaifying_program: Program,
}

impl Rewriting {
    fn new(
        nonrecursive_program: &Program,
        recursive_program: &Program,
        deltaifying_program: &Program,
    ) -> Self {
        return Rewriting {
            nonrecursive_program: nonrecursive_program.clone(),
            recursive_program: recursive_program.clone(),
            deltaifying_program: deltaifying_program.clone(),
        };
    }
}

impl ImmediateConsequenceOperator<HashSetDatabase> for Rewriting {
    fn deltaify_idb(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return deltaify_idb_by_renaming(&self.deltaifying_program, fact_store)
    }

    fn nonrecursive_program(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return evaluate_rules_sequentially(&self.nonrecursive_program, fact_store);
    }

    fn recursive_program(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return evaluate_rules_sequentially(&self.recursive_program, fact_store);
    }
}

pub struct ParallelRewriting {
    pub nonrecursive_program: Program,
    pub recursive_program: Program,
    pub deltaifying_program: Program,
}

impl ParallelRewriting {
    fn new(
        nonrecursive_program: &Program,
        recursive_program: &Program,
        deltaifying_program: &Program,
    ) -> Self {
        return ParallelRewriting {
            nonrecursive_program: nonrecursive_program.clone(),
            recursive_program: recursive_program.clone(),
            deltaifying_program: deltaifying_program.clone(),
        };
    }
}

pub fn deltaify_idb_by_renaming(
    deltaify_idb_program: &Program,
    fact_store: &HashSetDatabase,
) -> HashSetDatabase {

    let mut out = fact_store.clone();
    deltaify_idb_program
        .iter()
        .for_each(|rule| {
            if let Some(relation) = fact_store.storage.get(&(rule.body[0].relation_id.get())) {
                out.storage.insert(rule.head.relation_id.get(), relation.clone());
            }
        });

    return out
}

impl ImmediateConsequenceOperator<HashSetDatabase> for ParallelRewriting {
    fn deltaify_idb(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return deltaify_idb_by_renaming(&self.deltaifying_program, fact_store)
    }

    fn nonrecursive_program(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return evaluate_rules_in_parallel(&self.nonrecursive_program, fact_store);
    }

    fn recursive_program(&self, fact_store: &HashSetDatabase) -> HashSetDatabase {
        return evaluate_rules_in_parallel(&self.recursive_program, fact_store);
    }
}

pub struct ChibiDatalog {
    pub fact_store: HashSetDatabase,
    pub(crate) interner: Interner,
    parallel: bool,
    intern: bool,
    program: Program,
    sugared_program: SugaredProgram,
}

impl Default for ChibiDatalog {
    fn default() -> Self {
        ChibiDatalog {
            fact_store: Default::default(),
            interner: Default::default(),
            parallel: true,
            intern: true,
            program: vec![],
            sugared_program: vec![],
        }
    }
}

impl ChibiDatalog {
    pub fn new(parallel: bool, intern: bool) -> Self {
        return Self {
            parallel,
            intern,
            ..Default::default()
        };
    }
    fn new_evaluation(
        &self,
        immediate_consequence_operator: Box<dyn ImmediateConsequenceOperator<HashSetDatabase>>
    ) -> IncrementalEvaluation<HashSetDatabase> {
        return IncrementalEvaluation::new(immediate_consequence_operator);
    }
    fn update_materialization(&mut self) {
        let evaluation = self.evaluate_program_bottom_up(&self.sugared_program.clone());

        evaluation
            .into_iter()
            .for_each(|(symbol, relation)| {
                relation.into_iter().for_each(|row| {
                    self.insert_typed(&symbol, row);
                });
            });
    }
}

impl Dynamic for ChibiDatalog {
    fn insert(&mut self, table: &str, row: UntypedRow) {
        self.insert_typed(table, ty_to_row(&row))
    }

    fn delete(&mut self, table: &str, row: &UntypedRow) {
        self.delete_typed(table, &ty_to_row(row))
    }
}

impl DynamicTyped for ChibiDatalog {
    fn insert_typed(&mut self, table: &str, row: Row) {
        let (relation_id, typed_row) =
            idempotent_intern(&mut self.interner, self.intern, table, row);

        self.fact_store.insert_at(relation_id, typed_row)
    }
    fn delete_typed(&mut self, table: &str, row: &Row) {
        let (relation_id, typed_row) =
            idempotent_intern(&mut self.interner, self.intern, table, row.clone());

        self.fact_store.delete_at(relation_id, &typed_row)
    }
}

impl BottomUpEvaluator for ChibiDatalog {
    fn evaluate_program_bottom_up(&mut self, program: &Vec<SugaredRule>) -> EvaluationResult {
        let deltaifier = deltaify_idb(program);
        let (nonrecursive, recursive) = make_sne_programs(program);

        let programs: Vec<_> = [nonrecursive, recursive, deltaifier]
            .into_iter()
            .map(|sugared_program| {
                return idempotent_program_strong_intern(&mut self.interner, self.intern, &sugared_program);
            })
            .collect();

        let im_op = Box::new(ParallelRewriting::new(&programs[0], &programs[1], &programs[2]));
        let mut evaluation = self.new_evaluation(im_op);
        if !self.parallel {
            evaluation.immediate_consequence_operator = Box::new(Rewriting::new(&programs[0], &programs[1], &programs[2]));
        }

        let now = Instant::now();
        evaluation.semi_naive(&self.fact_store);
        println!("{} {}", "inference time:".green(), now.elapsed().as_millis().to_string().green());

        return evaluation.output.storage.into_iter().fold(
            Default::default(),
            |mut acc: EvaluationResult, (relation_id, row_set)| {
                let spur = Spur::try_from_usize(relation_id as usize - 1).unwrap();
                let sym = self.interner.rodeo.resolve(&spur);

                acc.insert(sym.to_string(), row_set);
                acc
            },
        );
    }
}

impl Materializer for ChibiDatalog {
    fn materialize(&mut self, program: &SugaredProgram) {
        idempotent_program_weak_intern(&mut self.interner, self.intern, program)
            .into_iter()
            .for_each(|sugared_rule| self.sugared_program.push(sugared_rule));

        self.program = self
            .sugared_program
            .iter()
            .map(|sugared_rule| self.interner.intern_rule_weak(&sugared_rule))
            .collect();

        self.update_materialization()
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
            delete_rederive(self, &self.sugared_program.clone(), retractions)
        }

        if additions.len() > 0 {
            additions.into_iter().for_each(|(sym, row)| {
                self.insert_typed(sym, row);
            });

            self.update_materialization();
        }
    }

    fn triple_count(&self) -> usize {
        let size: usize = self
            .fact_store
            .storage
            .iter()
            .map(|(_sym, rel)| return rel.len())
            .sum();

        return size;
    }
}

impl RelationDropper for ChibiDatalog {
    fn drop_relation(&mut self, table: &str) {
        let sym = self.interner.rodeo.get_or_intern(table);

        self.fact_store.storage.remove(&sym.into_inner().get());
    }
}

impl Queryable for ChibiDatalog {
    fn contains_row(&self, table: &str, row: &UntypedRow) -> bool {
        if let Some(relation_id) = self.interner.rodeo.get(table) {
            let mut typed_row = ty_to_row(row);
            if self.intern {
                if let Some(existing_typed_row) = self.interner.try_intern_row(&typed_row) {
                    typed_row = existing_typed_row
                } else {
                    return false;
                }
            }
            return self
                .fact_store
                .storage
                .get(&relation_id.into_inner().get())
                .unwrap()
                .contains(&typed_row);
        }

        return false;
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{SugaredRule, TypedValue};
    use crate::models::reasoner::{BottomUpEvaluator, Dynamic, Materializer, Queryable};
    use crate::models::relational_algebra::Row;
    use crate::reasoning::reasoners::chibi::ChibiDatalog;
    use indexmap::IndexSet;

    #[test]
    fn test_chibi_operations() {
        let mut reasoner: ChibiDatalog = ChibiDatalog::new(false, false);

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
    fn test_chibi_datalog() {
        let mut reasoner: ChibiDatalog = ChibiDatalog::new(false, false);
        reasoner.insert("edge", vec![Box::new("a"), Box::new("b")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("c")]);
        reasoner.insert("edge", vec![Box::new("b"), Box::new("d")]);

        let query = vec![
            SugaredRule::from("reachable(?x, ?y) <- [edge(?x, ?y)]"),
            SugaredRule::from("reachable(?x, ?z) <- [reachable(?x, ?y), reachable(?y, ?z)]"),
        ];

        let new_tuples = reasoner
            .evaluate_program_bottom_up(&query)
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
