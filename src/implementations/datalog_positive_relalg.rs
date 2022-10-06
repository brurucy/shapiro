use std::time::Instant;
use crate::implementations::datalog_positive_infer::evaluate_rule;
use crate::models::datalog::{BottomUpEvaluator, Rule};
use crate::models::instance::Instance;
use crate::models::relational_algebra::{Relation, RelationalExpression};
use rayon::prelude::*;
use crate::implementations::rule_graph::{generate_rule_dependency_graph, stratify};

pub fn evaluate_program(knowledge_base: &Instance, program: Vec<Rule>) -> Instance {
    let mut previous_delta = Instance::new(knowledge_base.use_indexes);
    let mut current_delta = Instance::new(knowledge_base.use_indexes);
    let mut output = Instance::new(knowledge_base.use_indexes);
    let relational_program: Vec<(String, RelationalExpression, String)> = program
        .iter()
        .map(|rule| (rule.head.symbol.to_string(), RelationalExpression::from(rule), RelationalExpression::from(rule).to_string()))
        .collect();

    loop {
        previous_delta = current_delta.clone();
        let mut edb_plus_previous_delta = knowledge_base.clone();
        previous_delta
            .database
            .iter()
            .for_each(|relation| {
                relation
                    .1
                    .ward
                    .iter()
                    .for_each(|(row, notdeleted)| {
                        if *notdeleted {
                            edb_plus_previous_delta.insert_typed(&relation.0, row.clone())
                        }
                    })
            });
        current_delta = Instance::new(knowledge_base.use_indexes);
        let evals: Vec<Relation> = relational_program
            .clone()
            .into_par_iter()
            .filter_map(|(symbol, expression, repr)| {
                println!("evaluating: {}", repr);
                return edb_plus_previous_delta.evaluate(&expression, &symbol)
            })
            .collect();

        evals
            .iter()
                    .for_each(|relation| {
                        relation
                            .ward
                            .iter()
                            .for_each(|(row, notdeleted)| {
                                if *notdeleted {
                                    current_delta.insert_typed(&relation.symbol, row.clone());
                                    output.insert_typed(&relation.symbol, row.clone());
                                }
                            })
                    });

        if previous_delta == current_delta {
            break;
        }
    }

    return output;
}

pub struct SimpleDatalog {
    pub fact_store: Instance
}

impl BottomUpEvaluator for SimpleDatalog {
    fn evaluate_program_bottom_up(&self, program: Vec<Rule>) -> Instance {
        let rule_graph = generate_rule_dependency_graph(&program);
        let (_valid, stratification) = stratify(&rule_graph);
        let program = stratification.iter().flatten().cloned().cloned().collect();

        return evaluate_program(&self.fact_store, program);
    }
    // fn evaluate_program_bottom_up(&self, program: Vec<Rule>) -> Instance {
    //     let rule_graph = generate_rule_dependency_graph(&program);
    //     let (_valid, stratification) = stratify(&rule_graph);
    //
    //     let mut output = Instance::new(false);
    //
    //     stratification
    //         .iter()
    //         .for_each(|program| {
    //             evaluate_program(&self.fact_store, program.iter().cloned().cloned().collect())
    //                 .database
    //                 .iter()
    //                 .for_each(|relation| {
    //                     relation
    //                         .1
    //                         .ward
    //                         .iter()
    //                         .for_each(|(row, sign)| {
    //                             if *sign {
    //                                 output.insert_typed(relation.0, row.clone());
    //                             }
    //                         })
    //                 });
    //         });
    //
    //     return output
    // }
}

impl Default for SimpleDatalog {
    fn default() -> Self {
        SimpleDatalog {
            fact_store: Instance::new(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{BottomUpEvaluator, Rule, Term, TypedValue};
    use std::collections::HashSet;
    use std::ops::Deref;
    use crate::implementations::datalog_positive_relalg::SimpleDatalog;

    #[test]
    fn test_simple_datalog() {
        let mut reasoner: SimpleDatalog = Default::default();
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


