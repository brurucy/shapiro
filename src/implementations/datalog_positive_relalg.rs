use crate::implementations::datalog_positive_infer::evaluate_rule;
use crate::models::datalog::{BottomUpEvaluator, Rule};
use crate::models::instance::Instance;
use crate::models::relational_algebra::RelationalExpression;

pub fn evaluate_program(knowledge_base: &Instance, program: Vec<Rule>) -> Instance {
    let mut previous_delta = Instance::new();
    let mut current_delta = Instance::new();
    let mut output = Instance::new();
    let relational_program: Vec<(String, RelationalExpression)> = program
        .iter()
        .map(|rule| (rule.head.symbol.to_string(), RelationalExpression::from(rule)))
        .collect();

    loop {
        previous_delta = current_delta.clone();
        let mut edb_plus_previous_delta = knowledge_base.clone();
        previous_delta
            .database
            .clone()
            .into_iter()
            .for_each(|relation| {
                relation
                    .1
                    .into_iter()
                    .for_each(|row| {
                        edb_plus_previous_delta.insert_typed(&relation.0, row)
                    })
            });
        current_delta = Instance::new();
        relational_program.clone().into_iter().for_each(|(symbol, expression)| {
            if let Some(rule_evaluation) = edb_plus_previous_delta.evaluate(&expression, &symbol) {
                rule_evaluation
                    .into_iter()
                    .for_each(|row| {
                        current_delta.insert_typed(&symbol, row.clone());
                        output.insert_typed(&symbol, row);
                })
            }
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
        return evaluate_program(&self.fact_store, program);
    }
}

impl Default for SimpleDatalog {
    fn default() -> Self {
        SimpleDatalog {
            fact_store: Instance::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{BottomUpEvaluator, Rule, Term, TypedValue};
    use std::collections::HashSet;
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


