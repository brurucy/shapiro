use std::collections::HashMap;

use crate::implementations::relational_algebra::evaluate;

use super::{
    datalog::TypedValue,
    relational_algebra::{ColumnType, Expression, Relation},
};

pub type Database = HashMap<String, Relation>;

#[derive(Clone, PartialEq)]
pub struct Instance {
    pub database: Database,
}

impl Instance {
    pub fn insert(&mut self, table: &str, row: Vec<TypedValue>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.insert(&row)
        } else {
            let mut new_relation = Relation::new(&super::relational_algebra::RelationSchema {
                column_types: row
                    .clone()
                    .into_iter()
                    .map(|row_value| match row_value {
                        TypedValue::Str(_) => ColumnType::Str,
                        TypedValue::Bool(_) => ColumnType::Bool,
                        TypedValue::UInt(_) => ColumnType::UInt,
                        TypedValue::Float(_) => ColumnType::OrderedFloat,
                    })
                    .collect(),
                symbol: table.to_string(),
            });
            new_relation.insert(&row);
            self.database
                .insert(new_relation.symbol.clone(), new_relation);
        }
    }
    pub fn insert_relation(&mut self, relation: &Relation) {
        self.database
            .insert(relation.symbol.to_string(), relation.clone());
    }
    pub fn view(&self, table: &str) -> Vec<Vec<TypedValue>> {
        if let Some(relation) = self.database.get(table) {
            return relation.clone().into_iter().collect();
        } else {
            return vec![];
        }
    }
    pub fn new() -> Self {
        return Self {
            database: HashMap::new(),
        };
    }
    pub fn evaluate(&self, expression: &Expression, view_name: &str) -> Relation {
        return evaluate(expression, self.database.clone(), view_name);
    }
}
