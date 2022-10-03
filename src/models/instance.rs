use std::collections::HashMap;

use crate::implementations::relational_algebra::evaluate;
use crate::models::datalog::{Atom, Ty};

use super::{
    datalog::TypedValue,
    relational_algebra::{ColumnType, Relation, RelationalExpression},
};

pub type Database = HashMap<String, Relation>;

pub enum IndexBacking {
    Hash,
    BTree,
    SkipList,
    Spine,
}

pub struct InstanceCfg {
    pub lazy_indexing: bool,
    pub index_backing: IndexBacking,
    pub joins: bool,
}

impl Default for InstanceCfg {
    fn default() -> Self {
        return InstanceCfg {
            lazy_indexing: false,
            index_backing: IndexBacking::BTree,
            joins: true,
        };
    }
}

#[derive(Clone, PartialEq)]
pub struct Instance {
    pub database: Database,
    lazy_index: bool,
}

impl Instance {
    pub fn insert(&mut self, table: &str, row: Vec<Box<dyn Ty>>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.insert(row)
        } else {
            let mut new_relation = Relation::new(&super::relational_algebra::RelationSchema {
                column_types: row
                    .iter()
                    .map(|row_value| match row_value.to_typed_value() {
                        TypedValue::Str(_) => ColumnType::Str,
                        TypedValue::Bool(_) => ColumnType::Bool,
                        TypedValue::UInt(_) => ColumnType::UInt,
                        TypedValue::Float(_) => ColumnType::OrderedFloat,
                    })
                    .collect(),
                symbol: table.to_string(),
            });
            new_relation.insert(row);
            self.database
                .insert(new_relation.symbol.clone(), new_relation);
        }
    }
    pub(crate) fn insert_typed(&mut self, table: &str, row: Vec<TypedValue>) {
        if let Some(relation) = self.database.get_mut(table) {
            relation.insert_typed(&row)
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
            new_relation.insert_typed(&row);
            self.database
                .insert(new_relation.symbol.clone(), new_relation);
        }
    }
    pub fn insert_relation(&mut self, relation: &Relation) {
        self.database
            .insert(relation.symbol.to_string(), relation.clone());
    }
    pub fn insert_atom(&mut self, atom: &Atom) {
        let row = (&atom.terms)
            .into_iter()
            .map(|term| term.clone().into())
            .collect();
        self.insert_typed(&atom.symbol.to_string(), row)
    }
    pub fn view(&self, table: &str) -> Vec<Vec<TypedValue>> {
        return if let Some(relation) = self.database.get(table) {
            relation.clone().into_iter().collect()
        } else {
            vec![]
        };
    }
    pub fn new() -> Self {
        return Self {
            database: HashMap::new(),
            lazy_index: false,
        };
    }
    pub fn new_cfg(cfg: InstanceCfg) -> Self {
        return Self {
            database: HashMap::new(),
            lazy_index: cfg.lazy_indexing,
        };
    }
    pub fn evaluate(&self, expression: &RelationalExpression, view_name: &str) -> Option<Relation> {
        return evaluate(expression, &self.database, view_name);
    }
}
