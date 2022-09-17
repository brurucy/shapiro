use std::borrow::Borrow;
use crate::models::datalog::TypedValue;
use crate::models::relational_algebra::{Column, SelectionTypedValue};

pub fn select_value(column: &Column, value: SelectionTypedValue) -> Column {
    return Column {
        ty: column.ty,
        contents: column
            .contents
            .clone()
            .into_iter()
            .filter(|item| {
                match item {
                    TypedValue::Str(outer) => {
                        match value {
                            SelectionTypedValue::Str(inner) => outer == inner,
                            _ => false
                        }
                        true
                    }
                    TypedValue::Bool(outer) => {
                        match value {
                            SelectionTypedValue::Bool(inner) => outer == inner,
                            _ => false
                        }
                    }
                    TypedValue::UInt(outer) => {
                        match value {
                            SelectionTypedValue::UInt(inner) => outer == inner,
                            _ => false
                        }
                    }
                    TypedValue::Float(outer) => {
                        match value {
                            SelectionTypedValue::Float(inner) => outer == inner,
                            _ => false
                        }
                    }
                }
            })
            .collect()
    }
}