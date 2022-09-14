use std::collections::HashMap;

use ordered_float::OrderedFloat;

use super::datalog::{self, Rule};

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum Type {
    Str(String),
    Bool(bool),
    UInt(u32),
    Column(usize),
    Float(OrderedFloat<f64>),
}

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum Column {
    Name(String),
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct RelationValue {
    pub columns: Vec<Column>,
    pub symbol: String,
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Term {
    Selection(usize, Type),
    Projection(Vec<usize>),
    Relation(RelationValue),
    Product,
    Join,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExpressionNode {
    idx: usize,
    value: Term,
    parent: Option<usize>,
    left_child: Option<usize>,
    right_child: Option<usize>,
}

impl ExpressionNode {
    fn new(idx: usize, value: Term) -> Self {
        Self {
            idx,
            value,
            parent: None,
            left_child: None,
            right_child: None,
        }
    }
}

struct ExpressionArena {
    arena: Vec<ExpressionNode>,
}

impl ExpressionArena {
    fn new() -> Self {
        Self { arena: vec![] }
    }

    fn push(&mut self, value: Term) -> usize {
        for node in &self.arena {
            if node.value == value {
                return node.idx;
            }
        }
        let idx = self.arena.len();
        self.arena.push(ExpressionNode::new(idx, value));
        idx
    }

    fn get_node(&mut self, idx: usize) -> Option<Term> {
        if self.arena.len() > idx {
            return Some(self.arena[idx].value.clone());
        }
        None
    }

    fn set_left_child(&mut self, idx: usize, left_idx: usize) -> usize {
        if self.arena.len() > idx {
            self.arena[idx].left_child = Some(left_idx);
        }
        left_idx
    }

    fn set_right_child(&mut self, idx: usize, right_idx: usize) -> usize {
        if self.arena.len() > idx {
            self.arena[idx].right_child = Some(right_idx);
        }
        right_idx
    }

    fn get_children_idx(&mut self, idx: usize) -> (Option<usize>, Option<usize>) {
        if self.arena.len() > idx {
            let node = self.arena[idx].clone();
            return (node.left_child, node.right_child);
        }
        return (None, None);
    }
}

pub fn constant_eviction(rule: &Rule) -> Rule {
    let mut new_rule = rule.clone();

    rule.head.terms.into_iter().for_each(|term| {
        if let datalog::Term::Constant(typeValue) = term {
            new_rule.body.push(super::datalog::Atom {
                terms: vec![
                    datalog::Term::Variable(typeValue.to_string()),
                    datalog::Term::Constant(typeValue),
                ],
                symbol: "EQ".to_string(),
                sign: datalog::Sign::Positive,
            })
        }
    });
    new_rule
}

impl From<Rule> for ExpressionArena {
    fn from(rule: Rule) -> Self {
        return ExpressionArena::new();
    }
}
