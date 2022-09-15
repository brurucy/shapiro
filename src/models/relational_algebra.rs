use ordered_float::OrderedFloat;

use super::datalog::{self, constant_to_eq, duplicate_to_eq, Atom, Rule};

type Relation = Atom;

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum SelectionTypedValue {
    Str(String),
    Bool(bool),
    UInt(u32),
    Column(usize),
    Float(OrderedFloat<f64>),
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Term {
    Selection(usize, SelectionTypedValue),
    Projection(Vec<usize>),
    Relation(Relation),
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

pub struct ExpressionArena {
    arena: Vec<ExpressionNode>,
    root: Option<usize>,
}

impl ExpressionArena {
    fn new() -> Self {
        Self {
            arena: vec![],
            root: None,
        }
    }

    fn set_root(&mut self, idx: usize) -> usize {
        self.root = Some(idx);
        idx
    }

    fn push(&mut self, value: Term) -> usize {
        for node in &self.arena {
            if node.value == value {
                return node.idx;
            }
        }
        let idx = self.arena.len();
        self.arena.push(ExpressionNode::new(idx, value));
        if let None = self.root {
            self.root = Some(idx)
        }
        idx
    }

    fn get_node(&mut self, idx: usize) -> Option<Term> {
        if self.arena.len() > idx {
            return Some(self.arena[idx].value.clone());
        }
        None
    }

    fn set_parent(&mut self, idx: usize, parent: usize) -> usize {
        if self.arena.len() > idx && self.arena.len() > parent {
            self.arena[idx].parent = Some(parent);
        }
        return idx;
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

impl From<&Rule> for ExpressionArena {
    fn from(rule: &Rule) -> Self {
        let constant_pushing_application = constant_to_eq(rule);
        let duplicate_to_eq_application = duplicate_to_eq(&constant_pushing_application);

        let rule_body_terms: Vec<super::datalog::Term> = duplicate_to_eq_application
            .body
            .clone()
            .into_iter()
            .flat_map(|body_atom| body_atom.terms.clone())
            .collect();

        let projected_head_indexes: Vec<usize> = duplicate_to_eq_application
            .head
            .terms
            .into_iter()
            .map(|head_term| {
                Option::unwrap(
                    rule_body_terms
                        .clone()
                        .into_iter()
                        .position(|term| term == head_term),
                )
            })
            .collect();

        let head_projection = Term::Projection(projected_head_indexes.clone());

        let rule_body = duplicate_to_eq_application.body.clone();

        let mut unsafe_arena = ExpressionArena::new();

        // Adding the products
        let mut body_iter = rule_body.into_iter().peekable();
        while let Some(atom) = body_iter.next() {
            if let Some(_) = body_iter.peek() {
                let product_idx = unsafe_arena.push(Term::Product);
                let current_relation_idx = unsafe_arena.push(Term::Relation(atom.clone()));

                unsafe_arena.set_left_child(product_idx, current_relation_idx);
                unsafe_arena.set_parent(current_relation_idx, product_idx);

                let previous_product_idx = product_idx - 2;
                if previous_product_idx > 0 {
                    unsafe_arena.set_right_child(previous_product_idx, product_idx);
                    unsafe_arena.set_parent(product_idx, previous_product_idx);
                }
            } else {
                let current_relation_idx = unsafe_arena.push(Term::Relation(atom.clone()));

                let previous_product_idx = current_relation_idx - 2;
                if previous_product_idx > 0 {
                    unsafe_arena.set_right_child(previous_product_idx, current_relation_idx);
                    unsafe_arena.set_parent(current_relation_idx, previous_product_idx);
                }
            }
        }
        // Constant to selection
        unsafe_arena.arena = unsafe_arena
            .arena
            .into_iter()
            .map(|x| {
                {
                    if let Term::Relation(atom) = x.value {
                        atom.terms.into_iter(|term| {
                            if let super::datalog::Term::Constant(typed_value) = term {
                                let newvarsymbol = format!("?{}", typed_value.clone())

                                let newvar = super::datalog::Term::Variable(f);
                            }
                        })
                    }
                }
                x
            })
            .collect();

        // Duplicate to selection

        let projection_idx = unsafe_arena.push(head_projection);
        unsafe_arena.set_root(projection_idx);
        unsafe_arena
    }
}
