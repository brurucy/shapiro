use std::collections::{HashMap, VecDeque};
use std::fmt::{format, Display, Formatter};

use ordered_float::OrderedFloat;

use super::datalog::{self, constant_to_eq, duplicate_to_eq, Atom, Rule, TypedValue};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ColumnType {
    Str,
    Bool,
    UInt,
    Float(OrderedFloat<f64>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub ty: ColumnType,
    pub contents: Vec<datalog::TypedValue>,
}

impl Column {
    pub fn is_empty(&self) -> bool {
        return self.contents.is_empty();
    }
}

type Row = Vec<TypedValue>;

pub struct RowIterator {
    relation: Relation,
    row: usize,
}

impl Iterator for RowIterator {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.relation.columns.len() == 0 {
            return None;
        }

        if self.relation.columns.len() >= self.row {
            return None;
        }

        let row: Vec<TypedValue> = self
            .relation
            .columns
            .clone()
            .into_iter()
            .map(|column| column.contents[self.row].clone())
            .collect();

        self.row += 1;

        return Some(row.clone());
    }
}

type Database<'a> = HashMap<String, &'a Relation>;

#[derive(Clone, Debug, PartialEq)]
pub struct Relation {
    pub columns: Vec<Column>,
    pub symbol: String,
}

impl Relation {
    fn iter(&self) -> RowIterator {
        return RowIterator {
            relation: self.clone(),
            row: 0,
        };
    }
}

impl IntoIterator for Relation {
    type Item = Row;
    type IntoIter = RowIterator;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum SelectionTypedValue {
    Str(String),
    Bool(bool),
    UInt(u32),
    Column(usize),
    Float(OrderedFloat<f64>),
}

impl Display for SelectionTypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SelectionTypedValue::Str(inner) => write!(f, "{}", inner),
            SelectionTypedValue::Bool(inner) => write!(f, "{}", inner),
            SelectionTypedValue::UInt(inner) => write!(f, "{}u32", inner),
            SelectionTypedValue::Column(inner) => write!(f, "{}usize", inner),
            SelectionTypedValue::Float(inner) => {
                write!(f, "{}f64", inner)
            }
        }
    }
}

#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Term {
    Selection(usize, SelectionTypedValue),
    Projection(Vec<usize>),
    Relation(Atom),
    Product,
}

impl Display for Term {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Selection(column_index, typed_val) => {
                write!(f, "σ_{}={}", column_index, typed_val)
            }
            Term::Projection(column_indexes) => write!(
                f,
                "π_[{}]",
                column_indexes
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Term::Relation(atom) => write!(f, "{}", atom),
            Term::Product => write!(f, "{}", "×"),
        }
    }
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

#[derive(PartialEq, Debug, Clone)]
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

    fn set_root(&mut self, idx: usize) {
        self.root = Some(idx);
    }

    fn allocate(&mut self, value: Term) -> usize {
        let addr = self.arena.len();
        self.arena.push(ExpressionNode::new(addr, value));
        if let None = self.root {
            self.root = Some(addr)
        }
        addr
    }

    fn set_parent(&mut self, addr: usize, parent_addr: usize) {
        if self.arena.len() > addr && self.arena.len() > parent_addr {
            self.arena[addr].parent = Some(parent_addr);
        }
    }

    fn set_value(&mut self, addr: usize, value: &Term) {
        if self.arena.len() > addr {
            self.arena[addr].value = value.clone();
        }
    }

    fn set_left_child(&mut self, addr: usize, left_child_addr: usize) {
        if self.arena.len() > addr {
            self.arena[addr].left_child = Some(left_child_addr);
        }
    }

    fn set_right_child(&mut self, addr: usize, right_child_addr: usize) {
        if self.arena.len() > addr {
            self.arena[addr].right_child = Some(right_child_addr);
        }
    }

    fn to_string(&self) -> String {
        return if let Some(root_addr) = self.root {
            let root_node = self.arena[root_addr].clone();

            match root_node.value {
                Term::Relation(atom) => atom.to_string(),
                Term::Product => {
                    let mut left_subtree = self.clone();
                    left_subtree.set_root(root_node.left_child.unwrap());
                    let mut right_subtree = self.clone();
                    right_subtree.set_root(root_node.right_child.unwrap());

                    format!(
                        "{}({}, {})",
                        Term::Product.to_string(),
                        left_subtree.to_string(),
                        right_subtree.to_string()
                    )
                }
                unary_operators => {
                    let mut left_subtree = self.clone();
                    left_subtree.set_root(root_node.left_child.unwrap());

                    format!(
                        "{}({})",
                        unary_operators.to_string(),
                        left_subtree.to_string()
                    )
                }
            }
        } else {
            "".to_string()
        };
    }
}

impl From<&Rule> for ExpressionArena {
    fn from(rule: &Rule) -> Self {
        // Shifting complexity from the head to the body
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

        let mut body_iter = rule_body.into_iter().peekable();
        // Adding all products
        let mut previous_product_idx = 0usize;
        while let Some(atom) = body_iter.next() {
            if let Some(_) = body_iter.peek() {
                let product_idx = unsafe_arena.allocate(Term::Product);
                let current_relation_idx = unsafe_arena.allocate(Term::Relation(atom.clone()));

                unsafe_arena.set_left_child(product_idx, current_relation_idx);
                unsafe_arena.set_parent(current_relation_idx, product_idx);

                if product_idx != previous_product_idx {
                    unsafe_arena.set_right_child(previous_product_idx, product_idx);
                    unsafe_arena.set_parent(product_idx, previous_product_idx);
                }
                previous_product_idx = product_idx;
            } else {
                let current_relation_idx = unsafe_arena.allocate(Term::Relation(atom.clone()));

                if current_relation_idx != previous_product_idx {
                    unsafe_arena.set_right_child(previous_product_idx, current_relation_idx);
                    unsafe_arena.set_parent(current_relation_idx, previous_product_idx);
                }
            }
        }
        println!("{}", unsafe_arena.to_string());
        // Constant to selection
        let mut product_idx = 0;
        unsafe_arena.arena.clone().into_iter().for_each(|node| {
            if let Term::Relation(atom) = node.value {
                let mut new_atom = atom.clone();
                atom.terms.into_iter().enumerate().for_each(|(idx, term)| {
                    if let datalog::Term::Constant(typed_value) = term.clone() {
                        let selection: Term;
                        match typed_value.clone() {
                            datalog::TypedValue::Str(str_value) => {
                                selection = Term::Selection(
                                    product_idx,
                                    SelectionTypedValue::Str(str_value),
                                )
                            }
                            datalog::TypedValue::Bool(bool_value) => {
                                selection = Term::Selection(
                                    product_idx,
                                    SelectionTypedValue::Bool(bool_value),
                                )
                            }
                            datalog::TypedValue::UInt(uint_value) => {
                                selection = Term::Selection(
                                    product_idx,
                                    SelectionTypedValue::UInt(uint_value),
                                )
                            }
                            datalog::TypedValue::Float(float_value) => {
                                selection = Term::Selection(
                                    product_idx,
                                    SelectionTypedValue::Float(float_value),
                                )
                            }
                        }

                        let newvarsymbol = format!("?{}", typed_value);
                        let newvar = datalog::Term::Variable(newvarsymbol);

                        new_atom.terms[idx] = newvar;

                        let selection_node_id = unsafe_arena.allocate(selection);
                        unsafe_arena.set_left_child(selection_node_id, unsafe_arena.root.unwrap());
                        unsafe_arena.set_root(selection_node_id);
                    }
                    product_idx += 1;
                });
                unsafe_arena.set_value(node.idx, &Term::Relation(new_atom));
            }
        });
        // Equality to selection

        let relations = unsafe_arena.arena.clone().into_iter().enumerate().fold(
            vec![],
            |mut acc, (node_idx, node)| {
                if let Term::Relation(atom) = node.value {
                    acc.extend(
                        atom.terms
                            .into_iter()
                            .enumerate()
                            .map(|(term_idx, term)| (term, term_idx, node_idx)),
                    );
                }
                acc
            },
        );
        relations.clone().into_iter().enumerate().for_each(
            |(idx_outer, (term_outer, _term_outer_inner_idx, _outer_node_idx))| {
                relations.clone().into_iter().enumerate().for_each(
                    |(idx_inner, (term_inner, term_inner_inner_idx, inner_node_idx))| {
                        if idx_inner > idx_outer {
                            if let datalog::Term::Variable(symbol) = term_outer.clone() {
                                if term_outer == term_inner {
                                    let newvarsymbol = format!("{}{}", symbol.clone(), idx_inner);

                                    let newvar = datalog::Term::Variable(newvarsymbol.to_string());

                                    if let Term::Relation(ref mut atom) =
                                        unsafe_arena.arena[inner_node_idx].value
                                    {
                                        atom.terms[term_inner_inner_idx] = newvar
                                    }

                                    let selection_node_idx =
                                        unsafe_arena.allocate(Term::Selection(
                                            idx_outer,
                                            SelectionTypedValue::Column(idx_inner),
                                        ));
                                    unsafe_arena.set_left_child(
                                        selection_node_idx,
                                        unsafe_arena.root.unwrap(),
                                    );
                                    unsafe_arena.set_root(selection_node_idx);
                                }
                            }
                        }
                    },
                )
            },
        );

        let projection_idx = unsafe_arena.allocate(head_projection);
        unsafe_arena.set_left_child(projection_idx, unsafe_arena.root.unwrap());
        unsafe_arena.set_root(projection_idx);
        unsafe_arena
    }
}

mod test {
    use crate::models::relational_algebra::{ExpressionArena, ExpressionNode, Term};
    use crate::parsers::datalog::{parse_atom, parse_rule};

    #[test]
    fn test_rule_to_expression() {
        let rule = "HardcoreToTheMega(?x, ?z) <- [T(?x, ?y), T(?y, ?z), U(?y, hardcore)]";
        // x(T(?x, ?y), x(T(?y, ?z), T(?y, hardcore)))
        // [T(?x, ?y), T(?y1, ?z), U(?y2, ?Strhardcore), EQ(?y1, ?y), EQ(?y2, ?y))]
        let parsed_rule = parse_rule(rule);

        let expected_expression_arena = "π_[0, 3](σ_2=4usize(σ_1=4usize(σ_1=2usize(σ_5=hardcore(×(T(?x, ?y), ×(T(?y2, ?z), U(?y4, ?Strhardcore))))))))";
        let actual_expression_arena = ExpressionArena::from(&parsed_rule).to_string();
        assert_eq!(expected_expression_arena, actual_expression_arena)
    }
}
