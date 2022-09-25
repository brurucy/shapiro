use std::collections::HashSet;
use std::collections::{BTreeSet, HashMap};
use std::fmt::{Display, Formatter};

use ordered_float::OrderedFloat;

use super::datalog::{self, constant_to_eq, duplicate_to_eq, Atom, Rule, TypedValue};
use super::tree::Tree;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ColumnType {
    Str,
    Bool,
    UInt,
    OrderedFloat,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub ty: ColumnType,
    pub contents: Vec<TypedValue>,
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

        if self.relation.columns[0].contents.len() <= self.row {
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

#[derive(Clone, Debug, PartialEq)]
pub struct Index {
    pub index: BTreeSet<(TypedValue, usize)>,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Relation {
    pub columns: Vec<Column>,
    pub symbol: String,
    pub indexes: Vec<Index>,
}

pub struct RelationSchema {
    pub column_types: Vec<ColumnType>,
    pub symbol: String,
}

impl Relation {
    pub fn activate_index(&mut self, idx: usize) {
        self.indexes[idx].index.extend(
            self.columns[idx]
                .contents
                .clone()
                .into_iter()
                .enumerate()
                .map(|(row_id, row)| (row, row_id)),
        );
        self.indexes[idx].active = true
    }
    pub fn insert(&mut self, row: &Vec<TypedValue>) {
        let active_indexes: HashSet<usize> = self
            .indexes
            .clone()
            .into_iter()
            .enumerate()
            .filter(|(column_idx, idx)| idx.active)
            .map(|(column_idx, idx)| column_idx)
            .collect();

        row.into_iter()
            .enumerate()
            .for_each(|(column_idx, column_value)| {
                self.columns[column_idx].contents.push(column_value.clone());
                if active_indexes.contains(&column_idx) {
                    self.indexes[column_idx].index.insert((
                        column_value.clone(),
                        self.columns[column_idx].contents.len() - 1,
                    ));
                }
            });
    }
    fn iter(&self) -> RowIterator {
        return RowIterator {
            relation: self.clone(),
            row: 0,
        };
    }
    pub fn new(schema: &RelationSchema) -> Self {
        let columns = schema
            .column_types
            .clone()
            .into_iter()
            .map(|ty| {
                return (Column {
                    ty,
                    contents: vec![],
                });
            })
            .collect();

        let indexes = schema
            .column_types
            .clone()
            .into_iter()
            .map(|_ty| {
                return Index {
                    index: BTreeSet::new(),
                    active: false,
                };
            })
            .collect();

        Relation {
            columns,
            symbol: schema.symbol.to_string(),
            indexes,
        }
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

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum Term {
    Selection(usize, SelectionTypedValue),
    Projection(Vec<usize>),
    Relation(Atom),
    Product,
    Join(usize, usize),
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
            Term::Join(left_column_idx, right_column_idx) => {
                write!(f, "{}_{}={}", "⋈", left_column_idx, right_column_idx)
            }
        }
    }
}

pub type Expression = Tree<Term>;

// Paper: SkipList + Roaring Bitmap + BTree + Fenwick Indexed Tree
// 1: Figure out the join stuff, maybe undo selection pushdown
// 2: Make the join function
// 3: Make the index
// 4: Auto index. two-cases: bitmap and regular
pub fn select_product_to_join(expr: &Expression) -> Expression {
    let mut expr_local = expr.clone();
    let pre_order = expr.pre_order();

    let mut term_idx = 0;
    let terms = pre_order
        .clone()
        .into_iter()
        .fold(HashMap::new(), |mut acc, curr| {
            if let Term::Relation(atom) = curr.value {
                atom.terms.into_iter().for_each(|term| {
                    acc.insert(term, term_idx);
                    term_idx += 1;
                })
            }
            acc
        });

    let mut selection_set = BTreeSet::new();
    pre_order.into_iter().for_each(|node| match node.value {
        Term::Selection(_left_column_idx, SelectionTypedValue::Column(_right_column_idx)) => {
            selection_set.insert(node);
        }
        Term::Product => {
            let selection_nodes = selection_set.clone().into_iter();

            for selection_node in selection_nodes {
                if let Term::Selection(
                    left_column_idx,
                    SelectionTypedValue::Column(right_column_idx),
                ) = selection_node.value
                {
                    let left_column_idx = left_column_idx;
                    let right_column_idx = right_column_idx;

                    let left_child_address = node.left_child.unwrap();
                    let mut left_subtree = expr.clone();
                    left_subtree.set_root(left_child_address);
                    let left_pre_order = left_subtree.pre_order();
                    let left_term_idxs: BTreeSet<usize> = left_pre_order
                        .into_iter()
                        .filter(|node| {
                            if let Term::Relation(_) = node.value {
                                return true;
                            }
                            return false;
                        })
                        .map(|node| node.value)
                        .flat_map(|relation| {
                            if let Term::Relation(atom) = relation {
                                return atom
                                    .terms
                                    .into_iter()
                                    .map(|term| *terms.get(&term).unwrap())
                                    .collect();
                            }
                            return vec![];
                        })
                        .collect();

                    let right_child_address = node.right_child.unwrap();
                    let mut right_subtree = expr.clone();
                    right_subtree.set_root(right_child_address);
                    let right_pre_order = right_subtree.pre_order();
                    let right_term_idxs: BTreeSet<usize> = right_pre_order
                        .into_iter()
                        .filter(|node| {
                            if let Term::Relation(_) = node.value {
                                return true;
                            }
                            return false;
                        })
                        .map(|node| node.value)
                        .flat_map(|relation| {
                            if let Term::Relation(atom) = relation {
                                return atom
                                    .terms
                                    .into_iter()
                                    .map(|term| *terms.get(&term).unwrap())
                                    .collect();
                            }
                            return vec![];
                        })
                        .collect();

                    if left_term_idxs.contains(&left_column_idx)
                        && right_term_idxs.contains(&right_column_idx)
                    {
                        let left_column_idx = left_term_idxs
                            .into_iter()
                            .position(|x| x == left_column_idx)
                            .unwrap();

                        let right_column_idx = right_term_idxs
                            .into_iter()
                            .position(|x| x == right_column_idx)
                            .unwrap();

                        let join = Term::Join(left_column_idx, right_column_idx);

                        expr_local.set_value(node.idx, &join);

                        let selection_node = expr_local.arena[selection_node.idx].clone();
                        let selection_parent_idx = expr_local.arena[selection_node.idx].parent;
                        let parent_addr = selection_parent_idx.unwrap();
                        let parent = expr_local.arena[parent_addr].clone();
                        if let Some(left_child_addr) = parent.left_child {
                            if left_child_addr == selection_node.idx {
                                expr_local.arena[parent_addr].left_child =
                                    Some(selection_node.left_child.unwrap());
                            } else {
                                expr_local.arena[parent_addr].right_child =
                                    Some(selection_node.left_child.unwrap());
                            }
                        }
                        expr_local.delete(selection_node.idx);
                        selection_set.remove(&selection_node);
                        break;
                    }
                }
            }
        }
        _ => {}
    });

    return expr_local;
}

// The Expression. One of Guillaume le Million's greatest hits in Revachol was "Don't Worry (Your Pretty Little Head)". The Phoenix is one of the many nicknames of Guillaume le Million, considered Revachol's second greatest (male) disco artist.
fn rule_body_to_expression(rule: &Rule) -> Expression {
    let rule_body = rule.body.clone();

    let mut expression = Expression::new();

    let mut body_iter = rule_body.into_iter().peekable();

    let mut previous_product_idx = 0usize;
    while let Some(atom) = body_iter.next() {
        if let Some(_) = body_iter.peek() {
            let product_idx = expression.allocate(&Term::Product);
            let current_relation_idx = expression.allocate(&Term::Relation(atom.clone()));

            expression.set_left_child(product_idx, current_relation_idx);

            if product_idx != previous_product_idx {
                expression.set_right_child(previous_product_idx, product_idx);
            }
            previous_product_idx = product_idx;
        } else {
            let current_relation_idx = expression.allocate(&Term::Relation(atom.clone()));

            if current_relation_idx != previous_product_idx {
                expression.set_right_child(previous_product_idx, current_relation_idx);
            }
        }
    }

    return expression;
}

fn constant_to_selection(rule: &Rule, expr: &Expression) -> Expression {
    let mut expression = expr.clone();
    expression.arena.clone().into_iter().for_each(|node| {
        if let Term::Relation(atom) = node.value {
            let mut new_atom = atom.clone();
            atom.terms.into_iter().enumerate().for_each(|(idx, term)| {
                if let datalog::Term::Constant(typed_value) = term.clone() {
                    let selection: Term;
                    match typed_value.clone() {
                        TypedValue::Str(str_value) => {
                            selection = Term::Selection(idx, SelectionTypedValue::Str(str_value))
                        }
                        TypedValue::Bool(bool_value) => {
                            selection = Term::Selection(idx, SelectionTypedValue::Bool(bool_value))
                        }
                        TypedValue::UInt(uint_value) => {
                            selection = Term::Selection(idx, SelectionTypedValue::UInt(uint_value))
                        }
                        TypedValue::Float(float_value) => {
                            selection =
                                Term::Selection(idx, SelectionTypedValue::Float(float_value))
                        }
                    }

                    let newvarsymbol = format!("?{}", typed_value);
                    let newvar = datalog::Term::Variable(newvarsymbol);

                    new_atom.terms[idx] = newvar;

                    let selection_node_id = expression.allocate(&selection);

                    if let Some(parent_addr) = node.parent {
                        let parent = expression.arena[parent_addr].clone();
                        if let Some(left_child_addr) = parent.left_child {
                            if left_child_addr == node.idx {
                                expression.arena[parent_addr].left_child = Some(selection_node_id)
                            } else {
                                expression.arena[parent_addr].right_child = Some(selection_node_id)
                            }
                        }
                    }

                    expression.set_left_child(selection_node_id, node.idx);
                    if let Some(root_addr) = expression.root {
                        if root_addr == node.idx {
                            expression.set_root(selection_node_id)
                        }
                    }
                }
            });
            expression.set_value(node.idx, &Term::Relation(new_atom));
        }
    });
    return expression;
}

fn equality_to_selection(rule: &Rule, expr: &Expression) -> Expression {
    let mut expression = expr.clone();
    let relations = expression.arena.clone().into_iter().enumerate().fold(
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
                                    expression.arena[inner_node_idx].value
                                {
                                    atom.terms[term_inner_inner_idx] = newvar
                                }

                                let selection_node_idx = expression.allocate(&Term::Selection(
                                    idx_outer,
                                    SelectionTypedValue::Column(idx_inner),
                                ));
                                expression
                                    .set_left_child(selection_node_idx, expression.root.unwrap());
                                expression.set_root(selection_node_idx);
                            }
                        }
                    }
                },
            )
        },
    );
    return expression;
}

fn project_head(rule: &Rule) -> Term {
    let rule_body_terms: Vec<datalog::Term> = rule
        .body
        .clone()
        .into_iter()
        .flat_map(|body_atom| body_atom.terms.clone())
        .collect();

    let projected_head_indexes: Vec<usize> = rule
        .head
        .terms
        .clone()
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

    return Term::Projection(projected_head_indexes.clone());
}

impl From<&Rule> for Expression {
    fn from(rule: &Rule) -> Self {
        // Shifting complexity from the head to the body
        let constant_pushing_application = constant_to_eq(rule);
        let duplicate_to_eq_application = duplicate_to_eq(&constant_pushing_application);
        // Turning the body into products
        let products = rule_body_to_expression(&duplicate_to_eq_application);
        // Morphing relations with constants to selection equalities
        let products_and_selections =
            constant_to_selection(&duplicate_to_eq_application, &products);
        let mut expression =
            equality_to_selection(&duplicate_to_eq_application, &products_and_selections);
        // Projecting the head
        let projection_idx = expression.allocate(&project_head(&duplicate_to_eq_application));
        expression.set_left_child(projection_idx, expression.root.unwrap());
        expression.set_root(projection_idx);
        expression
    }
}

mod test {
    use crate::models::relational_algebra::{Expression, Term};
    use crate::parsers::datalog::{parse_atom, parse_rule};

    use super::select_product_to_join;

    #[test]
    fn test_rule_to_expression() {
        let rule = "HardcoreToTheMega(?x, ?z) <- [T(?x, ?y), T(?y, ?z), U(?y, hardcore)]";
        let parsed_rule = parse_rule(rule);

        let expected_expression_arena = "π_[0, 3](σ_2=4usize(σ_1=4usize(σ_1=2usize(×(T(?x, ?y), ×(T(?y2, ?z), σ_1=hardcore(U(?y4, ?Strhardcore))))))))";

        let actual_expression_arena = Expression::from(&parsed_rule).to_string();
        assert_eq!(expected_expression_arena, actual_expression_arena)
    }

    #[test]
    fn test_select_product_to_join() {
        let rule = "HardcoreToTheMega(?x, ?z) <- [T(?x, ?y), T(?y, ?z), U(?y, hardcore)]";
        let parsed_rule = parse_rule(rule);

        let expected_expression_arena = "π_[0, 3](σ_1=4usize(⋈_1=0(T(?x, ?y), ⋈_0=0(T(?y2, ?z), σ_1=hardcore(U(?y4, ?Strhardcore))))))";

        let rule_to_expression = Expression::from(&parsed_rule);
        let actual_expression = select_product_to_join(&rule_to_expression);
        let actual_expression_to_string = actual_expression.to_string();

        assert_eq!(expected_expression_arena, actual_expression_to_string)
    }
}
