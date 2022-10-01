use ordered_float::{Float, OrderedFloat};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

use super::instance::Instance;

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum TypedValue {
    Str(String),
    Bool(bool),
    UInt(u32),
    Float(OrderedFloat<f64>),
}

impl Display for TypedValue {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TypedValue::Str(inner) => write!(f, "Str{}", inner),
            TypedValue::Bool(inner) => write!(f, "Bool{}", inner),
            TypedValue::UInt(inner) => write!(f, "UInt{}", inner),
            TypedValue::Float(inner) => write!(f, "Float{}", inner),
        }
    }
}

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum Term {
    Constant(TypedValue),
    Variable(String),
}

impl Display for Term {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Constant(value) => write!(f, "{}", value),
            Term::Variable(value) => write!(f, "{}", value),
        }
    }
}

pub type Substitutions = HashMap<String, TypedValue>;

#[derive(PartialEq, Eq, Clone, Debug, PartialOrd, Ord, Hash)]
pub enum Sign {
    Positive,
    Negative,
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct Atom {
    pub terms: Vec<Term>,
    pub symbol: String,
    pub sign: Sign,
}

impl Display for Atom {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let terms: String = self
            .terms
            .clone()
            .into_iter()
            .map(|term| term.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        let atom_representation: String = format!("({})", terms);

        write!(f, "{}{}", self.symbol, atom_representation)
    }
}

impl Hash for Atom {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.symbol.hash(state);
        self.sign.hash(state);
        for term in &self.terms {
            term.hash(state)
        }
    }
}

pub type Body = Vec<Atom>;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct Rule {
    pub head: Atom,
    pub body: Body,
}

pub trait BottomUpEvaluator {
    fn evaluate_program_bottom_up(&self, program: Vec<Rule>) -> Instance;
}

pub trait TopDownEvaluator {
    fn evaluate_program_top_down(&self, query: &Rule, program: Vec<Rule>) -> Instance;
}

pub fn remove_redundant_atoms(rule: &Rule) -> Rule {
    todo!()
}

pub fn constant_to_eq(rule: &Rule) -> Rule {
    let mut new_rule = rule.clone();

    rule.clone()
        .head
        .terms
        .into_iter()
        .enumerate()
        .for_each(|(idx, term)| {
            if let Term::Constant(typed_value) = term.clone() {
                let newvarsymbol = format!("?{}", typed_value.clone());

                let newvar = Term::Variable(newvarsymbol.to_string());

                new_rule.head.terms[idx] = newvar.clone();
                new_rule.body.push(Atom {
                    terms: vec![newvar, Term::Constant(typed_value)],
                    symbol: "EQ".to_string(),
                    sign: Sign::Positive,
                })
            }
        });
    new_rule
}

pub fn duplicate_to_eq(rule: &Rule) -> Rule {
    let mut new_rule = rule.clone();

    rule.clone()
        .head
        .terms
        .into_iter()
        .enumerate()
        .for_each(|(idx_outer, term_outer)| {
            rule.clone()
                .head
                .terms
                .into_iter()
                .enumerate()
                .for_each(|(idx_inner, term_inner)| {
                    if idx_inner > idx_outer {
                        if let Term::Variable(symbol) = term_outer.clone() {
                            if term_outer == term_inner
                                && new_rule.head.terms[idx_outer] == new_rule.head.terms[idx_inner]
                            {
                                let newvarsymbol = format!("{}{}", symbol.clone(), idx_inner);

                                let newvar = Term::Variable(newvarsymbol.to_string());

                                new_rule.head.terms[idx_inner] = newvar.clone();
                                new_rule.body.push(Atom {
                                    terms: vec![term_inner.clone(), newvar],
                                    symbol: "EQ".to_string(),
                                    sign: Sign::Positive,
                                })
                            }
                        }
                    };
                })
        });

    return new_rule;
}

#[cfg(test)]
mod tests {
    use crate::{models::datalog::duplicate_to_eq, parsers::datalog::parse_rule};

    use super::constant_to_eq;

    #[test]
    fn test_constant_pushdown() {
        let rule = parse_rule("T(?x, y) <- [T(?x, ?z)]");

        let constant_pushing_application = constant_to_eq(&rule);
        let expected_constant_pushed_rule = parse_rule("T(?x, ?Stry) <- [T(?x, ?z), EQ(?Stry, y)]");

        assert_eq!(constant_pushing_application, expected_constant_pushed_rule);
        assert_eq!(
            expected_constant_pushed_rule,
            constant_to_eq(&expected_constant_pushed_rule)
        )
    }

    #[test]
    fn test_duplicate_pushdown() {
        let rule = parse_rule("U(?x, ?x, ?x, ?x, y) <- [T(?x, ?z)]");

        let duplicate_pushing_application = duplicate_to_eq(&rule);
        let expected_duplicate_pushed_rule = parse_rule(
            "U(?x, ?x1, ?x2, ?x3, y) <- [T(?x, ?z), EQ(?x, ?x1), EQ(?x, ?x2), EQ(?x, ?x3)]",
        );

        assert_eq!(
            duplicate_pushing_application,
            expected_duplicate_pushed_rule
        );

        assert_eq!(
            expected_duplicate_pushed_rule,
            duplicate_to_eq(&expected_duplicate_pushed_rule)
        )
    }
}
