use super::datalog;
use ordered_float::OrderedFloat;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord, Display)]
pub enum Type {
    Str(String),
    Bool(bool),
    UInt(u32),
    Float(OrderedFloat<f64>),
}

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum Term {
    Constant(Type),
    Variable(String),
}

pub type Substitutions = HashMap<String, Type>;

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

pub trait BottomUpEvaluator<I>
where
    I: IntoIterator<Item = Atom> + Clone + Default + Eq + FromIterator<Atom> + Extend<Atom>,
{
    fn evaluate_program_bottom_up(&self, program: Vec<Rule>) -> I;
}

pub trait TopDownEvaluator<I>
where
    I: IntoIterator<Item = Atom> + Clone + Default + Eq + FromIterator<Atom> + Extend<Atom>,
{
    fn evaluate_program_top_down(&self, query: &Rule, program: Vec<Rule>) -> I;
}
