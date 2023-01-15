use itertools::Itertools;
use ordered_float::OrderedFloat;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;

use crate::parsers::datalog::{parse_sugared_rule, parse_sugared_atom};

// TypedValue are the allowed types in the datalog model. Not canonical.
#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum TypedValue {
    Str(String),
    Bool(bool),
    UInt(u32),
    // Internal type, lives only inside the reasoner
    InternedStr(NonZeroU32),
    Float(OrderedFloat<f64>),
}

impl TryInto<u32> for TypedValue {
    type Error = ();

    fn try_into(self) -> Result<u32, Self::Error> {
        match self {
            TypedValue::UInt(inner) => Ok(inner),
            _ => Err(()),
        }
    }
}

impl TryInto<String> for TypedValue {
    type Error = ();

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            TypedValue::Str(inner) => Ok(inner.to_string()),
            _ => Err(()),
        }
    }
}

impl TryInto<bool> for TypedValue {
    type Error = ();

    fn try_into(self) -> Result<bool, Self::Error> {
        match self {
            TypedValue::Bool(inner) => Ok(inner),
            _ => Err(()),
        }
    }
}

impl TryInto<f64> for TypedValue {
    type Error = ();

    fn try_into(self) -> Result<f64, Self::Error> {
        match self {
            TypedValue::Float(inner) => Ok(inner.into_inner()),
            _ => Err(()),
        }
    }
}

impl Into<Box<dyn Ty>> for TypedValue {
    fn into(self) -> Box<dyn Ty> {
        return match self {
            TypedValue::Str(inner) => Box::new(inner),
            TypedValue::Bool(inner) => Box::new(inner),
            TypedValue::UInt(inner) => Box::new(inner),
            TypedValue::Float(inner) => Box::new(inner.into_inner()),
            _ => panic!("woopsie!"),
        };
    }
}

// Ty is a short-lived type used only to allow for the convenience of being able to use regular vanilla
// rust types.
pub trait Ty {
    fn to_typed_value(&self) -> TypedValue;
}

impl Ty for String {
    fn to_typed_value(&self) -> TypedValue {
        return TypedValue::Str(self.to_string());
    }
}

impl Ty for &str {
    fn to_typed_value(&self) -> TypedValue {
        return TypedValue::Str(self.to_string());
    }
}

impl Ty for u32 {
    fn to_typed_value(&self) -> TypedValue {
        return TypedValue::UInt(self.clone());
    }
}

impl Ty for bool {
    fn to_typed_value(&self) -> TypedValue {
        return TypedValue::Bool(self.clone());
    }
}

impl Ty for f64 {
    fn to_typed_value(&self) -> TypedValue {
        return TypedValue::Float(OrderedFloat(self.clone()));
    }
}

impl Display for TypedValue {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TypedValue::Str(inner) => write!(f, "Str{}", inner),
            TypedValue::Bool(inner) => write!(f, "Bool{}", inner),
            TypedValue::UInt(inner) => write!(f, "UInt{}", inner),
            TypedValue::Float(inner) => write!(f, "Float{}", inner),
            TypedValue::InternedStr(inner) => write!(f, "IStr{}", inner),
        }
    }
}

// A Term is either a Variable or a Constant
#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord)]
pub enum Term {
    Constant(TypedValue),
    Variable(u8),
}

impl Into<TypedValue> for Term {
    fn into(self) -> TypedValue {
        match self {
            Term::Constant(constant) => constant,
            Term::Variable(_name) => {
                panic!("cannot insert not-ground atom")
            }
        }
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Constant(value) => write!(f, "{}", value),
            Term::Variable(value) => write!(f, "?{}", value),
        }
    }
}


// Used strictly for program transformations
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct SugaredAtom {
    pub terms: Vec<Term>,
    pub symbol: String,
    pub positive: bool,
}

impl From<&str> for SugaredAtom {
    fn from(str: &str) -> Self {
        return parse_sugared_atom(str);
    }
}

impl Display for SugaredAtom {
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

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct SugaredRule {
    pub head: SugaredAtom,
    pub body: Vec<SugaredAtom>,
}

impl From<&str> for SugaredRule {
    fn from(str: &str) -> Self {
        return parse_sugared_rule(str);
    }
}

impl Display for SugaredRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let body = self.body.iter().map(|atom| atom.to_string()).join(", ");
        write!(f, "{} <- [{}]", self.head, body)
    }
}

pub type Program = Vec<Rule>;
pub type SugaredProgram = Vec<SugaredRule>;

// Used for computation.
#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct Atom {
    pub terms: Vec<Term>,
    pub relation_id: NonZeroU32,
    pub sign: bool,
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

        write!(f, "{}{}", self.relation_id, atom_representation)
    }
}

impl Hash for Atom {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.relation_id.hash(state);
        self.sign.hash(state);
        for term in &self.terms {
            term.hash(state)
        }
    }
}

impl Default for Atom {
    fn default() -> Self {
        Self {
            terms: vec![],
            relation_id: NonZeroU32::new(1).unwrap(),
            sign: false,
        }
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct Rule {
    pub head: Atom,
    pub body: Vec<Atom>
}

impl Default for Rule {
    fn default() -> Self {
        Self {
            head: Atom::default(),
            body: vec![],
        }
    }
}