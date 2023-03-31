use crate::misc::string_interning::Interner;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::num::NonZeroU32;

use crate::parsers::datalog::{parse_sugared_atom, parse_sugared_rule};

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

// Ty is a short-lived type used only to allow for the convenience of being able to use regular
// vanilla rust types.
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
            TypedValue::Str(inner) => write!(f, "{}", inner),
            TypedValue::Bool(inner) => write!(f, "{}", inner),
            TypedValue::UInt(inner) => write!(f, "{}", inner),
            TypedValue::Float(inner) => write!(f, "{}", inner),
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

pub type SugaredProgram = Vec<SugaredRule>;

// Used strictly for program transformations
#[derive(Clone, Ord, PartialOrd, Debug)]
pub struct SugaredAtom {
    pub terms: Vec<Term>,
    pub symbol: String,
    pub positive: bool,
}

// An atom is equivalent to another, if and only if they both have the same arity and all of their
// constants match in the same position
impl PartialEq for SugaredAtom {
    fn eq(&self, other: &Self) -> bool {
        if self.terms.len() != other.terms.len() {
            return false;
        }

        for i in 0..(self.terms).len() {
            match (&self.terms[i], &other.terms[i]) {
                (Term::Constant(left_inner), Term::Constant(right_inner)) => {
                    if left_inner != right_inner {
                        return false;
                    }
                }
                (Term::Variable(_), Term::Constant(_)) => return false,
                (Term::Constant(_), Term::Variable(_)) => return false,
                _ => (),
            };
        }

        return true;
    }
}

impl Eq for SugaredAtom {}

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

impl Default for SugaredAtom {
    fn default() -> Self {
        Self {
            terms: vec![],
            symbol: "".to_string(),
            positive: true,
        }
    }
}

impl Hash for SugaredAtom {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.symbol.hash(state);
        self.positive.hash(state);
        for term in &self.terms {
            match term {
                Term::Constant(inner) => inner.hash(state),
                // Identity hashing
                _ => 0.hash(state),
            }
        }
    }
}

#[derive(Clone, Ord, PartialOrd, Hash, Debug)]
pub struct SugaredRule {
    pub head: SugaredAtom,
    pub body: Vec<SugaredAtom>,
}

impl PartialEq for SugaredRule {
    fn eq(&self, other: &Self) -> bool {
        self.head == other.head && self.body == other.body
    }
}

impl Eq for SugaredRule {}

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

impl Default for SugaredRule {
    fn default() -> Self {
        Self {
            head: SugaredAtom::default(),
            body: vec![],
        }
    }
}

pub type Program = Vec<Rule>;

// Used for computation.
#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct Atom {
    pub terms: Vec<Term>,
    pub relation_id: NonZeroU32,
    pub positive: bool,
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
        self.positive.hash(state);
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
            positive: true,
        }
    }
}

impl Atom {
    #[allow(dead_code)]
    pub(crate) fn from_str_with_interner(str: &str, interner: &mut Interner) -> Self {
        let sugared_atom = parse_sugared_atom(str);

        return interner.intern_atom_weak(&sugared_atom);
    }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct Rule {
    pub head: Atom,
    pub body: Vec<Atom>,
}

impl Default for Rule {
    fn default() -> Self {
        Self {
            head: Atom::default(),
            body: vec![],
        }
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let body = self.body.iter().map(|atom| atom.to_string()).join(", ");
        write!(f, "{} <- [{}]", self.head, body)
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{SugaredAtom, SugaredRule};

    #[test]
    fn test_atom_eq() {
        let left_atom = SugaredAtom::from("T_rdf:type(?x, ?y)");
        let right_atom = SugaredAtom::from("T_rdf:type(?y, ?x)");

        assert_eq!(left_atom, right_atom)
    }

    #[test]
    fn test_rule_eq() {
        let left_rule = SugaredRule::from("T_rdf:type(?x, ?y) <- [T(?x, rdf:type, ?y)]");
        let right_rule = SugaredRule::from("T_rdf:type(?y, ?x) <- [T(?y, rdf:type, ?x)]");

        assert_eq!(left_rule, right_rule)
    }
}
