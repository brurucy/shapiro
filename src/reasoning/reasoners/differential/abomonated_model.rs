use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use abomonation_derive::Abomonation;
use crate::models::datalog::TypedValue;
use crate::reasoning::reasoners::differential::abomonated_parsing::{parse_atom, parse_rule};

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord, Abomonation )]
pub enum AbomonatedTypedValue {
    Str(String),
    Bool(bool),
    UInt(u32),
    InternedStr(usize),
}

impl From<TypedValue> for AbomonatedTypedValue {
    fn from(value: TypedValue) -> Self {
        return match value {
            TypedValue::Str(inner) => AbomonatedTypedValue::Str(inner),
            TypedValue::Bool(inner) => AbomonatedTypedValue::Bool(inner),
            TypedValue::UInt(inner) => AbomonatedTypedValue::UInt(inner),
            TypedValue::InternedStr(inner) => AbomonatedTypedValue::InternedStr(inner),
            _ => panic!("floats are not supported by differential reasoner!")
        }
    }
}

impl TryInto<u32> for AbomonatedTypedValue {
    type Error = ();

    fn try_into(self) -> Result<u32, Self::Error> {
        match self {
            AbomonatedTypedValue::UInt(inner) => Ok(inner),
            _ => Err(())
        }
    }
}

impl TryInto<String> for AbomonatedTypedValue {
    type Error = ();

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            AbomonatedTypedValue::Str(inner) => Ok(inner.to_string()),
            _ => Err(())
        }
    }
}

impl TryInto<bool> for AbomonatedTypedValue {
    type Error = ();

    fn try_into(self) -> Result<bool, Self::Error> {
        match self {
            AbomonatedTypedValue::Bool(inner) => Ok(inner),
            _ => Err(())
        }
    }
}

pub trait Ty {
    fn to_typed_value(&self) -> AbomonatedTypedValue;
}

impl Ty for String {
    fn to_typed_value(&self) -> AbomonatedTypedValue {
        return AbomonatedTypedValue::Str(self.to_string());
    }
}

impl Ty for &str {
    fn to_typed_value(&self) -> AbomonatedTypedValue {
        return AbomonatedTypedValue::Str(self.to_string());
    }
}

impl Ty for u32 {
    fn to_typed_value(&self) -> AbomonatedTypedValue {
        return AbomonatedTypedValue::UInt(self.clone());
    }
}

impl Ty for bool {
    fn to_typed_value(&self) -> AbomonatedTypedValue {
        return AbomonatedTypedValue::Bool(self.clone());
    }
}

impl Display for AbomonatedTypedValue {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            AbomonatedTypedValue::Str(inner) => write!(f, "Str{}", inner),
            AbomonatedTypedValue::Bool(inner) => write!(f, "Bool{}", inner),
            AbomonatedTypedValue::UInt(inner) => write!(f, "UInt{}", inner),
            AbomonatedTypedValue::InternedStr(inner) => write!(f, "IStr{}", inner)
        }
    }
}

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord, Abomonation)]
pub enum AbomonatedTerm {
    Constant(AbomonatedTypedValue),
    Variable(u8),
}

impl Into<AbomonatedTypedValue> for AbomonatedTerm {
    fn into(self) -> AbomonatedTypedValue {
        match self {
            AbomonatedTerm::Constant(constant) => constant,
            AbomonatedTerm::Variable(_name) => {
                panic!("cannot insert not-ground atom")
            }
        }
    }
}

impl Display for AbomonatedTerm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AbomonatedTerm::Constant(value) => write!(f, "{}", value),
            AbomonatedTerm::Variable(value) => write!(f, "{}", value),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, PartialOrd, Ord, Hash, Abomonation)]
pub enum AbomonatedSign {
    Positive,
    Negative,
}

#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord, Abomonation)]
pub struct AbomonatedAtom {
    pub terms: Vec<AbomonatedTerm>,
    pub symbol: String,
    pub sign: AbomonatedSign,
}

impl Display for AbomonatedAtom {
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

impl From<&str> for AbomonatedAtom {
    fn from(str: &str) -> Self {
        return parse_atom(str);
    }
}

impl Hash for AbomonatedAtom {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.symbol.hash(state);
        self.sign.hash(state);
        for term in &self.terms {
            term.hash(state)
        }
    }
}

pub type AbomonatedBody = Vec<AbomonatedAtom>;

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Abomonation)]
pub struct AbomonatedRule {
    pub head: AbomonatedAtom,
    pub body: AbomonatedBody,
}

impl From<&str> for AbomonatedRule {
    fn from(str: &str) -> Self {
        return parse_rule(str);
    }
}

impl Display for AbomonatedRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let body = self
            .body
            .iter()
            .map(|atom| atom.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{} <- [{}]", self.head, body)
    }
}