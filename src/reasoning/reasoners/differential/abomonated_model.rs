use std::hash::Hash;
use abomonation_derive::Abomonation;
use crate::models::datalog::{Atom, Rule, Term, TypedValue};

// This duplication is necessary in order not to poison the original implementation
#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord, Abomonation)]
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

impl Into<TypedValue> for AbomonatedTypedValue {
    fn into(self) -> TypedValue {
        match self {
            AbomonatedTypedValue::Str(inner) => TypedValue::Str(inner),
            AbomonatedTypedValue::Bool(inner) => TypedValue::Bool(inner),
            AbomonatedTypedValue::UInt(inner) => TypedValue::UInt(inner),
            AbomonatedTypedValue::InternedStr(inner) => TypedValue::InternedStr(inner),
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

pub type AbomonatedAtom = (String, bool, Vec<AbomonatedTerm>);

impl From<Atom> for AbomonatedAtom {
    fn from(atom: Atom) -> AbomonatedAtom {
        let terms = atom
            .terms
            .into_iter()
            .map(|term| {
                match term {
                    Term::Constant(inner) => AbomonatedTerm::Constant(AbomonatedTypedValue::from(inner)),
                    Term::Variable(inner) => AbomonatedTerm::Variable(inner)
                }
            })
            .collect();
        
        return (atom.symbol, atom.sign, terms)
    }
}

pub type AbomonatedRule = (AbomonatedAtom, Vec<AbomonatedAtom>);

impl From<Rule> for AbomonatedRule {
    fn from(rule: Rule) -> Self {
        let head = AbomonatedAtom::from(rule.head);
        let body = rule.body.iter().map(|atom| AbomonatedAtom::from(atom.clone())).collect();

        return (head, body)
    }
}