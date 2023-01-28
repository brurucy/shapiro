use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::num::NonZeroU32;
use abomonation_derive::Abomonation;
use itertools::Itertools;
use crate::models::datalog::{Atom, Rule, Term, TypedValue};

#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord, Abomonation)]
pub enum AbomonatedTypedValue {
    //Str(String),
    Bool(bool),
    UInt(u32),
    InternedStr(NonZeroU32),
}

impl Display for AbomonatedTypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AbomonatedTypedValue::Bool(inner) => write!(f, "{}", inner),
            AbomonatedTypedValue::UInt(inner) => write!(f, "{}", inner),
            AbomonatedTypedValue::InternedStr(inner) => write!(f, "Is{}", inner)
        }
    }
}

impl From<TypedValue> for AbomonatedTypedValue {
    fn from(value: TypedValue) -> Self {
        return match value {
            //TypedValue::Str(inner) => AbomonatedTypedValue::Str(inner),
            TypedValue::Bool(inner) => AbomonatedTypedValue::Bool(inner),
            TypedValue::UInt(inner) => AbomonatedTypedValue::UInt(inner),
            TypedValue::InternedStr(inner) => AbomonatedTypedValue::InternedStr(inner),
            _ => panic!("floats and strings are not supported by differential reasoner!")
        };
    }
}

impl Into<TypedValue> for AbomonatedTypedValue {
    fn into(self) -> TypedValue {
        match self {
            //AbomonatedTypedValue::Str(inner) => TypedValue::Str(inner),
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

impl Display for AbomonatedTerm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AbomonatedTerm::Constant(inner) => write!(f, "Const{}", inner),
            AbomonatedTerm::Variable(inner) => write!(f, "Var{}", inner)
        }
    }
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

pub type AbomonatedAtom = (NonZeroU32, bool, Vec<AbomonatedTerm>);
pub type MaskedAtom = (NonZeroU32, Vec<Option<AbomonatedTypedValue>>);

pub fn mask(aboatom: &AbomonatedAtom) -> MaskedAtom {
    let sym = aboatom.0;

    let out = aboatom
        .2
        .iter()
        .map(|term| match term {
            AbomonatedTerm::Constant(inner) => Some(inner.clone()),
            AbomonatedTerm::Variable(_) => None
        })
        .collect();

    return (sym, out);
}

pub fn permute_mask(masked_atom: MaskedAtom) -> impl Iterator<Item=MaskedAtom> {
    let sym = masked_atom.0;
    let arity = masked_atom.1.len();

    masked_atom
        .1
        .into_iter()
        .enumerate()
        .filter(|(_idx, possibly_some)| *possibly_some != None)
        .powerset()
        .map(move |x| {
            let mut vec = vec![None; arity];

            x
                .iter()
                .for_each(|(idx, value)| {
                    vec[*idx] = (*value).clone()
                });

            return (sym, vec);
        })
}

pub fn abomonate_atom(atom: Atom) -> AbomonatedAtom {
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

    return (atom.relation_id, atom.sign, terms);
}

pub type AbomonatedRule = (AbomonatedAtom, Vec<AbomonatedAtom>);

pub fn abomonate_rule(rule: Rule) -> AbomonatedRule {
    let head = abomonate_atom(rule.head);
    let body = rule.body.iter().map(|atom| abomonate_atom(atom.clone())).collect();

    return (head, body)
}