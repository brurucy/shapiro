use std::fmt::{Display, Formatter};
use std::hash::Hash;
use abomonation_derive::Abomonation;
use itertools::Itertools;
use crate::models::datalog::{Atom, Rule, Term, TypedValue};

// This duplication is necessary in order not to poison the original implementation
#[derive(Eq, PartialEq, Clone, Debug, Hash, PartialOrd, Ord, Abomonation)]
pub enum AbomonatedTypedValue {
    Str(String),
    Bool(bool),
    UInt(u32),
    InternedStr(usize),
}

impl Display for AbomonatedTypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AbomonatedTypedValue::Str(inner) => write!(f, "\"{}\"", inner),
            AbomonatedTypedValue::Bool(inner) => write!(f, "{}", inner),
            AbomonatedTypedValue::UInt(inner) => write!(f, "{}", inner),
            AbomonatedTypedValue::InternedStr(inner) => write!(f, "Is{}", inner)
        }
    }
}

impl From<TypedValue> for AbomonatedTypedValue {
    fn from(value: TypedValue) -> Self {
        return match value {
            TypedValue::Str(inner) => AbomonatedTypedValue::Str(inner),
            TypedValue::Bool(inner) => AbomonatedTypedValue::Bool(inner),
            TypedValue::UInt(inner) => AbomonatedTypedValue::UInt(inner),
            TypedValue::InternedStr(inner) => AbomonatedTypedValue::InternedStr(inner),
            _ => panic!("floats are not supported by differential reasoner!")
        };
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

pub type AbomonatedAtom = (String, bool, Vec<AbomonatedTerm>);
pub type MaskedAtom = (String, Vec<Option<AbomonatedTypedValue>>);

pub fn mask(aboatom: &AbomonatedAtom) -> MaskedAtom {
    let sym = aboatom.0.to_string();

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

pub fn permute_mask(masked_atom: &MaskedAtom) -> Vec<MaskedAtom> {
    let sym = masked_atom.0.to_string();
    let arity = masked_atom.1.len();

    let out = masked_atom
        .1
        .iter()
        .enumerate()
        .filter(|(idx, possibly_some)| **possibly_some != None)
        .powerset()
        .map(|x| {
            let mut vec = vec![None; arity];

            x
                .iter()
                .for_each(|(idx, value)| {
                    vec[*idx] = (*value).clone()
                });

            return (sym.to_string(), vec);
        })
        .collect();

    out
}

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

        return (atom.symbol, atom.sign, terms);
    }
}

pub type AbomonatedRule = (AbomonatedAtom, Vec<AbomonatedAtom>);

impl From<Rule> for AbomonatedRule {
    fn from(rule: Rule) -> Self {
        let head = AbomonatedAtom::from(rule.head);
        let body = rule.body.iter().map(|atom| AbomonatedAtom::from(atom.clone())).collect();

        return (head, body);
    }
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::Atom;
    use crate::models::index::BTreeIndex;
    use crate::models::instance::InstanceWithIndex;
    use crate::reasoning::reasoners::differential::abomonated_model::{AbomonatedAtom, AbomonatedTypedValue, mask, permute_mask};
    use crate::reasoning::reasoners::differential::abomonated_model::AbomonatedTerm::Constant;

    #[test]
    fn test_permute_mask() {
        let rule_atom_0 = AbomonatedAtom::from(Atom::from("T(?X, ?Y, PLlab)"));
        let ground_atom_0 = AbomonatedAtom::from(Atom::from("T(student, takesClassesFrom, PLlab"));

        let mut permutations_rule_atom_0 = permute_mask(&mask(&rule_atom_0));
        let mut permutations_ground_atom_0 = permute_mask(&mask(&ground_atom_0));
        permutations_rule_atom_0.sort();
        permutations_ground_atom_0.sort();

        let t = "T".to_string();
        let mut expected_permutations_rule_atom_0 = vec![
            (t.clone(), vec![
                None,
                None,
                None,
            ]),
            (t.clone(), vec![
                None,
                None,
                Some(AbomonatedTypedValue::Str("PLlab".to_string())),
            ]),
        ];
        expected_permutations_rule_atom_0.sort();
        assert_eq!(permutations_rule_atom_0, expected_permutations_rule_atom_0);

        let mut expected_permutations_ground_atom_0 = vec![
            ("T".to_string(), vec![None, None, None]),
            ("T".to_string(), vec![None, None, Some(AbomonatedTypedValue::Str("PLlab".to_string()))]),
            ("T".to_string(), vec![None, Some(AbomonatedTypedValue::Str("takesClassesFrom".to_string())), None]),
            ("T".to_string(), vec![None, Some(AbomonatedTypedValue::Str("takesClassesFrom".to_string())), Some(AbomonatedTypedValue::Str("PLlab".to_string()))]),
            ("T".to_string(), vec![Some(AbomonatedTypedValue::Str("student".to_string())), None, None]),
            ("T".to_string(), vec![Some(AbomonatedTypedValue::Str("student".to_string())), None, Some(AbomonatedTypedValue::Str("PLlab".to_string()))]),
            ("T".to_string(), vec![Some(AbomonatedTypedValue::Str("student".to_string())), Some(AbomonatedTypedValue::Str("takesClassesFrom".to_string())), None]),
            ("T".to_string(), vec![Some(AbomonatedTypedValue::Str("student".to_string())), Some(AbomonatedTypedValue::Str("takesClassesFrom".to_string())), Some(AbomonatedTypedValue::Str("PLlab".to_string()))]),
        ];
        expected_permutations_ground_atom_0.sort();

        assert_eq!(permutations_ground_atom_0, expected_permutations_ground_atom_0);
    }
}