use lasso::{Key, Rodeo, Spur};
use crate::models::datalog::{Atom, Rule, Sign, Term, TypedValue};

pub struct Interner {
    rodeo: Rodeo
}

impl Interner {
    pub(crate) fn intern_atom(&mut self, atom: &Atom) -> Atom {
        let mut new_terms = atom
            .terms
            .iter()
            .map(|term| {
                match term {
                    Term::Constant(inner) => {
                        match inner {
                            TypedValue::Str(inner) => {
                                Term::Constant(TypedValue::InternedStr(self.rodeo.get_or_intern(inner.as_str()).into_usize()))
                            },
                            not_str => Term::Constant(not_str.clone())
                        }
                    }
                    variable => variable.clone()
                }
            });
        return Atom {
            terms: new_terms.collect(),
            symbol: atom.symbol.clone(),
            sign: Sign::Positive
        }
    }

    fn unintern_atom(&self, atom: &Atom) -> Atom {
        let mut new_terms = atom
            .terms
            .iter()
            .map(|term| {
                match term {
                    Term::Constant(inner) => {
                        match inner {
                            TypedValue::InternedStr(inner) => {
                                Term::Constant(TypedValue::Str(self.rodeo.resolve(&Spur::try_from_usize(*inner).unwrap()).to_string()))
                            },
                            not_str => Term::Constant(not_str.clone())
                        }
                    }
                    variable => variable.clone()
                }
            });
        return Atom {
            terms: new_terms.collect(),
            symbol: atom.symbol.clone(),
            sign: Sign::Positive
        }
    }

    fn unintern_rule(&self, rule: &Rule) -> Rule {
        let mut new_rule = rule.clone();
        new_rule.head = self.unintern_atom(&new_rule.head);
        new_rule.body = new_rule.body
            .iter()
            .map(|body_atom| self.unintern_atom(body_atom))
            .collect();

        return new_rule
    }

    pub(crate) fn intern_rule(&mut self, rule: &Rule) -> Rule {
        let mut new_rule = rule.clone();
        new_rule.head = self.intern_atom(&new_rule.head);
        new_rule.body = new_rule.body
            .iter()
            .map(|body_atom| self.intern_atom(body_atom))
            .collect();

        return new_rule
    }

    fn new() -> Self {
        return Self::default()
    }
}

impl Default for Interner {
    fn default() -> Self {
        return Self {
            rodeo: Default::default()
        }
    }
}