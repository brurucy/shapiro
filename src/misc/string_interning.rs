use crate::models::datalog::{Atom, Rule, SugaredAtom, SugaredRule, Term, TypedValue};
use crate::models::relational_algebra::Row;
use lasso::Rodeo;

pub struct Interner {
    pub rodeo: Rodeo,
}

impl Interner {
    // Interns both the symbol and the terms.
    pub(crate) fn intern_atom(&mut self, sugared_atom: &SugaredAtom) -> Atom {
        let new_terms = sugared_atom.terms.iter().map(|term| match term {
            Term::Constant(inner) => match inner {
                TypedValue::Str(inner) => Term::Constant(TypedValue::InternedStr(
                    self.rodeo.get_or_intern(inner.as_str()).into_inner(),
                )),
                not_str => Term::Constant(not_str.clone()),
            },
            variable => variable.clone(),
        })
            .collect();

        let relation_id = self.rodeo.get_or_intern(&sugared_atom.symbol).into_inner();

        return Atom {
            terms: new_terms,
            relation_id,
            sign: true,
        };
    }

    // Weak interning refers to only interning the relation symbol, not the terms themselves.
    pub(crate) fn intern_atom_weak(&mut self, sugared_atom: &SugaredAtom) -> Atom {
        return Atom {
            terms: sugared_atom.terms.clone(),
            relation_id: self.rodeo.get_or_intern(&sugared_atom.symbol).into_inner(),
            sign: true,
        }
    }

    pub(crate) fn intern_sugared_atom(&mut self, sugared_atom: &SugaredAtom) -> SugaredAtom {
        let new_terms = sugared_atom.terms.iter().map(|term| match term {
            Term::Constant(inner) => match inner {
                TypedValue::Str(inner) => Term::Constant(TypedValue::InternedStr(
                    self.rodeo.get_or_intern(inner.as_str()).into_inner(),
                )),
                not_str => Term::Constant(not_str.clone()),
            },
            variable => variable.clone(),
        });

        return SugaredAtom {
            terms: new_terms.collect(),
            symbol: sugared_atom.symbol.clone(),
            positive: true,
        };
    }

    pub fn intern_typed_values(&mut self, values: &Row) -> Row {
        return values
            .iter()
            .map(|typed_value| match typed_value {
                TypedValue::Str(inner) => {
                    TypedValue::InternedStr(self.rodeo.get_or_intern(inner).into_inner())
                }
                not_str => not_str.clone(),
            })
            .collect();
    }

    pub(crate) fn intern_sugared_rule(&mut self, rule: &SugaredRule) -> SugaredRule {
        let mut new_rule: SugaredRule = Default::default();
        new_rule.head = self.intern_sugared_atom(&rule.head);
        new_rule.body = rule
            .body
            .iter()
            .map(|body_atom| self.intern_sugared_atom(&body_atom))
            .collect();

        return new_rule;
    }

    pub(crate) fn intern_rule(&mut self, rule: &SugaredRule) -> Rule {
        let mut new_rule: Rule = Default::default();
        new_rule.head = self.intern_atom(&rule.head);
        new_rule.body = rule
            .body
            .iter()
            .map(|body_atom| self.intern_atom(&body_atom))
            .collect();

        return new_rule;
    }

    pub(crate) fn intern_rule_weak(&mut self, rule: &SugaredRule) -> Rule {
        let mut new_rule: Rule = Default::default();
        new_rule.head = self.intern_atom_weak(&rule.head);
        new_rule.body = rule
            .body
            .iter()
            .map(|body_atom| self.intern_atom_weak(&body_atom))
            .collect();

        return new_rule;
    }
}

impl Default for Interner {
    fn default() -> Self {
        return Self {
            rodeo: Default::default(),
        };
    }
}
