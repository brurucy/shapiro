use crate::models::datalog::{Atom, Rule, Term, TypedValue};
use crate::models::relational_algebra::Row;
use lasso::{Key, Rodeo};

pub struct Interner {
    pub rodeo: Rodeo,
}

impl Interner {
    pub(crate) fn intern_atom(&mut self, atom: &Atom) -> Atom {
        let new_terms = atom.terms.iter().map(|term| match term {
            Term::Constant(inner) => match inner {
                TypedValue::Str(inner) => Term::Constant(TypedValue::InternedStr(
                    self.rodeo.get_or_intern(inner.as_str()).into_inner(),
                )),
                not_str => Term::Constant(not_str.clone()),
            },
            variable => variable.clone(),
        });
        return Atom {
            terms: new_terms.collect(),
            symbol: atom.symbol.clone(),
            sign: true,
        };
    }

    pub fn intern_typed_values(&mut self, values: Row) -> Row {
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

    pub(crate) fn intern_rule(&mut self, rule: &Rule) -> Rule {
        let mut new_rule = rule.clone();
        new_rule.head = self.intern_atom(&new_rule.head);
        new_rule.body = new_rule
            .body
            .iter()
            .map(|body_atom| self.intern_atom(body_atom))
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
