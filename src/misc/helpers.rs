use crate::models::relational_algebra::Row;
use crate::models::datalog::{Term, Ty};

pub fn terms_to_row(terms: Vec<Term>) -> Row {
    terms
        .into_iter()
        .map(|term| {
            match term {
                Term::Constant(inner) => inner,
                // It is reachable, but for the specific case that this function is being used
                // it isn't
                _ => unreachable!()
            }
        })
        .collect()
}

pub fn ty_to_row(tys: Vec<Box<dyn Ty>>) -> Row {
        tys
            .into_iter()
            .map(|ty| ty.to_typed_value())
            .collect()
}