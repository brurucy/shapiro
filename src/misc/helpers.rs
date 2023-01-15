use crate::models::datalog::{Term, Ty, TypedValue};

fn terms_to_boxed_term_slice(terms: Vec<Term>) -> Box<[TypedValue]> {
    terms
        .into_iter()
        .map(|term| {
            match term {
                Term::Constant(inner) => inner,
                // It is reachable, but for the specific case that this function is being used
                // it isn't
                Term::Variable(inner) => unreachable!()
            }
        })
        .collect()
}

fn dyn_ty_to_boxed_term_slice(tys: Vec<Box<dyn Ty>>) -> Box<[TypedValue]> {
        tys
            .into_iter()
            .map(|ty| ty.to_typed_value())
            .collect()
}