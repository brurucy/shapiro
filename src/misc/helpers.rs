use crate::misc::rule_graph::sort_program;
use crate::misc::string_interning::Interner;
use crate::models::datalog::{Program, SugaredProgram, Term, Ty};
use crate::models::relational_algebra::Row;

pub fn terms_to_row(terms: Vec<Term>) -> Row {
    terms
        .into_iter()
        .map(|term| {
            match term {
                Term::Constant(inner) => inner,
                // It is reachable, but for the specific case that this function is being used
                // it isn't
                _ => unreachable!(),
            }
        })
        .collect()
}

pub fn ty_to_row(tys: &Vec<Box<dyn Ty>>) -> Row {
    tys.iter().map(|ty| ty.to_typed_value()).collect()
}

pub fn idempotent_intern(
    interner: &mut Interner,
    intern: bool,
    table: &str,
    row: Row,
) -> (u32, Row) {
    let typed_row = if intern {
        interner.intern_row(row)
    } else {
        row
    };
    let relation_id = interner.rodeo.get_or_intern(table).into_inner().get();

    return (relation_id, typed_row);
}

pub fn idempotent_program_weak_intern(
    interner: &mut Interner,
    intern: bool,
    sugared_program: &SugaredProgram,
) -> SugaredProgram {
    let sugared_program = sort_program(&sugared_program);

    let interned_sugared_program: Vec<_> = sugared_program
        .into_iter()
        .map(|rule| {
            if intern {
                interner.intern_sugared_rule(&rule)
            } else {
                rule.clone()
            }
        })
        .collect();

    return interned_sugared_program;
}

pub fn idempotent_program_strong_intern(
    interner: &mut Interner,
    intern: bool,
    sugared_program: &SugaredProgram,
) -> Program {
    let sugared_program = sort_program(&sugared_program);

    let interned_sugared_program: Vec<_> = sugared_program
        .into_iter()
        .map(|rule| {
            if intern {
                interner.intern_rule(&rule)
            } else {
                interner.intern_rule_weak(&rule)
            }
        })
        .collect();

    return interned_sugared_program;
}
