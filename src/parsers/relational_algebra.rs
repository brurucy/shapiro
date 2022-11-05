use std::iter::Peekable;
use logos::{Lexer, Logos};
use crate::lexers::datalog::DatalogToken::RParen;

use crate::lexers::relational_algebra::RelationalAlgebraToken;
use crate::models::relational_algebra::{Relation, RelationalExpression};

fn parse_expr<'a>(lexer: &mut Peekable<Lexer<'a, RelationalAlgebraToken<'a>>>) -> Option<RelationalExpression> {
    if let Some(token) = lexer.next() {
        match token {
            RelationalAlgebraToken::Str() => {
                
            }
        }
    }
    return None
}