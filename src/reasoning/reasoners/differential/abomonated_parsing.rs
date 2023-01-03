use std::collections::BTreeMap;
use logos::{Lexer, Logos};
use std::iter::Peekable;
use crate::lexers::datalog::DatalogToken;
use crate::reasoning::reasoners::differential::abomonated_model::{AbomonatedAtom, AbomonatedRule, AbomonatedSign, AbomonatedTerm, AbomonatedTypedValue};

fn parse_lexed_atom<'a>(lexer: &mut Peekable<Lexer<'a, DatalogToken<'a>>>, interner: &mut BTreeMap<&'a str, u8>) -> AbomonatedAtom {
    let mut terms: Vec<AbomonatedTerm> = vec![];
    while let Some(token) = lexer.next() {
        match token {
            DatalogToken::RParen => break,
            DatalogToken::Str(current_token_value) => terms.push(AbomonatedTerm::Constant(AbomonatedTypedValue::Str(
                current_token_value.to_string(),
            ))),
            DatalogToken::UIntConst(current_token_value) => {
                terms.push(AbomonatedTerm::Constant(AbomonatedTypedValue::UInt(current_token_value)))
            }
            DatalogToken::BoolConst(current_token_value) => {
                terms.push(AbomonatedTerm::Constant(AbomonatedTypedValue::Bool(current_token_value)))
            }
            DatalogToken::Variable(current_token_value) => {
                let mut current_idx = interner.len() as u8;
                if let Some(idx) = interner.get(current_token_value) {
                    current_idx = *idx
                } else {
                    interner.insert(current_token_value, current_idx);
                }
                terms.push(AbomonatedTerm::Variable(current_idx))
            }
            _ => continue,
        }
    }
    return AbomonatedAtom {
        terms,
        symbol: "".to_string(),
        sign: AbomonatedSign::Positive,
    };
}

pub fn parse_atom(atom: &str) -> AbomonatedAtom {
    let mut lexer = DatalogToken::lexer(atom).peekable();
    let mut atom = AbomonatedAtom {
        terms: vec![],
        symbol: "".to_string(),
        sign: AbomonatedSign::Positive,
    };
    let mut interner: BTreeMap<&str, u8> = BTreeMap::new();

    while let Some(token) = lexer.next() {
        match token {
            DatalogToken::Negation => atom.sign = AbomonatedSign::Negative,
            DatalogToken::Str(predicate_symbol) => {
                let parsed_atom = parse_lexed_atom(&mut lexer, &mut interner);
                atom.symbol = predicate_symbol.to_string();
                atom.terms = parsed_atom.terms;
            }
            _ => {}
        }
    }

    return atom;
}

pub fn parse_rule(rule: &str) -> AbomonatedRule {
    let mut lexer = DatalogToken::lexer(rule).peekable();
    let mut head = AbomonatedAtom {
        terms: vec![],
        symbol: "".to_string(),
        sign: AbomonatedSign::Positive,
    };
    let mut body: Vec<AbomonatedAtom> = vec![];
    let mut look_behind: DatalogToken = DatalogToken::Error;
    let mut look_ahead: DatalogToken = DatalogToken::Error;

    let mut interner = Default::default();
    while let Some(token) = lexer.next() {
        if let Some(peek) = lexer.peek() {
            look_ahead = peek.clone()
        }
        match token {
            DatalogToken::Str(symbol) => {
                if look_ahead == DatalogToken::LParen {
                    let mut parsed_atom = parse_lexed_atom(&mut lexer, &mut interner);
                    parsed_atom.symbol = symbol.parse().unwrap();
                    if look_behind == DatalogToken::HeadDirection
                        || look_behind == DatalogToken::Error
                    {
                        head = parsed_atom;
                        continue;
                    }
                    if look_behind == DatalogToken::Negation {
                        parsed_atom.sign = AbomonatedSign::Negative;
                    }
                    body.push(parsed_atom)
                }
            }
            _ => {}
        }
        look_behind = token
    }
    return AbomonatedRule { head, body };
}
