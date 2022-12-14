use logos::{Lexer, Logos};
use std::collections::BTreeMap;
use std::iter::Peekable;

use crate::lexers::datalog::DatalogToken;
use crate::models::datalog::Sign::{Negative, Positive};
use crate::models::datalog::{Atom, Rule, Term, TypedValue};

fn parse_lexed_atom<'a>(
    lexer: &mut Peekable<Lexer<'a, DatalogToken<'a>>>,
    interner: &mut BTreeMap<&'a str, u8>,
) -> Atom {
    let mut terms: Vec<Term> = vec![];
    while let Some(token) = lexer.next() {
        match token {
            DatalogToken::RParen => break,
            DatalogToken::Str(current_token_value) => terms.push(Term::Constant(TypedValue::Str(
                current_token_value.to_string(),
            ))),
            DatalogToken::UIntConst(current_token_value) => {
                terms.push(Term::Constant(TypedValue::UInt(current_token_value)))
            }
            DatalogToken::BoolConst(current_token_value) => {
                terms.push(Term::Constant(TypedValue::Bool(current_token_value)))
            }
            DatalogToken::Variable(current_token_value) => {
                let mut current_idx = interner.len() as u8;
                if let Some(idx) = interner.get(current_token_value) {
                    current_idx = *idx
                } else {
                    interner.insert(current_token_value, current_idx);
                }
                terms.push(Term::Variable(current_idx))
            }
            _ => continue,
        }
    }
    return Atom {
        terms,
        symbol: "".to_string(),
        sign: Positive,
    };
}

pub fn parse_atom(atom: &str) -> Atom {
    let mut lexer = DatalogToken::lexer(atom).peekable();
    let mut atom = Atom {
        terms: vec![],
        symbol: "".to_string(),
        sign: Positive,
    };
    let mut interner: BTreeMap<&str, u8> = BTreeMap::new();

    while let Some(token) = lexer.next() {
        match token {
            DatalogToken::Negation => atom.sign = Negative,
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

pub fn parse_rule(rule: &str) -> Rule {
    let mut lexer = DatalogToken::lexer(rule).peekable();
    let mut head = Atom {
        terms: vec![],
        symbol: "".to_string(),
        sign: Positive,
    };
    let mut body: Vec<Atom> = vec![];
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
                        parsed_atom.sign = Negative;
                    }
                    body.push(parsed_atom)
                }
            }
            _ => {}
        }
        look_behind = token
    }
    return Rule { head, body };
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{Atom, Rule, Sign, Term, TypedValue};

    #[test]
    fn test_parse_atom() {
        let some_atom_1 = "X(?a, 5, true)";
        let some_atom_2 = "!Y(?a, yeah, false)";
        let some_atom_3 = "Z(?a, 4, 5)";

        let parsed_atom_1 = Atom::from(some_atom_1);
        let parsed_atom_2 = Atom::from(some_atom_2);
        let parsed_atom_3 = Atom::from(some_atom_3);

        let expected_parsed_atom_1 = Atom {
            terms: vec![
                Term::Variable(0),
                Term::Constant(TypedValue::UInt(5)),
                Term::Constant(TypedValue::Bool(true)),
            ],
            symbol: "X".to_string(),
            sign: Sign::Positive,
        };
        let expected_parsed_atom_2 = Atom {
            terms: vec![
                Term::Variable(0),
                Term::Constant(TypedValue::Str("yeah".to_string())),
                Term::Constant(TypedValue::Bool(false)),
            ],
            symbol: "Y".to_string(),
            sign: Sign::Negative,
        };
        let expected_parsed_atom_3 = Atom {
            terms: vec![
                Term::Variable(0),
                Term::Constant(TypedValue::UInt(4)),
                Term::Constant(TypedValue::UInt(5)),
            ],
            symbol: "Z".to_string(),
            sign: Sign::Positive,
        };

        assert_eq!(parsed_atom_1, expected_parsed_atom_1);
        assert_eq!(parsed_atom_2, expected_parsed_atom_2);
        assert_eq!(parsed_atom_3, expected_parsed_atom_3);
    }

    #[test]
    fn test_parse_rule() {
        let some_rule = "[X(?a, 5, true), !Y(?a, yeah, false)] -> Z(?a, 4, 5)";
        let some_reversed_rule = "Z(?a, 4, 5) <- [X(?a, 5, true), !Y(?a, yeah, false)]";
        let expected_parsing = Rule {
            head: Atom {
                terms: vec![
                    Term::Variable(0),
                    Term::Constant(TypedValue::UInt(4)),
                    Term::Constant(TypedValue::UInt(5)),
                ],
                symbol: "Z".to_string(),
                sign: Sign::Positive,
            },
            body: vec![
                Atom {
                    terms: vec![
                        Term::Variable(0),
                        Term::Constant(TypedValue::UInt(5)),
                        Term::Constant(TypedValue::Bool(true)),
                    ],
                    symbol: "X".to_string(),
                    sign: Sign::Positive,
                },
                Atom {
                    terms: vec![
                        Term::Variable(0),
                        Term::Constant(TypedValue::Str("yeah".to_string())),
                        Term::Constant(TypedValue::Bool(false)),
                    ],
                    symbol: "Y".to_string(),
                    sign: Sign::Negative,
                },
            ],
        };
        let some_parsed_rule = Rule::from(some_rule);
        let some_parsed_reversed_rule = Rule::from(some_reversed_rule);
        assert_eq!(expected_parsing, some_parsed_rule);
        assert_eq!(expected_parsing, some_parsed_reversed_rule)
    }
}
