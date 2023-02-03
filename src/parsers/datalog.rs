use logos::{Lexer, Logos};
use std::collections::BTreeMap;
use std::iter::Peekable;

use crate::lexers::datalog::DatalogToken;
use crate::models::datalog::{SugaredRule, SugaredAtom, Term, TypedValue};

fn parse_lexed_sugared_atom<'a>(
    lexer: &mut Peekable<Lexer<'a, DatalogToken<'a>>>,
    interner: &mut BTreeMap<&'a str, u8>,
) -> SugaredAtom {
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
    return SugaredAtom {
        terms,
        symbol: "".to_string(),
        positive: true,
    };
}

pub fn parse_sugared_atom(sugared_atom: &str) -> SugaredAtom {
    let mut lexer = DatalogToken::lexer(sugared_atom).peekable();
    let mut sugared_atom = SugaredAtom {
        terms: vec![],
        symbol: "".to_string(),
        positive: true,
    };
    let mut interner: BTreeMap<&str, u8> = BTreeMap::new();

    while let Some(token) = lexer.next() {
        match token {
            DatalogToken::Negation => sugared_atom.positive = false,
            DatalogToken::Str(predicate_symbol) => {
                let parsed_sugared_atom = parse_lexed_sugared_atom(&mut lexer, &mut interner);
                sugared_atom.symbol = predicate_symbol.to_string();
                sugared_atom.terms = parsed_sugared_atom.terms;
            }
            _ => {}
        }
    }

    return sugared_atom;
}

pub fn parse_sugared_rule(rule: &str) -> SugaredRule {
    let mut lexer = DatalogToken::lexer(rule).peekable();
    let mut head = SugaredAtom {
        terms: vec![],
        symbol: "".to_string(),
        positive: false,
    };
    let mut body: Vec<SugaredAtom> = vec![];
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
                    let mut parsed_sugared_atom = parse_lexed_sugared_atom(&mut lexer, &mut interner);
                    parsed_sugared_atom.symbol = symbol.parse().unwrap();
                    if look_behind == DatalogToken::HeadDirection
                        || look_behind == DatalogToken::Error
                    {
                        head = parsed_sugared_atom;
                        continue;
                    }
                    if look_behind == DatalogToken::Negation {
                        parsed_sugared_atom.positive = false;
                    }
                    body.push(parsed_sugared_atom)
                }
            }
            _ => {}
        }
        look_behind = token
    }
    return SugaredRule { head, body };
}

#[cfg(test)]
mod tests {
    use crate::models::datalog::{SugaredAtom, SugaredRule, Term, TypedValue};

    #[test]
    fn test_parse_sugared_atom() {
        let some_sugared_atom_1 = "X(?a, 5, true)";
        let some_sugared_atom_2 = "!Y(?a, yeah, false)";
        let some_sugared_atom_3 = "Z(?a, 4, 5)";

        let parsed_sugared_atom_1 = SugaredAtom::from(some_sugared_atom_1);
        let parsed_sugared_atom_2 = SugaredAtom::from(some_sugared_atom_2);
        let parsed_sugared_atom_3 = SugaredAtom::from(some_sugared_atom_3);

        let expected_parsed_sugared_atom_1 = SugaredAtom {
            terms: vec![
                Term::Variable(0),
                Term::Constant(TypedValue::UInt(5)),
                Term::Constant(TypedValue::Bool(true)),
            ],
            symbol: "X".to_string(),
            positive: true,
        };
        let expected_parsed_sugared_atom_2 = SugaredAtom {
            terms: vec![
                Term::Variable(0),
                Term::Constant(TypedValue::Str("yeah".to_string())),
                Term::Constant(TypedValue::Bool(false)),
            ],
            symbol: "Y".to_string(),
            positive: false,
        };
        let expected_parsed_sugared_atom_3 = SugaredAtom {
            terms: vec![
                Term::Variable(0),
                Term::Constant(TypedValue::UInt(4)),
                Term::Constant(TypedValue::UInt(5)),
            ],
            symbol: "Z".to_string(),
            positive: true,
        };

        assert_eq!(parsed_sugared_atom_1, expected_parsed_sugared_atom_1);
        assert_eq!(parsed_sugared_atom_2, expected_parsed_sugared_atom_2);
        assert_eq!(parsed_sugared_atom_3, expected_parsed_sugared_atom_3);
    }

    #[test]
    fn test_parse_rule() {
        let some_rule = "[X(?a, 5, true), !Y(?a, yeah, false)] -> Z(?a, 4, 5)";
        let some_reversed_rule = "Z(?a, 4, 5) <- [X(?a, 5, true), !Y(?a, yeah, false)]";
        let expected_parsing = SugaredRule {
            head: SugaredAtom {
                terms: vec![
                    Term::Variable(0),
                    Term::Constant(TypedValue::UInt(4)),
                    Term::Constant(TypedValue::UInt(5)),
                ],
                symbol: "Z".to_string(),
                positive: true,
            },
            body: vec![
                SugaredAtom {
                    terms: vec![
                        Term::Variable(0),
                        Term::Constant(TypedValue::UInt(5)),
                        Term::Constant(TypedValue::Bool(true)),
                    ],
                    symbol: "X".to_string(),
                    positive: true,
                },
                SugaredAtom {
                    terms: vec![
                        Term::Variable(0),
                        Term::Constant(TypedValue::Str("yeah".to_string())),
                        Term::Constant(TypedValue::Bool(false)),
                    ],
                    symbol: "Y".to_string(),
                    positive: false,
                },
            ],
        };
        let some_parsed_rule = SugaredRule::from(some_rule);
        let some_parsed_reversed_rule = SugaredRule::from(some_reversed_rule);
        assert_eq!(expected_parsing, some_parsed_rule);
        assert_eq!(expected_parsing, some_parsed_reversed_rule)
    }
}
