use logos::Logos;
use ordered_float::OrderedFloat;

#[derive(Logos, Debug, PartialEq, Hash, Eq, Clone)]
pub enum DatalogToken<'a> {
    #[regex(r"\?[A-Za-z0-9]+")]
    Variable(&'a str),
    #[regex(r"[0-9]+", |lex| lex.slice().parse())]
    UIntConst(u32),
    #[regex(r"[A-Za-z]+")]
    Str(&'a str),
    #[regex(r"(true|false)", |lex| lex.slice().parse())]
    BoolConst(bool),
    #[regex(r"[-]?[0-9]*(\.[0-9]+)", |lex| lex.slice().parse())]
    FloatConst(OrderedFloat<f64>),
    #[token("!")]
    Negation,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[regex(r"->|<-")]
    HeadDirection,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token(",")]
    Comma,
    #[error]
    #[regex(r"[ \t\n\f]+", logos::skip)]
    Error,
}

mod test {
    use crate::lexers::datalog::DatalogToken;
    use logos::Logos;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_lex_rule() {
        // [X(?a, 5, true), !Y(?a, yeah, false)] -> Z(?a, 4, 5)
        // Z(?a, 4, 5) <- [X(?a, 5, true), !Y(?a, yeah, false)]
        let mut lex =
            DatalogToken::lexer("[X(?a, 5, true), !Y(?a, yeah, false)] -> Z(?a, -4.1, 5)");

        assert_eq!(lex.next(), Some(DatalogToken::LBracket));

        assert_eq!(lex.next(), Some(DatalogToken::Str("X")));
        assert_eq!(lex.slice(), "X");

        assert_eq!(lex.next(), Some(DatalogToken::LParen));

        assert_eq!(lex.next(), Some(DatalogToken::Variable("?a")));
        assert_eq!(lex.slice(), "?a");

        assert_eq!(lex.next(), Some(DatalogToken::Comma));

        assert_eq!(lex.next(), Some(DatalogToken::UIntConst(5)));
        assert_eq!(lex.slice(), "5");

        assert_eq!(lex.next(), Some(DatalogToken::Comma));

        assert_eq!(lex.next(), Some(DatalogToken::BoolConst(true)));
        assert_eq!(lex.slice(), "true");

        assert_eq!(lex.next(), Some(DatalogToken::RParen));

        assert_eq!(lex.next(), Some(DatalogToken::Comma));

        assert_eq!(lex.next(), Some(DatalogToken::Negation));

        assert_eq!(lex.next(), Some(DatalogToken::Str("Y")));
        assert_eq!(lex.slice(), "Y");

        assert_eq!(lex.next(), Some(DatalogToken::LParen));

        assert_eq!(lex.next(), Some(DatalogToken::Variable("?a")));
        assert_eq!(lex.slice(), "?a");

        assert_eq!(lex.next(), Some(DatalogToken::Comma));

        assert_eq!(lex.next(), Some(DatalogToken::Str("yeah")));
        assert_eq!(lex.slice(), "yeah");

        assert_eq!(lex.next(), Some(DatalogToken::Comma));

        assert_eq!(lex.next(), Some(DatalogToken::BoolConst(false)));
        assert_eq!(lex.slice(), "false");

        assert_eq!(lex.next(), Some(DatalogToken::RParen));
        assert_eq!(lex.next(), Some(DatalogToken::RBracket));

        assert_eq!(lex.next(), Some(DatalogToken::HeadDirection));

        assert_eq!(lex.next(), Some(DatalogToken::Str("Z")));
        assert_eq!(lex.slice(), "Z");

        assert_eq!(lex.next(), Some(DatalogToken::LParen));

        assert_eq!(lex.next(), Some(DatalogToken::Variable("?a")));
        assert_eq!(lex.slice(), "?a");

        assert_eq!(lex.next(), Some(DatalogToken::Comma));

        assert_eq!(
            lex.next(),
            Some(DatalogToken::FloatConst(OrderedFloat(-4.1)))
        );
        assert_eq!(lex.slice(), "-4.1");

        assert_eq!(lex.next(), Some(DatalogToken::Comma));

        assert_eq!(lex.next(), Some(DatalogToken::UIntConst(5)));
        assert_eq!(lex.slice(), "5");
    }
}
