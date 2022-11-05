use logos::Logos;
use ordered_float::OrderedFloat;

#[derive(Logos, Debug, PartialEq, Hash, Eq, Clone)]
pub enum RelationalAlgebraToken<'a> {
    #[regex(r"\?[A-Za-z0-9:]+")]
    Variable(&'a str),
    #[regex(r"[0-9]+", |lex| lex.slice().parse())]
    UIntConst(u32),
    #[regex(r"([0-9])+usize", |lex| lex.slice())]
    Column(&'a str),
    #[regex(r"[A-Za-z:]+")]
    Str(&'a str),
    #[regex(r"(true|false)", |lex| lex.slice().parse())]
    BoolConst(bool),
    #[regex(r"[-]?[0-9]*(\.[0-9]+)", |lex| lex.slice().parse())]
    FloatConst(OrderedFloat<f64>),
    #[token("join")]
    Join,
    #[token("select")]
    Select,
    #[token("product")]
    Product,
    #[token("project")]
    Project,
    #[token("_")]
    Underscore,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token(",")]
    Comma,
    #[token("=")]
    Equals,
    #[error]
    #[regex(r"[ \t\n\f]+", logos::skip)]
    Error,
}

mod tests {
    use crate::lexers::datalog::DatalogToken;
    use logos::Logos;
    use ordered_float::OrderedFloat;
    use crate::lexers::relational_algebra::RelationalAlgebraToken;

    #[test]
    fn test_lex_relalg_expr() {
        let mut lex =
            RelationalAlgebraToken::lexer("project_[3usize, rdf:type, 2usize](join_0=1(select_1=rdfs:domain(T(?a, ?Strrdfs:domain, ?x)), T(?y, ?a4, ?z)))");

        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Project));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Underscore));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::LBracket));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Column("3usize")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Comma));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Str("rdf:type")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Comma));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Column("2usize")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::RBracket));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::LParen));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Join));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Underscore));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::UIntConst(0)));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Equals));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::UIntConst(1)));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::LParen));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Select));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Underscore));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::UIntConst(1)));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Equals));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Str("rdfs:domain")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::LParen));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Str("T")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::LParen));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Variable("?a")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Comma));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Variable("?Strrdfs:domain")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Comma));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Variable("?x")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::RParen));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::RParen));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Comma));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Str("T")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::LParen));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Variable("?y")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Comma));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Variable("?a4")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Comma));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::Variable("?z")));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::RParen));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::RParen));
        assert_eq!(lex.next(), Some(RelationalAlgebraToken::RParen));
    }
}
