// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use logos::Lexer;
use logos::Logos;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

pub use self::TokenKind::*;
use crate::ParseError;
use crate::Range;
use crate::Result;

#[derive(Clone, PartialEq, Eq)]
pub struct Token<'a> {
    pub source: &'a str,
    pub kind: TokenKind,
    pub span: Range,
}

impl<'a> Token<'a> {
    pub fn new_eoi(source: &'a str) -> Self {
        Token {
            source,
            kind: TokenKind::EOI,
            span: (source.len()..source.len()).into(),
        }
    }

    pub fn text(&self) -> &'a str {
        &self.source[std::ops::Range::from(self.span)]
    }
}

impl<'a> std::fmt::Debug for Token<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}({:?})", self.kind, self.span)
    }
}

pub struct Tokenizer<'a> {
    source: &'a str,
    lexer: Lexer<'a, TokenKind>,
    prev_token: Option<TokenKind>,
    eoi: bool,
}

impl<'a> Tokenizer<'a> {
    pub fn new(source: &'a str) -> Self {
        Tokenizer {
            source,
            lexer: TokenKind::lexer(source),
            eoi: false,
            prev_token: None,
        }
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Result<Token<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.lexer.next() {
            Some(TokenKind::Error) => {
                let span = Some((self.lexer.span().start..self.source.len()).into());
                Some(Err(ParseError(
                    span,
                    "unable to recognize the rest tokens".to_string(),
                )))
            }
            Some(kind) => {
                // Skip hint-like comment that is in the invalid position.
                if !matches!(
                    self.prev_token,
                    Some(
                        TokenKind::INSERT
                            | TokenKind::SELECT
                            | TokenKind::REPLACE
                            | TokenKind::UPDATE
                            | TokenKind::DELETE
                            | TokenKind::COPY
                    )
                ) && kind == TokenKind::HintPrefix
                {
                    loop {
                        match self.next() {
                            // Hint-like comment ended. Return the next token.
                            Some(Ok(token)) if token.kind == TokenKind::HintSuffix => {
                                return self.next();
                            }
                            // Do not skip EOI.
                            Some(Ok(token)) if token.kind == TokenKind::EOI => {
                                return Some(Ok(token));
                            }
                            // In the comment, skip the contents.
                            Some(Ok(_)) => continue,
                            Some(Err(err)) => return Some(Err(err)),
                            None => return None,
                        }
                    }
                }
                self.prev_token = Some(kind);
                Some(Ok(Token {
                    source: self.source,
                    kind,
                    span: self.lexer.span().into(),
                }))
            }
            None if !self.eoi => {
                self.eoi = true;
                Some(Ok(Token::new_eoi(self.source)))
            }
            None => None,
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Logos, EnumIter, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TokenKind {
    #[error]
    Error,

    EOI,

    #[regex(r"[ \t\r\n\f]+", logos::skip)]
    Whitespace,

    #[regex(r"--[^\n\f]*", logos::skip)]
    Comment,

    #[regex(r"/\*[^\+]([^\*]|(\*[^/]))*\*/", logos::skip)]
    CommentBlock,

    #[regex(r#"[_a-zA-Z][_$a-zA-Z0-9]*"#)]
    Ident,

    #[regex(r#"\$[0-9]+"#)]
    ColumnPosition,

    #[regex(r#"\$[_a-zA-Z][_$a-zA-Z0-9]*"#)]
    VariableAccess,

    #[regex(r#"`[^`]*`"#)]
    #[regex(r#""([^"\\]|\\.|"")*""#)]
    #[regex(r#"'([^'\\]|\\.|'')*'"#)]
    LiteralString,

    #[regex(r#"\$\$([^\$]|(\$[^\$]))*\$\$"#)]
    LiteralCodeString,

    #[regex(r#"@([^\s`;'"()]|\\\s|\\'|\\"|\\\\)+"#)]
    LiteralAtString,

    #[regex(r"[xX]'[a-fA-F0-9]*'")]
    PGLiteralHex,
    #[regex(r"0[xX][a-fA-F0-9]+")]
    MySQLLiteralHex,

    #[regex(r"[0-9]+(_|[0-9])*")]
    LiteralInteger,

    #[regex(r"[0-9]+[eE][+-]?[0-9]+")]
    #[regex(r"([0-9]*\.[0-9]+([eE][+-]?[0-9]+)?)|([0-9]+\.[0-9]*([eE][+-]?[0-9]+)?)")]
    LiteralFloat,

    // Symbols
    #[token("/*+")]
    HintPrefix,
    #[token("*/")]
    HintSuffix,
    #[token("==")]
    DoubleEq,
    #[token("=")]
    Eq,
    #[token("<>")]
    #[token("!=")]
    NotEq,
    #[token("<")]
    Lt,
    #[token(">")]
    Gt,
    #[token("<=")]
    Lte,
    #[token(">=")]
    Gte,
    #[token("<=>")]
    Spaceship,
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Multiply,
    #[token("/")]
    Divide,
    #[token("//")]
    IntDiv,
    #[token("%")]
    Modulo,
    #[token("||")]
    StringConcat,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token(",")]
    Comma,
    #[token(".")]
    Dot,
    #[token(":")]
    Colon,
    #[token("::")]
    DoubleColon,
    #[token(":=")]
    ColonEqual,
    #[token(";")]
    SemiColon,
    #[token("\\")]
    Backslash,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token("^")]
    Caret,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token("->")]
    RArrow,
    #[token("->>")]
    LongRArrow,
    #[token("=>")]
    FatRArrow,
    #[token("#>")]
    HashRArrow,
    #[token("#>>")]
    HashLongRArrow,
    /// A case insensitive match regular expression operator in PostgreSQL
    #[token("~*")]
    TildeAsterisk,
    /// A case sensitive not match regular expression operator in PostgreSQL
    #[token("!*")]
    ExclamationMarkTilde,
    /// A case insensitive not match regular expression operator in PostgreSQL
    #[token("!~*")]
    ExclamationMarkTildeAsterisk,
    /// A bitwise and operator in PostgreSQL
    #[token("&")]
    BitWiseAnd,
    /// A bitwise or operator in PostgreSQL
    #[token("|")]
    BitWiseOr,
    /// A bitwise xor operator in PostgreSQL
    #[token("#")]
    BitWiseXor,
    /// A bitwise not operator in PostgreSQL
    #[token("~")]
    BitWiseNot,
    /// A bitwise shift left operator in PostgreSQL
    #[token("<<")]
    ShiftLeft,
    /// A bitwise shift right operator in PostgreSQL
    #[token(">>")]
    ShiftRight,
    /// Exclamation Mark `!` used for PostgreSQL factorial operator
    #[token("!")]
    Factorial,
    /// Double Exclamation Mark `!!` used for PostgreSQL prefix factorial operator
    #[token("!!")]
    DoubleExclamationMark,
    /// AtSign `@` used for PostgreSQL abs operator
    #[token("@")]
    Abs,
    /// A square root math operator in PostgreSQL
    #[token("|/")]
    SquareRoot,
    /// A cube root math operator in PostgreSQL
    #[token("||/")]
    CubeRoot,
    /// Placeholder used in prepared stmt
    /// Also used as JSON operator.
    #[token("?")]
    Placeholder,
    /// Used as JSON operator.
    #[token("?|")]
    QuestionOr,
    /// Used as JSON operator.
    #[token("?&")]
    QuestionAnd,
    /// Used as JSON operator.
    #[token("<@")]
    ArrowAt,
    /// Used as JSON operator.
    #[token("@>")]
    AtArrow,
    /// Used as JSON operator.
    #[token("@?")]
    AtQuestion,
    /// Used as JSON operator.
    #[token("@@")]
    AtAt,
    /// Used as JSON operator.
    #[token("#-")]
    HashMinus,

    // Keywords
    //
    // Steps to add keyword:
    // 1. Add the keyword to token kind variants by alphabetical order.
    // 2. Search in this file to see if the new keyword is a commented out reserved keyword. If
    //    so, uncomment the keyword in the reserved list.
    #[token("ACCOUNT", ignore(ascii_case))]
    ACCOUNT,
    #[token("ALL", ignore(ascii_case))]
    ALL,
    #[token("ALLOWED_IP_LIST", ignore(ascii_case))]
    ALLOWED_IP_LIST,
    #[token("ADD", ignore(ascii_case))]
    ADD,
    #[token("AFTER", ignore(ascii_case))]
    AFTER,
    #[token("AGGREGATING", ignore(ascii_case))]
    AGGREGATING,
    #[token("ANY", ignore(ascii_case))]
    ANY,
    #[token("APPEND_ONLY", ignore(ascii_case))]
    APPEND_ONLY,
    #[token("ARGS", ignore(ascii_case))]
    ARGS,
    #[token("AUTO", ignore(ascii_case))]
    AUTO,
    #[token("SOME", ignore(ascii_case))]
    SOME,
    #[token("ALTER", ignore(ascii_case))]
    ALTER,
    #[token("ALWAYS", ignore(ascii_case))]
    ALWAYS,
    #[token("ANALYZE", ignore(ascii_case))]
    ANALYZE,
    #[token("AND", ignore(ascii_case))]
    AND,
    #[token("ARRAY", ignore(ascii_case))]
    ARRAY,
    #[token("AS", ignore(ascii_case))]
    AS,
    #[token("AST", ignore(ascii_case))]
    AST,
    #[token("AT", ignore(ascii_case))]
    AT,
    #[token("ASC", ignore(ascii_case))]
    ASC,
    #[token("ANTI", ignore(ascii_case))]
    ANTI,
    #[token("ASYNC", ignore(ascii_case))]
    ASYNC,
    #[token("ATTACH", ignore(ascii_case))]
    ATTACH,
    #[token("BEFORE", ignore(ascii_case))]
    BEFORE,
    #[token("BETWEEN", ignore(ascii_case))]
    BETWEEN,
    #[token("BIGINT", ignore(ascii_case))]
    BIGINT,
    #[token("BINARY", ignore(ascii_case))]
    BINARY,
    #[token("BREAK", ignore(ascii_case))]
    BREAK,
    #[token("LONGBLOB", ignore(ascii_case))]
    LONGBLOB,
    #[token("MEDIUMBLOB", ignore(ascii_case))]
    MEDIUMBLOB,
    #[token("TINYBLOB", ignore(ascii_case))]
    TINYBLOB,
    #[token("BLOB", ignore(ascii_case))]
    BLOB,
    #[token("BINARY_FORMAT", ignore(ascii_case))]
    BINARY_FORMAT,
    #[token("BITMAP", ignore(ascii_case))]
    BITMAP,
    #[token("BLOCKED_IP_LIST", ignore(ascii_case))]
    BLOCKED_IP_LIST,
    #[token("BOOL", ignore(ascii_case))]
    BOOL,
    #[token("BOOLEAN", ignore(ascii_case))]
    BOOLEAN,
    #[token("BOTH", ignore(ascii_case))]
    BOTH,
    #[token("BY", ignore(ascii_case))]
    BY,
    #[token("BROTLI", ignore(ascii_case))]
    BROTLI,
    #[token("BZ2", ignore(ascii_case))]
    BZ2,
    #[token("BLOCK", ignore(ascii_case))]
    BLOCK,
    #[token("CALL", ignore(ascii_case))]
    CALL,
    #[token("CASE", ignore(ascii_case))]
    CASE,
    #[token("CAST", ignore(ascii_case))]
    CAST,
    #[token("CATALOG", ignore(ascii_case))]
    CATALOG,
    #[token("CATALOGS", ignore(ascii_case))]
    CATALOGS,
    #[token("CENTURY", ignore(ascii_case))]
    CENTURY,
    #[token("CHANGES", ignore(ascii_case))]
    CHANGES,
    #[token("CLUSTER", ignore(ascii_case))]
    CLUSTER,
    #[token("COMMENT", ignore(ascii_case))]
    COMMENT,
    #[token("COMMENTS", ignore(ascii_case))]
    COMMENTS,
    #[token("COMPACT", ignore(ascii_case))]
    COMPACT,
    #[token("CONNECTION", ignore(ascii_case))]
    CONNECTION,
    #[token("CONNECTIONS", ignore(ascii_case))]
    CONNECTIONS,
    #[token("CONSUME", ignore(ascii_case))]
    CONSUME,
    #[token("CONTENT_TYPE", ignore(ascii_case))]
    CONTENT_TYPE,
    #[token("CONTINUE", ignore(ascii_case))]
    CONTINUE,
    #[token("CHAR", ignore(ascii_case))]
    CHAR,
    #[token("COLUMN", ignore(ascii_case))]
    COLUMN,
    #[token("COLUMNS", ignore(ascii_case))]
    COLUMNS,
    #[token("CHARACTER", ignore(ascii_case))]
    CHARACTER,
    #[token("CONFLICT", ignore(ascii_case))]
    CONFLICT,
    #[token("COMPRESSION", ignore(ascii_case))]
    COMPRESSION,
    #[token("COPY_OPTIONS", ignore(ascii_case))]
    COPY_OPTIONS,
    #[token("COPY", ignore(ascii_case))]
    COPY,
    #[token("COUNT", ignore(ascii_case))]
    COUNT,
    #[token("CREDENTIAL", ignore(ascii_case))]
    CREDENTIAL,
    #[token("CREATE", ignore(ascii_case))]
    CREATE,
    #[token("CROSS", ignore(ascii_case))]
    CROSS,
    #[token("CSV", ignore(ascii_case))]
    CSV,
    #[token("CURRENT", ignore(ascii_case))]
    CURRENT,
    #[token("CURRENT_TIMESTAMP", ignore(ascii_case))]
    CURRENT_TIMESTAMP,
    #[token("DATABASE", ignore(ascii_case))]
    DATABASE,
    #[token("DATABASES", ignore(ascii_case))]
    DATABASES,
    #[token("DATA", ignore(ascii_case))]
    DATA,
    #[token("DATE", ignore(ascii_case))]
    DATE,
    #[token("DATE_ADD", ignore(ascii_case))]
    DATE_ADD,
    #[token("DATE_PART", ignore(ascii_case))]
    DATE_PART,
    #[token("DATE_SUB", ignore(ascii_case))]
    DATE_SUB,
    #[token("DATE_TRUNC", ignore(ascii_case))]
    DATE_TRUNC,
    #[token("DATETIME", ignore(ascii_case))]
    DATETIME,
    #[token("DAY", ignore(ascii_case))]
    DAY,
    #[token("DECADE", ignore(ascii_case))]
    DECADE,
    #[token("DECIMAL", ignore(ascii_case))]
    DECIMAL,
    #[token("DECLARE", ignore(ascii_case))]
    DECLARE,
    #[token("DEFAULT", ignore(ascii_case))]
    DEFAULT,
    #[token("DEFLATE", ignore(ascii_case))]
    DEFLATE,
    #[token("DELETE", ignore(ascii_case))]
    DELETE,
    #[token("DESC", ignore(ascii_case))]
    DESC,
    #[token("DETAILED_OUTPUT", ignore(ascii_case))]
    DETAILED_OUTPUT,
    #[token("DESCRIBE", ignore(ascii_case))]
    DESCRIBE,
    #[token("DISABLE", ignore(ascii_case))]
    DISABLE,
    #[token("DISABLE_VARIANT_CHECK", ignore(ascii_case))]
    DISABLE_VARIANT_CHECK,
    #[token("DISTINCT", ignore(ascii_case))]
    DISTINCT,
    #[token("RESPECT", ignore(ascii_case))]
    RESPECT,
    #[token("IGNORE", ignore(ascii_case))]
    IGNORE,
    #[token("DIV", ignore(ascii_case))]
    DIV,
    #[token("DOUBLE_SHA1_PASSWORD", ignore(ascii_case))]
    DOUBLE_SHA1_PASSWORD,
    #[token("DO", ignore(ascii_case))]
    DO,
    #[token("DOUBLE", ignore(ascii_case))]
    DOUBLE,
    #[token("DOW", ignore(ascii_case))]
    DOW,
    #[token("WEEK", ignore(ascii_case))]
    WEEK,
    #[token("DELTA", ignore(ascii_case))]
    DELTA,
    #[token("DOY", ignore(ascii_case))]
    DOY,
    #[token("DOWNLOAD", ignore(ascii_case))]
    DOWNLOAD,
    #[token("DOWNSTREAM", ignore(ascii_case))]
    DOWNSTREAM,
    #[token("DROP", ignore(ascii_case))]
    DROP,
    #[token("DRY", ignore(ascii_case))]
    DRY,
    #[token("DYNAMIC", ignore(ascii_case))]
    DYNAMIC,
    #[token("EXCEPT", ignore(ascii_case))]
    EXCEPT,
    #[token("EXCLUDE", ignore(ascii_case))]
    EXCLUDE,
    #[token("ELSE", ignore(ascii_case))]
    ELSE,
    #[token("EMPTY_FIELD_AS", ignore(ascii_case))]
    EMPTY_FIELD_AS,
    #[token("ENABLE", ignore(ascii_case))]
    ENABLE,
    #[token("ENABLE_VIRTUAL_HOST_STYLE", ignore(ascii_case))]
    ENABLE_VIRTUAL_HOST_STYLE,
    #[token("END", ignore(ascii_case))]
    END,
    #[token("ENDPOINT", ignore(ascii_case))]
    ENDPOINT,
    #[token("ENGINE", ignore(ascii_case))]
    ENGINE,
    #[token("ENGINES", ignore(ascii_case))]
    ENGINES,
    #[token("EPOCH", ignore(ascii_case))]
    EPOCH,
    #[token("ERROR_ON_COLUMN_COUNT_MISMATCH", ignore(ascii_case))]
    ERROR_ON_COLUMN_COUNT_MISMATCH,
    #[token("ESCAPE", ignore(ascii_case))]
    ESCAPE,
    #[token("EXCEPTION_BACKTRACE", ignore(ascii_case))]
    EXCEPTION_BACKTRACE,
    #[token("EXISTS", ignore(ascii_case))]
    EXISTS,
    #[token("EXPLAIN", ignore(ascii_case))]
    EXPLAIN,
    #[token("EXPIRE", ignore(ascii_case))]
    EXPIRE,
    #[token("EXTRACT", ignore(ascii_case))]
    EXTRACT,
    #[token("ELSEIF", ignore(ascii_case))]
    ELSEIF,
    #[token("FALSE", ignore(ascii_case))]
    FALSE,
    #[token("FIELDS", ignore(ascii_case))]
    FIELDS,
    #[token("FIELD_DELIMITER", ignore(ascii_case))]
    FIELD_DELIMITER,
    #[token("NAN_DISPLAY", ignore(ascii_case))]
    NAN_DISPLAY,
    #[token("NULL_DISPLAY", ignore(ascii_case))]
    NULL_DISPLAY,
    #[token("NULL_IF", ignore(ascii_case))]
    NULL_IF,
    #[token("FILE_FORMAT", ignore(ascii_case))]
    FILE_FORMAT,
    #[token("FILE", ignore(ascii_case))]
    FILE,
    #[token("FILES", ignore(ascii_case))]
    FILES,
    #[token("FINAL", ignore(ascii_case))]
    FINAL,
    #[token("FLASHBACK", ignore(ascii_case))]
    FLASHBACK,
    #[token("FLOAT", ignore(ascii_case))]
    FLOAT,
    #[token("FLOAT32", ignore(ascii_case))]
    FLOAT32,
    #[token("FLOAT64", ignore(ascii_case))]
    FLOAT64,
    #[token("FOR", ignore(ascii_case))]
    FOR,
    #[token("FORCE", ignore(ascii_case))]
    FORCE,
    #[token("FORMAT", ignore(ascii_case))]
    FORMAT,
    #[token("FOLLOWING", ignore(ascii_case))]
    FOLLOWING,
    #[token("FORMAT_NAME", ignore(ascii_case))]
    FORMAT_NAME,
    #[token("FORMATS", ignore(ascii_case))]
    FORMATS,
    #[token("FRAGMENTS", ignore(ascii_case))]
    FRAGMENTS,
    #[token("FROM", ignore(ascii_case))]
    FROM,
    #[token("FULL", ignore(ascii_case))]
    FULL,
    #[token("FUNCTION", ignore(ascii_case))]
    FUNCTION,
    #[token("FUNCTIONS", ignore(ascii_case))]
    FUNCTIONS,
    #[token("TABLE_FUNCTIONS", ignore(ascii_case))]
    TABLE_FUNCTIONS,
    #[token("SET_VAR", ignore(ascii_case))]
    SET_VAR,
    #[token("FUSE", ignore(ascii_case))]
    FUSE,
    #[token("GET", ignore(ascii_case))]
    GET,
    #[token("GENERATED", ignore(ascii_case))]
    GENERATED,
    #[token("GEOMETRY", ignore(ascii_case))]
    GEOMETRY,
    #[token("GLOBAL", ignore(ascii_case))]
    GLOBAL,
    #[token("GRAPH", ignore(ascii_case))]
    GRAPH,
    #[token("GROUP", ignore(ascii_case))]
    GROUP,
    #[token("GZIP", ignore(ascii_case))]
    GZIP,
    #[token("HAVING", ignore(ascii_case))]
    HAVING,
    #[token("HIGH", ignore(ascii_case))]
    HIGH,
    #[token("HISTORY", ignore(ascii_case))]
    HISTORY,
    #[token("HIVE", ignore(ascii_case))]
    HIVE,
    #[token("HOUR", ignore(ascii_case))]
    HOUR,
    #[token("HOURS", ignore(ascii_case))]
    HOURS,
    #[token("ICEBERG", ignore(ascii_case))]
    ICEBERG,
    #[token("INTERSECT", ignore(ascii_case))]
    INTERSECT,
    #[token("IDENTIFIED", ignore(ascii_case))]
    IDENTIFIED,
    #[token("IDENTIFIER", ignore(ascii_case))]
    IDENTIFIER,
    #[token("IF", ignore(ascii_case))]
    IF,
    #[token("IN", ignore(ascii_case))]
    IN,
    #[token("INCREMENTAL", ignore(ascii_case))]
    INCREMENTAL,
    #[token("INDEX", ignore(ascii_case))]
    INDEX,
    #[token("INFORMATION", ignore(ascii_case))]
    INFORMATION,
    #[token("INITIALIZE", ignore(ascii_case))]
    INITIALIZE,
    #[token("INNER", ignore(ascii_case))]
    INNER,
    #[token("INSERT", ignore(ascii_case))]
    INSERT,
    #[token("INT", ignore(ascii_case))]
    INT,
    #[token("INT16", ignore(ascii_case))]
    INT16,
    #[token("INT32", ignore(ascii_case))]
    INT32,
    #[token("INT64", ignore(ascii_case))]
    INT64,
    #[token("INT8", ignore(ascii_case))]
    INT8,
    #[token("INTEGER", ignore(ascii_case))]
    INTEGER,
    #[token("INTERVAL", ignore(ascii_case))]
    INTERVAL,
    #[token("INTO", ignore(ascii_case))]
    INTO,
    #[token("INVERTED", ignore(ascii_case))]
    INVERTED,
    #[token("IMMEDIATE", ignore(ascii_case))]
    IMMEDIATE,
    #[token("IS", ignore(ascii_case))]
    IS,
    #[token("ISODOW", ignore(ascii_case))]
    ISODOW,
    #[token("ISOYEAR", ignore(ascii_case))]
    ISOYEAR,
    #[token("JOIN", ignore(ascii_case))]
    JOIN,
    #[token("JSON", ignore(ascii_case))]
    JSON,
    #[token("JULIAN", ignore(ascii_case))]
    JULIAN,
    #[token("JWT", ignore(ascii_case))]
    JWT,
    #[token("KEY", ignore(ascii_case))]
    KEY,
    #[token("KILL", ignore(ascii_case))]
    KILL,
    #[token("LATERAL", ignore(ascii_case))]
    LATERAL,
    #[token("LOCATION_PREFIX", ignore(ascii_case))]
    LOCATION_PREFIX,
    #[token("LOCKS", ignore(ascii_case))]
    LOCKS,
    #[token("LOGICAL", ignore(ascii_case))]
    LOGICAL,
    #[token("LOOP", ignore(ascii_case))]
    LOOP,
    #[token("SECONDARY", ignore(ascii_case))]
    SECONDARY,
    #[token("ROLES", ignore(ascii_case))]
    ROLES,
    /// L2DISTANCE op, from https://github.com/pgvector/pgvector
    #[token("<->")]
    L2DISTANCE,
    #[token("LEADING", ignore(ascii_case))]
    LEADING,
    #[token("LEFT", ignore(ascii_case))]
    LEFT,
    #[token("LET", ignore(ascii_case))]
    LET,
    #[token("LIKE", ignore(ascii_case))]
    LIKE,
    #[token("LIMIT", ignore(ascii_case))]
    LIMIT,
    #[token("LIST", ignore(ascii_case))]
    LIST,
    #[token("LOW", ignore(ascii_case))]
    LOW,
    #[token("LZO", ignore(ascii_case))]
    LZO,
    #[token("MASKING", ignore(ascii_case))]
    MASKING,
    #[token("MAP", ignore(ascii_case))]
    MAP,
    #[token("MAX_FILE_SIZE", ignore(ascii_case))]
    MAX_FILE_SIZE,
    #[token("MASTER_KEY", ignore(ascii_case))]
    MASTER_KEY,
    #[token("MEDIUM", ignore(ascii_case))]
    MEDIUM,
    #[token("MEMO", ignore(ascii_case))]
    MEMO,
    #[token("MEMORY", ignore(ascii_case))]
    MEMORY,
    #[token("METRICS", ignore(ascii_case))]
    METRICS,
    #[token("MICROSECONDS", ignore(ascii_case))]
    MICROSECONDS,
    #[token("MILLENNIUM", ignore(ascii_case))]
    MILLENNIUM,
    #[token("MILLISECONDS", ignore(ascii_case))]
    MILLISECONDS,
    #[token("MINUTE", ignore(ascii_case))]
    MINUTE,
    #[token("MONTH", ignore(ascii_case))]
    MONTH,
    #[token("MODIFY", ignore(ascii_case))]
    MODIFY,
    #[token("MATERIALIZED", ignore(ascii_case))]
    MATERIALIZED,
    #[token("MUST_CHANGE_PASSWORD", ignore(ascii_case))]
    MUST_CHANGE_PASSWORD,
    #[token("NON_DISPLAY", ignore(ascii_case))]
    NON_DISPLAY,
    #[token("NATURAL", ignore(ascii_case))]
    NATURAL,
    #[token("NETWORK", ignore(ascii_case))]
    NETWORK,
    #[token("DISABLED", ignore(ascii_case))]
    DISABLED,
    #[token("NDJSON", ignore(ascii_case))]
    NDJSON,
    #[token("NO_PASSWORD", ignore(ascii_case))]
    NO_PASSWORD,
    #[token("NONE", ignore(ascii_case))]
    NONE,
    #[token("NOT", ignore(ascii_case))]
    NOT,
    #[token("NOTENANTSETTING", ignore(ascii_case))]
    NOTENANTSETTING,
    #[token("DEFAULT_ROLE", ignore(ascii_case))]
    DEFAULT_ROLE,
    #[token("NULL", ignore(ascii_case))]
    NULL,
    #[token("NULLABLE", ignore(ascii_case))]
    NULLABLE,
    #[token("OBJECT", ignore(ascii_case))]
    OBJECT,
    #[token("OF", ignore(ascii_case))]
    OF,
    #[token("OFFSET", ignore(ascii_case))]
    OFFSET,
    #[token("ON", ignore(ascii_case))]
    ON,
    #[token("ON_CREATE", ignore(ascii_case))]
    ON_CREATE,
    #[token("ON_SCHEDULE", ignore(ascii_case))]
    ON_SCHEDULE,
    #[token("OPTIMIZE", ignore(ascii_case))]
    OPTIMIZE,
    #[token("OPTIONS", ignore(ascii_case))]
    OPTIONS,
    #[token("OR", ignore(ascii_case))]
    OR,
    #[token("ORC", ignore(ascii_case))]
    ORC,
    #[token("ORDER", ignore(ascii_case))]
    ORDER,
    #[token("OUTPUT_HEADER", ignore(ascii_case))]
    OUTPUT_HEADER,
    #[token("OUTER", ignore(ascii_case))]
    OUTER,
    #[token("ON_ERROR", ignore(ascii_case))]
    ON_ERROR,
    #[token("OVER", ignore(ascii_case))]
    OVER,
    #[token("OVERWRITE", ignore(ascii_case))]
    OVERWRITE,
    #[token("PARTITION", ignore(ascii_case))]
    PARTITION,
    #[token("PARQUET", ignore(ascii_case))]
    PARQUET,
    #[token("PASSWORD", ignore(ascii_case))]
    PASSWORD,
    #[token("PASSWORD_MIN_LENGTH", ignore(ascii_case))]
    PASSWORD_MIN_LENGTH,
    #[token("PASSWORD_MAX_LENGTH", ignore(ascii_case))]
    PASSWORD_MAX_LENGTH,
    #[token("PASSWORD_MIN_UPPER_CASE_CHARS", ignore(ascii_case))]
    PASSWORD_MIN_UPPER_CASE_CHARS,
    #[token("PASSWORD_MIN_LOWER_CASE_CHARS", ignore(ascii_case))]
    PASSWORD_MIN_LOWER_CASE_CHARS,
    #[token("PASSWORD_MIN_NUMERIC_CHARS", ignore(ascii_case))]
    PASSWORD_MIN_NUMERIC_CHARS,
    #[token("PASSWORD_MIN_SPECIAL_CHARS", ignore(ascii_case))]
    PASSWORD_MIN_SPECIAL_CHARS,
    #[token("PASSWORD_MIN_AGE_DAYS", ignore(ascii_case))]
    PASSWORD_MIN_AGE_DAYS,
    #[token("PASSWORD_MAX_AGE_DAYS", ignore(ascii_case))]
    PASSWORD_MAX_AGE_DAYS,
    #[token("PASSWORD_MAX_RETRIES", ignore(ascii_case))]
    PASSWORD_MAX_RETRIES,
    #[token("PASSWORD_LOCKOUT_TIME_MINS", ignore(ascii_case))]
    PASSWORD_LOCKOUT_TIME_MINS,
    #[token("PASSWORD_HISTORY", ignore(ascii_case))]
    PASSWORD_HISTORY,
    #[token("PATTERN", ignore(ascii_case))]
    PATTERN,
    #[token("PIPELINE", ignore(ascii_case))]
    PIPELINE,
    #[token("PLAINTEXT_PASSWORD", ignore(ascii_case))]
    PLAINTEXT_PASSWORD,
    #[token("POLICIES", ignore(ascii_case))]
    POLICIES,
    #[token("POLICY", ignore(ascii_case))]
    POLICY,
    #[token("POSITION", ignore(ascii_case))]
    POSITION,
    #[token("PROCESSLIST", ignore(ascii_case))]
    PROCESSLIST,
    #[token("PRIORITY", ignore(ascii_case))]
    PRIORITY,
    #[token("PURGE", ignore(ascii_case))]
    PURGE,
    #[token("PUT", ignore(ascii_case))]
    PUT,
    #[token("PARTIAL", ignore(ascii_case))]
    PARTIAL,
    #[token("QUARTER", ignore(ascii_case))]
    QUARTER,
    #[token("QUERY", ignore(ascii_case))]
    QUERY,
    #[token("QUOTE", ignore(ascii_case))]
    QUOTE,
    #[token("RANGE", ignore(ascii_case))]
    RANGE,
    #[token("RAWDEFLATE", ignore(ascii_case))]
    RAWDEFLATE,
    #[token("READ_ONLY", ignore(ascii_case))]
    READ_ONLY,
    #[token("RECLUSTER", ignore(ascii_case))]
    RECLUSTER,
    #[token("RECORD_DELIMITER", ignore(ascii_case))]
    RECORD_DELIMITER,
    #[token("REFERENCE_USAGE", ignore(ascii_case))]
    REFERENCE_USAGE,
    #[token("REFRESH", ignore(ascii_case))]
    REFRESH,
    #[token("REGEXP", ignore(ascii_case))]
    REGEXP,
    #[token("RENAME", ignore(ascii_case))]
    RENAME,
    #[token("REPLACE", ignore(ascii_case))]
    REPLACE,
    #[token("RETURN_FAILED_ONLY", ignore(ascii_case))]
    RETURN_FAILED_ONLY,
    #[token("REVERSE", ignore(ascii_case))]
    REVERSE,
    #[token("SAMPLE", ignore(ascii_case))]
    SAMPLE,
    #[token("MERGE", ignore(ascii_case))]
    MERGE,
    #[token("MATCHED", ignore(ascii_case))]
    MATCHED,
    #[token("MISSING_FIELD_AS", ignore(ascii_case))]
    MISSING_FIELD_AS,
    #[token("NULL_FIELD_AS", ignore(ascii_case))]
    NULL_FIELD_AS,
    #[token("UNMATCHED", ignore(ascii_case))]
    UNMATCHED,
    #[token("ROW", ignore(ascii_case))]
    ROW,
    #[token("ROWS", ignore(ascii_case))]
    ROWS,
    #[token("ROW_TAG", ignore(ascii_case))]
    ROW_TAG,
    #[token("GRANT", ignore(ascii_case))]
    GRANT,
    #[token("REPEAT", ignore(ascii_case))]
    REPEAT,
    #[token("ROLE", ignore(ascii_case))]
    ROLE,
    #[token("PRECEDING", ignore(ascii_case))]
    PRECEDING,
    #[token("PRECISION", ignore(ascii_case))]
    PRECISION,
    #[token("PRESIGN", ignore(ascii_case))]
    PRESIGN,
    #[token("PRIVILEGES", ignore(ascii_case))]
    PRIVILEGES,
    #[token("QUALIFY", ignore(ascii_case))]
    QUALIFY,
    #[token("REMOVE", ignore(ascii_case))]
    REMOVE,
    #[token("RETAIN", ignore(ascii_case))]
    RETAIN,
    #[token("REVOKE", ignore(ascii_case))]
    REVOKE,
    #[token("RECURSIVE", ignore(ascii_case))]
    RECURSIVE,
    #[token("RETURN", ignore(ascii_case))]
    RETURN,
    #[token("RETURNS", ignore(ascii_case))]
    RETURNS,
    #[token("RESULTSET", ignore(ascii_case))]
    RESULTSET,
    #[token("RUN", ignore(ascii_case))]
    RUN,
    #[token("GRANTS", ignore(ascii_case))]
    GRANTS,
    #[token("REFRESH_MODE", ignore(ascii_case))]
    REFRESH_MODE,
    #[token("RIGHT", ignore(ascii_case))]
    RIGHT,
    #[token("RLIKE", ignore(ascii_case))]
    RLIKE,
    #[token("RAW", ignore(ascii_case))]
    RAW,
    #[token("OPTIMIZED", ignore(ascii_case))]
    OPTIMIZED,
    #[token("SCHEMA", ignore(ascii_case))]
    SCHEMA,
    #[token("SCHEMAS", ignore(ascii_case))]
    SCHEMAS,
    #[token("SECOND", ignore(ascii_case))]
    SECOND,
    #[token("MILLISECOND", ignore(ascii_case))]
    MILLISECOND,
    #[token("SELECT", ignore(ascii_case))]
    SELECT,
    #[token("PIVOT", ignore(ascii_case))]
    PIVOT,
    #[token("UNPIVOT", ignore(ascii_case))]
    UNPIVOT,
    #[token("SEGMENT", ignore(ascii_case))]
    SEGMENT,
    #[token("SET", ignore(ascii_case))]
    SET,
    #[token("UNSET", ignore(ascii_case))]
    UNSET,
    #[token("SESSION", ignore(ascii_case))]
    SESSION,
    #[token("SETTINGS", ignore(ascii_case))]
    SETTINGS,
    #[token("STAGES", ignore(ascii_case))]
    STAGES,
    #[token("STATISTIC", ignore(ascii_case))]
    STATISTIC,
    #[token("SUMMARY", ignore(ascii_case))]
    SUMMARY,
    #[token("SHA256_PASSWORD", ignore(ascii_case))]
    SHA256_PASSWORD,
    #[token("SHOW", ignore(ascii_case))]
    SHOW,
    #[token("SINCE", ignore(ascii_case))]
    SINCE,
    #[token("SIGNED", ignore(ascii_case))]
    SIGNED,
    #[token("SINGLE", ignore(ascii_case))]
    SINGLE,
    #[token("SIZE_LIMIT", ignore(ascii_case))]
    SIZE_LIMIT,
    #[token("MAX_FILES", ignore(ascii_case))]
    MAX_FILES,
    #[token("SKIP_HEADER", ignore(ascii_case))]
    SKIP_HEADER,
    #[token("SMALLINT", ignore(ascii_case))]
    SMALLINT,
    #[token("SNAPPY", ignore(ascii_case))]
    SNAPPY,
    #[token("SNAPSHOT", ignore(ascii_case))]
    SNAPSHOT,
    #[token("SPLIT_SIZE", ignore(ascii_case))]
    SPLIT_SIZE,
    #[token("STAGE", ignore(ascii_case))]
    STAGE,
    #[token("SYNTAX", ignore(ascii_case))]
    SYNTAX,
    #[token("USAGE", ignore(ascii_case))]
    USAGE,
    #[token("UPDATE", ignore(ascii_case))]
    UPDATE,
    #[token("UPLOAD", ignore(ascii_case))]
    UPLOAD,
    #[token("SEQUENCE", ignore(ascii_case))]
    SEQUENCE,
    #[token("SHARE", ignore(ascii_case))]
    SHARE,
    #[token("SHARES", ignore(ascii_case))]
    SHARES,
    #[token("SUPER", ignore(ascii_case))]
    SUPER,
    #[token("STATUS", ignore(ascii_case))]
    STATUS,
    #[token("STORED", ignore(ascii_case))]
    STORED,
    #[token("STREAM", ignore(ascii_case))]
    STREAM,
    #[token("STREAMS", ignore(ascii_case))]
    STREAMS,
    #[token("STRING", ignore(ascii_case))]
    STRING,
    #[token("SUBSTRING", ignore(ascii_case))]
    SUBSTRING,
    #[token("SUBSTR", ignore(ascii_case))]
    SUBSTR,
    #[token("SEMI", ignore(ascii_case))]
    SEMI,
    #[token("SOUNDS", ignore(ascii_case))]
    SOUNDS,
    #[token("SYNC", ignore(ascii_case))]
    SYNC,
    #[token("SYSTEM", ignore(ascii_case))]
    SYSTEM,
    #[token("STORAGE_TYPE", ignore(ascii_case))]
    STORAGE_TYPE,
    #[token("TABLE", ignore(ascii_case))]
    TABLE,
    #[token("TABLES", ignore(ascii_case))]
    TABLES,
    #[token("TARGET_LAG", ignore(ascii_case))]
    TARGET_LAG,
    #[token("TEXT", ignore(ascii_case))]
    TEXT,
    #[token("LONGTEXT", ignore(ascii_case))]
    LONGTEXT,
    #[token("MEDIUMTEXT", ignore(ascii_case))]
    MEDIUMTEXT,
    #[token("TINYTEXT", ignore(ascii_case))]
    TINYTEXT,
    #[token("TENANTSETTING", ignore(ascii_case))]
    TENANTSETTING,
    #[token("TENANTS", ignore(ascii_case))]
    TENANTS,
    #[token("TENANT", ignore(ascii_case))]
    TENANT,
    #[token("THEN", ignore(ascii_case))]
    THEN,
    #[token("TIMESTAMP", ignore(ascii_case))]
    TIMESTAMP,
    #[token("TIMEZONE_HOUR", ignore(ascii_case))]
    TIMEZONE_HOUR,
    #[token("TIMEZONE_MINUTE", ignore(ascii_case))]
    TIMEZONE_MINUTE,
    #[token("TIMEZONE", ignore(ascii_case))]
    TIMEZONE,
    #[token("TINYINT", ignore(ascii_case))]
    TINYINT,
    #[token("TO", ignore(ascii_case))]
    TO,
    #[token("TOKEN", ignore(ascii_case))]
    TOKEN,
    #[token("TRAILING", ignore(ascii_case))]
    TRAILING,
    #[token("TRANSIENT", ignore(ascii_case))]
    TRANSIENT,
    #[token("TRIM", ignore(ascii_case))]
    TRIM,
    #[token("TRUE", ignore(ascii_case))]
    TRUE,
    #[token("TRUNCATE", ignore(ascii_case))]
    TRUNCATE,
    #[token("TRY_CAST", ignore(ascii_case))]
    TRY_CAST,
    #[token("TSV", ignore(ascii_case))]
    TSV,
    #[token("TUPLE", ignore(ascii_case))]
    TUPLE,
    #[token("TYPE", ignore(ascii_case))]
    TYPE,
    #[token("UNBOUNDED", ignore(ascii_case))]
    UNBOUNDED,
    #[token("UNION", ignore(ascii_case))]
    UNION,
    #[token("UINT16", ignore(ascii_case))]
    UINT16,
    #[token("UINT32", ignore(ascii_case))]
    UINT32,
    #[token("UINT64", ignore(ascii_case))]
    UINT64,
    #[token("UINT8", ignore(ascii_case))]
    UINT8,
    #[token("UNDROP", ignore(ascii_case))]
    UNDROP,
    #[token("UNSIGNED", ignore(ascii_case))]
    UNSIGNED,
    #[token("URL", ignore(ascii_case))]
    URL,
    #[token("METHOD", ignore(ascii_case))]
    METHOD,
    #[token("AUTHORIZATION_HEADER", ignore(ascii_case))]
    AUTHORIZATION_HEADER,
    #[token("USE", ignore(ascii_case))]
    USE,
    #[token("USER", ignore(ascii_case))]
    USER,
    #[token("USERS", ignore(ascii_case))]
    USERS,
    #[token("USING", ignore(ascii_case))]
    USING,
    #[token("VACUUM", ignore(ascii_case))]
    VACUUM,
    #[token("VALUES", ignore(ascii_case))]
    VALUES,
    #[token("VALIDATION_MODE", ignore(ascii_case))]
    VALIDATION_MODE,
    #[token("VARBINARY", ignore(ascii_case))]
    VARBINARY,
    #[token("VARCHAR", ignore(ascii_case))]
    VARCHAR,
    #[token("VARIANT", ignore(ascii_case))]
    VARIANT,
    #[token("VARIABLE", ignore(ascii_case))]
    VARIABLE,
    #[token("VERBOSE", ignore(ascii_case))]
    VERBOSE,
    #[token("VIEW", ignore(ascii_case))]
    VIEW,
    #[token("VIEWS", ignore(ascii_case))]
    VIEWS,
    #[token("VIRTUAL", ignore(ascii_case))]
    VIRTUAL,
    #[token("WHEN", ignore(ascii_case))]
    WHEN,
    #[token("WHERE", ignore(ascii_case))]
    WHERE,
    #[token("WHILE", ignore(ascii_case))]
    WHILE,
    #[token("WINDOW", ignore(ascii_case))]
    WINDOW,
    #[token("WITH", ignore(ascii_case))]
    WITH,
    #[token("XML", ignore(ascii_case))]
    XML,
    #[token("XOR", ignore(ascii_case))]
    XOR,
    #[token("XZ", ignore(ascii_case))]
    XZ,
    #[token("YEAR", ignore(ascii_case))]
    YEAR,
    #[token("ZSTD", ignore(ascii_case))]
    ZSTD,
    #[token("NULLIF", ignore(ascii_case))]
    NULLIF,
    #[token("COALESCE", ignore(ascii_case))]
    COALESCE,
    #[token("RANDOM", ignore(ascii_case))]
    RANDOM,
    #[token("IFNULL", ignore(ascii_case))]
    IFNULL,
    #[token("NULLS", ignore(ascii_case))]
    NULLS,
    #[token("FIRST", ignore(ascii_case))]
    FIRST,
    #[token("LAST", ignore(ascii_case))]
    LAST,
    #[token("IGNORE_RESULT", ignore(ascii_case))]
    IGNORE_RESULT,
    #[token("GROUPING", ignore(ascii_case))]
    GROUPING,
    #[token("SETS", ignore(ascii_case))]
    SETS,
    #[token("CUBE", ignore(ascii_case))]
    CUBE,
    #[token("ROLLUP", ignore(ascii_case))]
    ROLLUP,
    #[token("INDEXES", ignore(ascii_case))]
    INDEXES,
    #[token("ADDRESS", ignore(ascii_case))]
    ADDRESS,
    #[token("OWNERSHIP", ignore(ascii_case))]
    OWNERSHIP,
    #[token("READ", ignore(ascii_case))]
    READ,
    #[token("WRITE", ignore(ascii_case))]
    WRITE,
    #[token("UDF", ignore(ascii_case))]
    UDF,
    #[token("HANDLER", ignore(ascii_case))]
    HANDLER,
    #[token("LANGUAGE", ignore(ascii_case))]
    LANGUAGE,
    #[token("TASK", ignore(ascii_case))]
    TASK,
    #[token("TASKS", ignore(ascii_case))]
    TASKS,
    #[token("TOP", ignore(ascii_case))]
    TOP,
    #[token("WAREHOUSE", ignore(ascii_case))]
    WAREHOUSE,
    #[token("SCHEDULE", ignore(ascii_case))]
    SCHEDULE,
    #[token("SUSPEND_TASK_AFTER_NUM_FAILURES", ignore(ascii_case))]
    SUSPEND_TASK_AFTER_NUM_FAILURES,
    #[token("CRON", ignore(ascii_case))]
    CRON,
    #[token("EXECUTE", ignore(ascii_case))]
    EXECUTE,
    #[token("SUSPEND", ignore(ascii_case))]
    SUSPEND,
    #[token("RESUME", ignore(ascii_case))]
    RESUME,
    #[token("PIPE", ignore(ascii_case))]
    PIPE,
    #[token("NOTIFICATION", ignore(ascii_case))]
    NOTIFICATION,
    #[token("INTEGRATION", ignore(ascii_case))]
    INTEGRATION,
    #[token("ENABLED", ignore(ascii_case))]
    ENABLED,
    #[token("WEBHOOK", ignore(ascii_case))]
    WEBHOOK,
    #[token("ERROR_INTEGRATION", ignore(ascii_case))]
    ERROR_INTEGRATION,
    #[token("AUTO_INGEST", ignore(ascii_case))]
    AUTO_INGEST,
    #[token("PIPE_EXECUTION_PAUSED", ignore(ascii_case))]
    PIPE_EXECUTION_PAUSED,
    #[token("PREFIX", ignore(ascii_case))]
    PREFIX,
    #[token("MODIFIED_AFTER", ignore(ascii_case))]
    MODIFIED_AFTER,
    #[token("UNTIL", ignore(ascii_case))]
    UNTIL,
    #[token("BEGIN", ignore(ascii_case))]
    BEGIN,
    #[token("TRANSACTION", ignore(ascii_case))]
    TRANSACTION,
    #[token("COMMIT", ignore(ascii_case))]
    COMMIT,
    #[token("ABORT", ignore(ascii_case))]
    ABORT,
    #[token("ROLLBACK", ignore(ascii_case))]
    ROLLBACK,
    #[token("TEMPORARY", ignore(ascii_case))]
    TEMPORARY,
    #[token("SECONDS", ignore(ascii_case))]
    SECONDS,
    #[token("DAYS", ignore(ascii_case))]
    DAYS,
    #[token("DICTIONARY", ignore(ascii_case))]
    DICTIONARY,
    #[token("DICTIONARIES", ignore(ascii_case))]
    DICTIONARIES,
    #[token("PRIMARY", ignore(ascii_case))]
    PRIMARY,
    #[token("SOURCE", ignore(ascii_case))]
    SOURCE,
}

// Reference: https://www.postgresql.org/docs/current/sql-keywords-appendix.html
impl TokenKind {
    pub fn is_literal(&self) -> bool {
        matches!(
            self,
            LiteralInteger
                | LiteralFloat
                | LiteralString
                | LiteralCodeString
                | PGLiteralHex
                | MySQLLiteralHex
        )
    }

    pub fn is_keyword(&self) -> bool {
        !matches!(
            self,
            Ident
                | LiteralString
                | LiteralCodeString
                | PGLiteralHex
                | MySQLLiteralHex
                | LiteralInteger
                | LiteralFloat
                | HintPrefix
                | HintSuffix
                | DoubleEq
                | Eq
                | NotEq
                | Lt
                | Gt
                | Lte
                | Gte
                | Spaceship
                | Plus
                | Minus
                | Multiply
                | Divide
                | IntDiv
                | Modulo
                | StringConcat
                | LParen
                | RParen
                | Comma
                | Dot
                | Colon
                | DoubleColon
                | ColonEqual
                | SemiColon
                | Backslash
                | LBracket
                | RBracket
                | BitWiseAnd
                | BitWiseOr
                | Caret
                | Factorial
                | LBrace
                | RBrace
                | RArrow
                | LongRArrow
                | HashRArrow
                | HashLongRArrow
                | FatRArrow
                | BitWiseXor
                | BitWiseNot
                | TildeAsterisk
                | ExclamationMarkTilde
                | ExclamationMarkTildeAsterisk
                | ShiftLeft
                | ShiftRight
                | DoubleExclamationMark
                | Abs
                | SquareRoot
                | CubeRoot
                | L2DISTANCE
                | Placeholder
                | QuestionOr
                | QuestionAnd
                | ArrowAt
                | AtArrow
                | AtQuestion
                | AtAt
                | HashMinus
                | EOI
        )
    }

    pub fn is_reserved_function_name(&self) -> bool {
        matches!(
            self,
            TokenKind::ALL
            // | TokenKind::ANALYSE
            | TokenKind::ANALYZE
            | TokenKind::AND
            // | TokenKind::ANY
            | TokenKind::ASC
            // | TokenKind::ASYMMETRIC
            | TokenKind::BETWEEN
            | TokenKind::BIGINT
            // | TokenKind::BIT
            | TokenKind::BOOLEAN
            | TokenKind::BOTH
            | TokenKind::CASE
            | TokenKind::CAST
            // | TokenKind::CHECK
            // | TokenKind::COALESCE
            // | TokenKind::COLLATE
            // | TokenKind::COLUMN
            // | TokenKind::CONSTRAINT
            // | TokenKind::CURRENT_CATALOG
            // | TokenKind::CURRENT_DATE
            // | TokenKind::CURRENT_ROLE
            // | TokenKind::CURRENT_TIME
            // | TokenKind::CURRENT_TIMESTAMP
            // | TokenKind::CURRENT_USER
            // | TokenKind::DEC
            // | TokenKind::DECIMAL
            | TokenKind::DEFAULT
            // | TokenKind::DEFERRABLE
            | TokenKind::DESC
            | TokenKind::DISTINCT
            | TokenKind::DO
            | TokenKind::ELSE
            | TokenKind::END
            | TokenKind::EXISTS
            | TokenKind::EXTRACT
            | TokenKind::DATE_PART
            | TokenKind::FALSE
            | TokenKind::FLOAT
            // | TokenKind::FOREIGN
            // | TokenKind::GREATEST
            // | TokenKind::GROUPING
            | TokenKind::CUBE
            | TokenKind::ROLLUP
            // | TokenKind::IFNULL
            | TokenKind::IN
            | TokenKind::IDENTIFIER
            // | TokenKind::INITIALLY
            // | TokenKind::INOUT
            | TokenKind::INT
            | TokenKind::INTEGER
            | TokenKind::INTERVAL
            | TokenKind::LATERAL
            | TokenKind::LEADING
            // | TokenKind::LEAST
            // | TokenKind::LOCALTIME
            // | TokenKind::LOCALTIMESTAMP
            // | TokenKind::NATIONAL
            // | TokenKind::NCHAR
            // | TokenKind::NONE
            // | TokenKind::NORMALIZE
            | TokenKind::NOT
            | TokenKind::NULL
            // | TokenKind::NULLIF
            // | TokenKind::NUMERIC
            // | TokenKind::ONLY
            | TokenKind::OR
            // | TokenKind::OUT
            // | TokenKind::OVERLAY
            // | TokenKind::PLACING
            | TokenKind::POSITION
            // | TokenKind::PRIMARY
            // | TokenKind::REAL
            // | TokenKind::REFERENCES
            // | TokenKind::ROW
            | TokenKind::SELECT
            // | TokenKind::SESSION_USER
            // | TokenKind::SETOF
            | TokenKind::SHARE
            | TokenKind::SHARES
            | TokenKind::SMALLINT
            | TokenKind::SOME
            | TokenKind::SUBSTRING
            | TokenKind::SUBSTR
            // | TokenKind::SYMMETRIC
            | TokenKind::TABLE
            | TokenKind::THEN
            // | TokenKind::TIME
            | TokenKind::TIMESTAMP
            | TokenKind::TRAILING
            // | TokenKind::TREAT
            | TokenKind::TRIM
            | TokenKind::TRUE
            | TokenKind::TRY_CAST
            // | TokenKind::UNIQUE
            //| TokenKind::USER
            | TokenKind::USING
            | TokenKind::VALUES
            | TokenKind::VARCHAR
            // | TokenKind::VARIADIC
            // | TokenKind::XMLATTRIBUTES
            // | TokenKind::XMLCONCAT
            // | TokenKind::XMLELEMENT
            // | TokenKind::XMLEXISTS
            // | TokenKind::XMLFOREST
            // | TokenKind::XMLNAMESPACES
            // | TokenKind::XMLPARSE
            // | TokenKind::XMLPI
            // | TokenKind::XMLROOT
            // | TokenKind::XMLSERIALIZE
            // | TokenKind::XMLTABLE
            | TokenKind::WHEN
            // | TokenKind::ARRAY
            | TokenKind::AS
            // | TokenKind::CHAR
            | TokenKind::CHARACTER
            | TokenKind::CREATE
            | TokenKind::ATTACH
            | TokenKind::EXCEPT
            // | TokenKind::FETCH
            | TokenKind::FOR
            | TokenKind::FROM
            // | TokenKind::GRANT
            | TokenKind::GROUP
            | TokenKind::HAVING
            | TokenKind::INTERSECT
            | TokenKind::INTO
            | TokenKind::LIMIT
            | TokenKind::OFFSET
            | TokenKind::ON
            | TokenKind::OF
            | TokenKind::ORDER
            | TokenKind::OVER
            | TokenKind::QUALIFY
            | TokenKind::ROWS
            // | TokenKind::PRECISION
            // | TokenKind::RETURNING
            | TokenKind::TO
            | TokenKind::UNION
            | TokenKind::WHERE
            // | TokenKind::WINDOW
            | TokenKind::WITH
            | TokenKind::DATE_ADD
            | TokenKind::DATE_SUB
            | TokenKind::DATE_TRUNC
            | TokenKind::IGNORE_RESULT
        )
    }

    pub fn is_reserved_ident(&self, after_as: bool) -> bool {
        match self {
            | TokenKind::ALL
            // | TokenKind::ANALYSE
            | TokenKind::ANALYZE
            | TokenKind::AND
            | TokenKind::ANY
            | TokenKind::FUNCTION
            | TokenKind::ASC
            | TokenKind::ANTI
            // | TokenKind::ASYMMETRIC
            // | TokenKind::AUTHORIZATION
            // | TokenKind::BINARY
            | TokenKind::BOTH
            | TokenKind::BLOCK
            | TokenKind::CASE
            | TokenKind::CAST
            // | TokenKind::CHECK
            // | TokenKind::COLLATE
            // | TokenKind::COLLATION
            // | TokenKind::COLUMN
            // | TokenKind::CONCURRENTLY
            // | TokenKind::CONSTRAINT
            | TokenKind::CONNECTION
            | TokenKind::CROSS
            // | TokenKind::CURRENT_CATALOG
            // | TokenKind::CURRENT_DATE
            // | TokenKind::CURRENT_ROLE
            // | TokenKind::CURRENT_SCHEMA
            // | TokenKind::CURRENT_TIME
            | TokenKind::CURRENT_TIMESTAMP
            // | TokenKind::CURRENT_USER
            // | TokenKind::DEFERRABLE
            | TokenKind::DESC
            | TokenKind::DISTINCT
            | TokenKind::DO
            | TokenKind::ELSE
            | TokenKind::END
            | TokenKind::FALSE
            // | TokenKind::FOREIGN
            // | TokenKind::FREEZE
            | TokenKind::FOR
            | TokenKind::FULL
            // | TokenKind::ILIKE
            | TokenKind::IN
            | TokenKind::IDENTIFIER
            // | TokenKind::INITIALLY
            | TokenKind::INNER
            | TokenKind::IS
            | TokenKind::JOIN
            | TokenKind::LATERAL
            | TokenKind::LEADING
            | TokenKind::LEFT
            | TokenKind::LIKE
            // | TokenKind::LOCALTIME
            // | TokenKind::LOCALTIMESTAMP
            | TokenKind::NATURAL
            | TokenKind::NOT
            | TokenKind::NULL
            // | TokenKind::ONLY
            | TokenKind::OR
            // | TokenKind::PLACING
            // | TokenKind::PRIMARY
            // | TokenKind::REFERENCES
            | TokenKind::RIGHT
            | TokenKind::SELECT
            | TokenKind::PIVOT
            | TokenKind::UNPIVOT
            | TokenKind::ROW
            | TokenKind::ROWS
            // | TokenKind::SESSION_USER
            // | TokenKind::SIMILAR
            | TokenKind::SOME
            | TokenKind::SEMI
            | TokenKind::SAMPLE
            // | TokenKind::SYMMETRIC
            // | TokenKind::TABLESAMPLE
            | TokenKind::THEN
            | TokenKind::TRAILING
            | TokenKind::TRANSACTION
            | TokenKind::TRUE
            // | TokenKind::UNIQUE
            //| TokenKind::USER
            | TokenKind::USING
            // | TokenKind::VARIADIC
            // | TokenKind::VERBOSE
            | TokenKind::WHEN => true,
            | TokenKind::ARRAY
            | TokenKind::AS
            | TokenKind::BETWEEN
            | TokenKind::CREATE
            | TokenKind::ATTACH
            | TokenKind::EXCEPT
            // | TokenKind::FETCH
            | TokenKind::FROM
            | TokenKind::GRANT
            | TokenKind::GROUP
            | TokenKind::HAVING
            | TokenKind::INTERSECT
            | TokenKind::INTO
            // | TokenKind::ISNULL
            | TokenKind::LIMIT
            | TokenKind::FORMAT
            // | TokenKind::NOTNULL
            | TokenKind::OFFSET
            | TokenKind::ON
            | TokenKind::OF
            | TokenKind::ORDER
            | TokenKind::OVER
            | TokenKind::PARTITION
            | TokenKind::QUALIFY
            | TokenKind::RANGE
            // | TokenKind::OVERLAPS
            // | TokenKind::RETURNING
            | TokenKind::STAGE
            | TokenKind::UDF
            | TokenKind::SHARE
            | TokenKind::SHARES
            | TokenKind::TO
            | TokenKind::UNION
            | TokenKind::WHERE
            | TokenKind::WINDOW
            | TokenKind::WITH
            | TokenKind::IGNORE_RESULT
            | TokenKind::MASKING
            | TokenKind::POLICY
            | TokenKind::TASK
            | TokenKind::PIPE
            | TokenKind::STREAM
            | TokenKind::NOTIFICATION
            if !after_as => true,
            _ => false
        }
    }
}

pub fn all_reserved_keywords() -> Vec<String> {
    let mut result = Vec::new();
    for token in TokenKind::iter() {
        result.push(format!("{:?}", token));
    }
    result
}
