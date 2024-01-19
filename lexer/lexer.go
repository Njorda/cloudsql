package lexer

import (
	"unicode"
)

type TokenType int

var keywords = []string{"SELECT", "FROM", "WHERE"}

const (
	TOKEN_EOF TokenType = iota
	TOKEN_ERROR
	TOKEN_KEYWORD
	TOKEN_IDENTIFIER
	TOKEN_SYMBOL
)

type Token struct {
	Type    TokenType
	Literal string
}

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case 0:
		tok = Token{Type: TOKEN_EOF, Literal: string("")}
	case '=', ';', '(', ')', ',', '\'', '/':
		tok = Token{Type: TOKEN_SYMBOL, Literal: string(l.ch)}
		l.readChar()
	default:
		if isLetter(l.ch) {

			literal := l.readIdentifier()
			if keyword(literal) {
				tok = Token{Type: TOKEN_KEYWORD, Literal: literal}
				return tok
			}
			tok = Token{Type: TOKEN_IDENTIFIER, Literal: literal}
			return tok
		}
		tok = Token{Type: TOKEN_ERROR, Literal: string(l.ch)}
		l.readChar()
	}
	return tok
}

func keyword(literal string) bool {
	for _, keyword := range keywords {
		if keyword == literal {
			return true
		}
	}
	return false
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) {
		l.readChar()
	}
	return l.input[position:l.position]
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_' || ch == '%'
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}
