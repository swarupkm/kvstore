package query

import (
	"strings"
	"unicode"
)

type TokenType int

const (
	TokKeyword TokenType = iota
	TokIdent
	TokString
	TokEqual
	TokEOF
	TokUnknown
)

type Token struct {
	Type    TokenType
	Value   string
}

var keywords = map[string]bool{
	"SET": true, "GET": true, "DELETE": true,
	"SELECT": true, "WHERE": true, "STARTS_WITH": true,
	"BETWEEN": true, "AND": true, "VALUE": true,
	"KEY": true,
}

type Lexer struct {
	input  string
	pos    int
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: strings.TrimSpace(input)}
}

func (l *Lexer) Next() Token {
	l.skipWhitespace()
	if l.pos >= len(l.input) {
		return Token{Type: TokEOF}
	}

	ch := l.input[l.pos]

	// quoted string
	if ch == '"' {
		return l.readString()
	}

	// equals sign
	if ch == '=' {
		l.pos++
		return Token{Type: TokEqual, Value: "="}
	}

	// word (keyword or identifier)
	if unicode.IsLetter(rune(ch)) || ch == '_' || ch == ':' {
		return l.readWord()
	}

	l.pos++
	return Token{Type: TokUnknown, Value: string(ch)}
}

func (l *Lexer) readString() Token {
	l.pos++ // skip opening quote
	start := l.pos
	for l.pos < len(l.input) && l.input[l.pos] != '"' {
		l.pos++
	}
	val := l.input[start:l.pos]
	if l.pos < len(l.input) {
		l.pos++ // skip closing quote
	}
	return Token{Type: TokString, Value: val}
}

func (l *Lexer) readWord() Token {
	start := l.pos
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if !unicode.IsLetter(rune(ch)) && !unicode.IsDigit(rune(ch)) && ch != '_' && ch != ':' {
			break
		}
		l.pos++
	}
	word := l.input[start:l.pos]
	upper := strings.ToUpper(word)
	if keywords[upper] {
		return Token{Type: TokKeyword, Value: upper}
	}
	return Token{Type: TokIdent, Value: word}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

// Peek returns the next token without consuming it
func (l *Lexer) Peek() Token {
	saved := l.pos
	tok := l.Next()
	l.pos = saved
	return tok
}