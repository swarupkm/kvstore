package query

import "fmt"

type Parser struct {
	lexer *Lexer
}

func NewParser(input string) *Parser {
	return &Parser{lexer: NewLexer(input)}
}

func (p *Parser) Parse() (*Statement, error) {
	tok := p.lexer.Next()
	if tok.Type != TokKeyword {
		return nil, fmt.Errorf("expected keyword, got %q", tok.Value)
	}

	switch tok.Value {
	case "SET":
		return p.parseSet()
	case "GET":
		return p.parseGet()
	case "DELETE":
		return p.parseDelete()
	case "SELECT":
		return p.parseSelect()
	default:
		return nil, fmt.Errorf("unknown statement: %q", tok.Value)
	}
}

// SET key = "value"
func (p *Parser) parseSet() (*Statement, error) {
	key, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	if err := p.expectEqual(); err != nil {
		return nil, err
	}
	val, err := p.expectString()
	if err != nil {
		return nil, err
	}
	return &Statement{Type: StmtSet, Key: key, Value: val}, nil
}

// GET key
func (p *Parser) parseGet() (*Statement, error) {
	key, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	return &Statement{Type: StmtGet, Key: key}, nil
}

// DELETE key
func (p *Parser) parseDelete() (*Statement, error) {
	key, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	return &Statement{Type: StmtDelete, Key: key}, nil
}

// SELECT value WHERE key = "x"
// SELECT value WHERE key STARTS_WITH "x"
// SELECT value WHERE key BETWEEN "x" AND "y"
func (p *Parser) parseSelect() (*Statement, error) {
	// expect VALUE keyword
	tok := p.lexer.Next()
	if tok.Type != TokKeyword || tok.Value != "VALUE" {
		return nil, fmt.Errorf("expected VALUE after SELECT, got %q", tok.Value)
	}

	// expect WHERE
	tok = p.lexer.Next()
	if tok.Type != TokKeyword || tok.Value != "WHERE" {
		return nil, fmt.Errorf("expected WHERE, got %q", tok.Value)
	}

	// expect KEY
	tok = p.lexer.Next()
	if tok.Type != TokKeyword || tok.Value != "KEY" {
		return nil, fmt.Errorf("expected KEY, got %q", tok.Value)
	}

	// expect operator
	op := p.lexer.Next()
	switch {
	case op.Type == TokEqual:
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return &Statement{
			Type:      StmtSelect,
			Condition: &Condition{Op: OpEqual, Value: val},
		}, nil

	case op.Type == TokKeyword && op.Value == "STARTS_WITH":
		val, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return &Statement{
			Type:      StmtSelect,
			Condition: &Condition{Op: OpStartsWith, Value: val},
		}, nil

	case op.Type == TokKeyword && op.Value == "BETWEEN":
		start, err := p.expectString()
		if err != nil {
			return nil, err
		}
		// expect AND
		and := p.lexer.Next()
		if and.Type != TokKeyword || and.Value != "AND" {
			return nil, fmt.Errorf("expected AND in BETWEEN, got %q", and.Value)
		}
		end, err := p.expectString()
		if err != nil {
			return nil, err
		}
		return &Statement{
			Type:      StmtSelect,
			Condition: &Condition{Op: OpBetween, Value: start, End: end},
		}, nil

	default:
		return nil, fmt.Errorf("unknown operator: %q", op.Value)
	}
}

func (p *Parser) expectIdent() (string, error) {
	tok := p.lexer.Next()
	if tok.Type != TokIdent && tok.Type != TokKeyword {
		return "", fmt.Errorf("expected identifier, got %q", tok.Value)
	}
	return tok.Value, nil
}

func (p *Parser) expectEqual() error {
	tok := p.lexer.Next()
	if tok.Type != TokEqual {
		return fmt.Errorf("expected '=', got %q", tok.Value)
	}
	return nil
}

func (p *Parser) expectString() (string, error) {
	tok := p.lexer.Next()
	if tok.Type != TokString {
		return "", fmt.Errorf("expected quoted string, got %q", tok.Value)
	}
	return tok.Value, nil
}