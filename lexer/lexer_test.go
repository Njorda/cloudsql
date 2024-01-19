package lexer

import (
	"fmt"
	"testing"
)

func TestLexer(t *testing.T) {
	input := "SELECT table, name, output FROM (SELECT * FROM tmp) table WHERE column = 'value';"
	lexer := NewLexer(input)
	i := 0
	expected := []string{"SELECT", "table", ",", "name", ",", "output", "FROM", "(", "SELECT", "*", "FROM", "tmp", ")", "table", "WHERE", "column", "=", "'", "value", "'", ";"}

	for tok := lexer.NextToken(); tok.Type != TOKEN_EOF; tok = lexer.NextToken() {
		if tok.Literal != expected[i] {
			t.Fatal(fmt.Errorf("Error: %v", "test"))
		}
		i++
	}
}

func TestKeyword(t *testing.T) {
	input := "SELECT table, name, output FROM table WHERE column = 'value';"
	lexer := NewLexer(input)
	i := 0
	expected := []bool{true, false, false, false, false, false, true, false, true, false, false, false, false, false, false}
	keyword := []string{"", "SELECT", "", "", "", "", "FROM", "", "WHERE", "", "", "", "", "", ""}
	for tok := lexer.NextToken(); tok.Type != TOKEN_EOF; tok = lexer.NextToken() {
		if expected[i] && tok.Type != TOKEN_KEYWORD && tok.Literal != keyword[i] {
			t.Fatal(fmt.Errorf("Error: %v , %v", expected[i], tok.Literal))
		}
		i++
	}
}
