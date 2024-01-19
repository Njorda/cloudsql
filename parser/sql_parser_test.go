package parser

import (
	"testing"

	"github.com/Njorda.cloudsql/lexer"
)

func TestParser(t *testing.T) {
	input := "SELECT table, name, output FROM table WHERE column = 'value';"
	lexer := lexer.NewLexer(input)
	parser := &Parser{query: *lexer}
	query, err := parser.ParseQuery()
	if err != nil {
		t.Fatal(err)
	}
	if len(query.Select) != 3 {
		t.Logf("Expected 3 columns, %v", query.Select)
	}
	if query.From != "table" {
		t.Fatal("Expected from to be inner query")
	}
	if query.Equals.Key != "column" {
		t.Fatal("Expected where key to be column")
	}
	if query.Equals.Value != "value" {
		t.Fatal("Expected where value to be value")
	}
}

func TestParserPrefix(t *testing.T) {
	input := "SELECT table, name, output FROM table WHERE column = 'value%';"
	lexer := lexer.NewLexer(input)
	parser := &Parser{query: *lexer}
	query, err := parser.ParseQuery()
	if err != nil {
		t.Fatal(err)
	}
	if len(query.Select) != 3 {
		t.Logf("Expected 3 columns, %v", query.Select)
	}
	if query.From != "table" {
		t.Fatal("Expected from to be inner query")
	}
	if query.Where.Key != "column" {
		t.Fatal("Expected where key to be column")
	}
	if query.Where.Value != "value" {
		t.Fatal("Expected where value to be value")
	}
}
