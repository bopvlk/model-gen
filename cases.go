package main

import (
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func firstLetterToLower(s string) string {

	if len(s) == 0 {
		return s
	}

	r := []rune(s)
	r[0] = unicode.ToLower(r[0])

	return string(r)
}

func toCamelCase(input string) string {
	parts := strings.Split(input, "_")
	for i := range parts {
		parts[i] = cases.Title(language.English).String(parts[i])
	}
	return strings.Join(parts, "")
}

func toSnakeCase(input string) string {
	var result []byte
	for i, c := range input {
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				result = append(result, '_')
			}
			c += 'a' - 'A'
		}
		result = append(result, byte(c))
	}
	return string(result)
}
