package tcp

import (
	"regexp"
	"strings"
)

const (
	InsertType = 0
	SelectType = 1
	OtherType  = 2
)

var selectRe = regexp.MustCompile(`\s+SELECT\s+`)

func getQueryType(query string) (int, error) {
	if isInsertType(query) {
		return InsertType, nil
	}
	if isSelectType(query) {
		return SelectType, nil
	}
	return OtherType, nil
}
func isInsertType(query string) bool {
	if f := strings.Fields(query); len(f) > 2 {
		return strings.EqualFold("INSERT", f[0]) && strings.EqualFold("INTO", f[1]) && !selectRe.MatchString(strings.ToUpper(query))
	}
	return false
}
func isSelectType(query string) bool {
	return selectRe.MatchString(strings.ToUpper(query))
}
