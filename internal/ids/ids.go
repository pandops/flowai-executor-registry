// Package ids validates FlowAI integer identifiers (design rule 43).
package ids

import (
	"regexp"
	"strconv"
)

var decimal = regexp.MustCompile(`^[1-9][0-9]*$`)

// Parse accepts only a positive decimal integer within int64 range without
// sign or leading zeros.
func Parse(s string) (int64, bool) {
	if !decimal.MatchString(s) {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
