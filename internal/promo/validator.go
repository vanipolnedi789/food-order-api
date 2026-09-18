package promo

import (
	"context"
	"strings"
	"unicode"
)

const (
	minCodeLength = 8
	maxCodeLength = 10
	minFileHits   = 2
)

// Checker is the business-level coupon API consumed by the discount engine.
// Storage implementations remain hidden behind Membership.
type Checker interface {
	Validate(ctx context.Context, code string) bool
}

// Membership reports possible membership in one coupon dataset.
//
// A true result is deliberately not called exact: probabilistic
// implementations such as Binary Fuse may return false positives. A false
// result, however, means the value is definitely absent.
type Membership interface {
	PossiblyContains(ctx context.Context, code string) bool
}

// Validator owns coupon format and cross-dataset policy. It intentionally
// knows nothing about maps, Binary Fuse filters, Redis, or files.
type Validator struct {
	datasets   []Membership
	minMatches int
}

// NewValidator constructs the current 2-of-3 coupon policy.
func NewValidator(datasets ...Membership) *Validator {
	return &Validator{datasets: append([]Membership(nil), datasets...), minMatches: minFileHits}
}

// Validate returns true when a well-formed code possibly appears in at least
// two independent datasets. Optional exact confirmation belongs in the
// Membership implementation, below this business policy.
func (v *Validator) Validate(ctx context.Context, code string) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	code = strings.TrimSpace(code)
	if !validCode(code) {
		return false
	}
	matches := 0
	for _, dataset := range v.datasets {
		if dataset.PossiblyContains(ctx, code) {
			matches++
			if matches >= v.minMatches {
				return true
			}
		}
	}
	return false
}

// collectCodes extracts valid coupon tokens from a line without retaining the
// line or building an in-memory set. Duplicate removal is the builder's job.
func collectCodes(line string, yield func(string) error) error {
	start := -1
	flush := func(end int) error {
		if start < 0 {
			return nil
		}
		token := line[start:end]
		start = -1
		if validLength(token) {
			return yield(token)
		}
		return nil
	}
	for i, r := range line {
		if isCodeRune(r) {
			if start < 0 {
				start = i
			}
			continue
		}
		if err := flush(i); err != nil {
			return err
		}
	}
	return flush(len(line))
}

// isCodeRune - returns true when the rune is a letter or digit.
func isCodeRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// validLength - returns true when the code length is between 8 and 10 characters.
func validLength(code string) bool {
	n := len(code)
	return n >= minCodeLength && n <= maxCodeLength
}

// validCode enforces the same token rules used by the offline builder.
func validCode(code string) bool {
	if !validLength(code) {
		return false
	}
	for _, r := range code {
		if !isCodeRune(r) {
			return false
		}
	}
	return true
}
