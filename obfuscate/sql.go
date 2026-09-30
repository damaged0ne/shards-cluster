package obfuscate

import (
	"bytes"
	"regexp"
	"strings"
	"unicode"
)

var (
	rePlaceholder = regexp.MustCompile(`\$\d+`)
	reHexNumber   = regexp.MustCompile(`\b0(?:x[0-9a-f]+|b[01]+)\b`)
	reNumber      = regexp.MustCompile(`[+-]?(?:\d+\.\d+|\d+\.|\.\d+|\d+)(?:e[+-]?\d+)?`)
	reWhitespace  = regexp.MustCompile(`\s+`)
	reTypecast    = regexp.MustCompile(`\s*::\s*"?\w+"?(?:\(\s*\d*\s*\))?(?:\[\s*\])?`)
	reOperator    = regexp.MustCompile(`([!#$%&*+\-/:<=>@^~|]+)`)
	rePunctuation = regexp.MustCompile(`([(),;[\]{}])`)
	reBoolean     = regexp.MustCompile(`(\W)(:?true|false|null)(\W|$)`)
	reValues      = regexp.MustCompile(`(values?)\s*(?:\(\s*\?\s*\)\s*,?\s*)+`)
)

// Dialect selects the lexical rules used to find string literals and comments.
type Dialect int

const (
	// DialectPostgres: "..." is a quoted identifier (kept), backslashes are literal in '...'
	// (except E'...' strings), $$...$$ and $tag$...$tag$ are string constants.
	DialectPostgres Dialect = iota
	// DialectMySQL: "..." is a string literal (masked, unless ANSI_QUOTES is enabled), backslash
	// escapes are honored in '...' and "..." strings, and '#' starts a single-line comment.
	DialectMySQL
)

// Sql obfuscates a query using the PostgreSQL lexical rules. It is kept for backward compatibility;
// use SqlWithDialect for queries of other databases.
func Sql(query string) string {
	return SqlWithDialect(query, DialectPostgres)
}

func SqlWithDialect(query string, dialect Dialect) string {
	if query == "" {
		return ""
	}
	query = removeCommentsAndStringsDialect(query, dialect)
	query = strings.ToLower(query)
	query = reWhitespace.ReplaceAllString(query, " ")
	query = rePlaceholder.ReplaceAllString(query, "?")
	query = reTypecast.ReplaceAllString(query, "")
	query = reHexNumber.ReplaceAllString(query, "?")
	query = reNumber.ReplaceAllString(query, "?")
	query = reBoolean.ReplaceAllString(query, "$1?$3")

	query = collapseLists(query)
	query = reValues.ReplaceAllString(query, "$1(?)")

	query = reOperator.ReplaceAllString(query, " $1 ")
	query = rePunctuation.ReplaceAllString(query, " $1 ")
	query = reWhitespace.ReplaceAllString(query, " ")
	query = strings.ReplaceAll(query, " ,", ",")
	query = strings.TrimLeft(query, " ")
	query = strings.TrimRight(query, "; ")
	return query
}

func removeCommentsAndStrings(query string) string {
	return removeCommentsAndStringsDialect(query, DialectPostgres)
}

func isIdentRune(r rune) bool {
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// skipQuoted returns the index of the closing quote of a literal whose opening quote is at i-1.
// A doubled quote is an escaped quote; if backslash is true, a backslash escapes the next character.
// Returns q.len for an unterminated (e.g. truncated) literal.
func skipQuoted(q *runes, i int, quote rune, backslash bool) int {
	for ; i < q.len; i++ {
		switch q.get(i) {
		case '\\':
			if backslash {
				i++
			}
		case quote:
			if q.get(i+1) == quote {
				i++
				continue
			}
			return i
		}
	}
	return q.len
}

// dollarTag returns the length of a PostgreSQL dollar-quote delimiter ($$ or $tag$) starting at i, or 0.
func dollarTag(q *runes, i int) int {
	if q.get(i) != '$' {
		return 0
	}
	j := i + 1
	for ; j < q.len; j++ {
		r := q.get(j)
		if r == '$' {
			return j - i + 1
		}
		if !(r == '_' || unicode.IsLetter(r) || (j > i+1 && unicode.IsDigit(r))) {
			return 0
		}
	}
	return 0
}

func hasPrefixAt(q *runes, i int, prefix []rune) bool {
	if i+len(prefix) > q.len {
		return false
	}
	for k, r := range prefix {
		if q.data[i+k] != r {
			return false
		}
	}
	return true
}

func removeCommentsAndStringsDialect(query string, dialect Dialect) string {
	mysql := dialect == DialectMySQL
	q := newRunes(query)
	var prev, curr, next rune
	var res bytes.Buffer
	for i := 0; i < q.len; i++ {
		curr, next = q.get(i), q.get(i+1)
		if i > 0 {
			prev = q.get(i - 1)
		}
		lcurr := unicode.ToLower(curr)
		prefixed := next == '\'' && (i == 0 || !isIdentRune(prev))
		switch {
		case curr == '\'': // string constant
			i = skipQuoted(q, i+1, '\'', mysql)
			res.WriteRune('?')
		case curr == '"' && mysql: // string constant in MySQL (unless ANSI_QUOTES)
			i = skipQuoted(q, i+1, '"', true)
			res.WriteRune('?')
		case curr == '"': // quoted identifier: keep as is, but don't treat quotes inside as string delimiters
			j := i + 1
			for ; j < q.len; j++ {
				if q.get(j) == '"' {
					if q.get(j+1) == '"' {
						j++
						continue
					}
					break
				}
			}
			if j >= q.len {
				j = q.len - 1
			}
			for k := i; k <= j; k++ {
				res.WriteRune(q.get(k))
			}
			i = j
		case curr == '`' && mysql: // quoted identifier
			j := i + 1
			for ; j < q.len && q.get(j) != '`'; j++ {
			}
			if j >= q.len {
				j = q.len - 1
			}
			for k := i; k <= j; k++ {
				res.WriteRune(q.get(k))
			}
			i = j
		case lcurr == 'e' && prefixed && !mysql: // postgres C-style escaped string
			i = skipQuoted(q, i+2, '\'', true)
			res.WriteRune('?')
		case (lcurr == 'b' || lcurr == 'x') && prefixed: // bit / hex string
			i += 2
			for ; i < q.len; i++ {
				if q.get(i) == '\'' {
					break
				}
			}
			res.WriteRune('?')
		case curr == '$' && !mysql && (i == 0 || !isIdentRune(prev)) && dollarTag(q, i) > 0: // postgres dollar-quoted string
			n := dollarTag(q, i)
			tag := q.data[i : i+n]
			i += n
			for ; i < q.len; i++ {
				if hasPrefixAt(q, i, tag) {
					i += n - 1
					break
				}
			}
			res.WriteRune('?')
		case curr == '-' && next == '-', curr == '#' && mysql: // single-line comment
			if curr == '-' {
				i++
			}
			i++
			for ; i < q.len; i++ {
				if q.get(i) == '\n' {
					res.WriteRune('\n')
					break
				}
			}
		case curr == '/' && next == '*': // multi-line comment
			i += 2
			for ; i < q.len; i++ {
				if q.get(i) == '*' && q.get(i+1) == '/' {
					i++
					break
				}
			}
		default:
			res.WriteRune(curr)
		}
	}
	return res.String()
}

func collapseLists(query string) string {
	q := newRunes(query)
	var res bytes.Buffer
	for i := 0; i < q.len; i++ {
		curr := q.get(i)
		switch curr {
		case '(':
			j := i + 1
			for level := 1; j < q.len && level > 0; j++ {
				switch q.get(j) {
				case '(':
					level++
					continue
				case ')':
					level--
					continue
				case '?', ' ', ',':
					continue
				default:
					goto OUT
				}
			}
			res.WriteString("(?)")
			i = j - 1
			continue
		case '[':
			j := i + 1
			for level := 1; j < q.len && level > 0; j++ {
				switch q.get(j) {
				case '[':
					level++
					continue
				case ']':
					level--
					continue
				case '?', ' ', ',':
					continue
				default:
					goto OUT
				}
			}
			res.WriteString("[?]")
			i = j - 1
			continue
		}
	OUT:
		res.WriteRune(curr)
	}
	return res.String()
}
