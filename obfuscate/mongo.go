package obfuscate

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	mongoMaxLen   = 4096
	mongoMaxDepth = 10
)

var mongoSkipKeys = map[string]bool{
	"lsid": true, "$clusterTime": true, "$db": true, "$readPreference": true,
	"readConcern": true, "writeConcern": true, "maxTimeMS": true, "cursor": true,
	"comment": true, "apiVersion": true, "apiStrict": true, "$audit": true,
	"txnNumber": true, "autocommit": true, "startTransaction": true, "stmtId": true,
	"stmtIds": true, "let": true, "clientOperationKey": true, "needsMerge": true,
	"fromRouter": true, "fromMongos": true, "shardVersion": true, "databaseVersion": true,
	"clusterTime": true, "signature": true, "keyId": true, "mayBypassWriteBlocking": true,
	"cmdNs": true, "command": true, "collectionType": true, "client": true, "batchSize": true,
}

var mongoUnmaskedKeys = map[string]bool{
	"sort": true, "projection": true, "hint": true, "collation": true,
	"key": true, "showRecordId": true, "singleBatch": true, "tailable": true,
	"allowDiskUse": true, "upsert": true, "multi": true, "ordered": true, "new": true, "remove": true,
}

func MongoQueryShape(shape bson.D) string {
	command := ""
	var rest bson.D
	for i, e := range shape {
		if e.Key == "command" {
			if s, ok := e.Value.(string); ok {
				command = s
			}
			continue
		}
		if i == 0 && command == "" && !mongoSkipKeys[e.Key] && !strings.HasPrefix(e.Key, "$") {
			command = e.Key
			continue
		}
		rest = append(rest, e)
	}
	if command == "" {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(command)

	if primary := mongoPrimaryArg(command); primary != "" {
		for _, e := range rest {
			if e.Key == primary {
				sb.WriteByte('(')
				mongoWriteValue(&sb, e.Value, !mongoUnmaskedKeys[primary], 0)
				sb.WriteByte(')')
				break
			}
		}
	}
	for _, e := range rest {
		if sb.Len() > mongoMaxLen {
			break
		}
		if mongoSkipKeys[e.Key] || e.Key == mongoPrimaryArg(command) {
			continue
		}
		sb.WriteByte(' ')
		sb.WriteString(e.Key)
		sb.WriteByte(':')
		mongoWriteValue(&sb, e.Value, !mongoUnmaskedKeys[e.Key], 0)
	}

	return truncateUTF8(strings.ToValidUTF8(sb.String(), "?"), mongoMaxLen)
}

// truncateUTF8 cuts s to at most n bytes without splitting a multi-byte character.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// reMongoFieldPath matches field paths ("$field.sub"), aggregation variables ("$$ROOT") and operators ("$gt").
var reMongoFieldPath = regexp.MustCompile(`^\$\$?[A-Za-z_][\w.]*$`)

func mongoPrimaryArg(command string) string {
	switch command {
	case "find", "count":
		return "filter"
	case "aggregate":
		return "pipeline"
	case "distinct", "findAndModify", "findandmodify":
		return "query"
	case "update":
		return "updates"
	case "delete":
		return "deletes"
	case "insert":
		return "documents"
	}
	return ""
}

func mongoWriteValue(sb *strings.Builder, v any, mask bool, depth int) {
	if sb.Len() > mongoMaxLen || depth > mongoMaxDepth {
		sb.WriteString("...")
		return
	}
	switch val := v.(type) {
	case primitive.D:
		sb.WriteByte('{')
		for i, e := range val {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(e.Key)
			sb.WriteString(": ")
			mongoWriteValue(sb, e.Value, mask && !mongoUnmaskedKeys[e.Key], depth+1)
			if sb.Len() > mongoMaxLen {
				break
			}
		}
		sb.WriteByte('}')
	case bson.M:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(k)
			sb.WriteString(": ")
			mongoWriteValue(sb, val[k], mask && !mongoUnmaskedKeys[k], depth+1)
			if sb.Len() > mongoMaxLen {
				break
			}
		}
		sb.WriteByte('}')
	case primitive.A:
		if mask && mongoAllScalars(val) {
			sb.WriteString("[?]")
			return
		}
		sb.WriteByte('[')
		for i, e := range val {
			if i > 0 {
				sb.WriteString(", ")
			}
			mongoWriteValue(sb, e, mask, depth+1)
			if sb.Len() > mongoMaxLen {
				break
			}
		}
		sb.WriteByte(']')
	case string:
		switch {
		case strings.HasPrefix(val, "?"): // $queryStats placeholder like "?number"
			sb.WriteByte('?')
		case reMongoFieldPath.MatchString(val): // field reference, variable or operator
			sb.WriteString(val)
		case !mask:
			sb.WriteString(val)
		default:
			sb.WriteByte('?')
		}
	case nil:
		sb.WriteByte('?')
	default:
		if mask {
			sb.WriteByte('?')
		} else {
			fmt.Fprintf(sb, "%v", val)
		}
	}
}

func mongoAllScalars(a primitive.A) bool {
	for _, e := range a {
		switch e.(type) {
		case primitive.D, bson.M, primitive.A:
			return false
		}
	}
	return true
}
