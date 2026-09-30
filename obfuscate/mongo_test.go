package obfuscate

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func TestMongoQueryShapeMasksDollarValues(t *testing.T) {
	got := MongoQueryShape(bson.D{
		{Key: "find", Value: "users"},
		{Key: "filter", Value: bson.D{
			{Key: "pwhash", Value: "$2b$10$N9qo8uLOickgx2ZMRZoMye"},
			{Key: "argon", Value: "$argon2id$v=19$m=65536"},
			{Key: "email", Value: "alice@example.com"},
		}},
	})
	assert.Equal(t, "find({pwhash: ?, argon: ?, email: ?})", got)
	assert.NotContains(t, got, "N9qo8")

	got = MongoQueryShape(bson.D{
		{Key: "aggregate", Value: "orders"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$project", Value: bson.D{{Key: "t", Value: "$total"}, {Key: "m", Value: "$limits.max"}, {Key: "s", Value: "$secret value"}}}},
			bson.D{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: "$$ROOT"}}}},
		}},
	})
	assert.Equal(t, "aggregate([{$project: {t: $total, m: $limits.max, s: ?}}, {$replaceRoot: {newRoot: $$ROOT}}])", got)
}

func TestMongoQueryShapeTruncatesOnRuneBoundary(t *testing.T) {
	projection := bson.D{}
	for i := 0; i < 406; i++ {
		projection = append(projection, bson.E{Key: strings.Repeat("я", i%7+3) + fmt.Sprint(i), Value: 1})
	}
	for _, extra := range []int{0, 1, 2, 3} {
		p := append(bson.D{{Key: strings.Repeat("a", extra), Value: 1}}, projection...)
		s := MongoQueryShape(bson.D{{Key: "find", Value: "c"}, {Key: "filter", Value: bson.D{}}, {Key: "projection", Value: p}})
		assert.LessOrEqual(t, len(s), mongoMaxLen)
		assert.True(t, utf8.ValidString(s), "invalid UTF-8 with offset %d", extra)
	}
	assert.Equal(t, "a", truncateUTF8("aя", 2))
	assert.Equal(t, "aя", truncateUTF8("aя", 3))
}
