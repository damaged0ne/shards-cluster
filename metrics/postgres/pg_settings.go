package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/coroot/coroot-cluster-agent/metrics/dbtracker"
	"github.com/coroot/coroot-cluster-agent/schema"
	"github.com/pmezard/go-difflib/difflib"
)

type Setting struct {
	Name     string
	Unit     string
	Value    float64
	RawValue string
	Source   string // default, configuration file, command line, environment variable, global, override, session, client, etc.
	Context  string // internal, postmaster, sighup, superuser-backend, backend, superuser, user
	IsMetric bool   // true for integer, real, bool vartypes
}

func (c *Collector) getSettings(ctx context.Context) ([]Setting, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT name, setting, unit, vartype, source, context FROM pg_settings ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Setting
	for rows.Next() {
		var name, value, unit, vartype, source, context sql.NullString
		if err := rows.Scan(&name, &value, &unit, &vartype, &source, &context); err != nil {
			c.logger.Warning("failed to scan pg_settings row:", err)
			continue
		}
		s := Setting{
			Name:     name.String,
			Unit:     unit.String,
			RawValue: value.String,
			Source:   source.String,
			Context:  context.String,
		}
		switch vartype.String {
		case "integer", "real":
			v, err := strconv.ParseFloat(value.String, 64)
			if err != nil {
				c.logger.Warningf("failed to parse value for %s=%s setting: %s", name.String, value.String, err)
				continue
			}
			s.Value = v
			s.IsMetric = true
		case "bool":
			if value.String == "on" {
				s.Value = 1
			}
			s.IsMetric = true
		}
		res = append(res, s)
	}
	return res, nil
}

// conninfoPasswordRe matches password-like keywords (password, sslpassword, ...) in a
// keyword/value connection string together with their value (quoted or bare).
var conninfoPasswordRe = regexp.MustCompile(`(?i)(\b\w*password\s*=\s*)('(?:[^'\\]|\\.)*'?|\S*)`)

// redactConninfo strips passwords from a libpq connection string (both the
// keyword/value and the URI forms), keeping the remaining keys intact.
func redactConninfo(ci string) string {
	if strings.HasPrefix(ci, "postgres://") || strings.HasPrefix(ci, "postgresql://") {
		u, err := url.Parse(ci)
		if err != nil {
			return dbtracker.RedactedValue
		}
		if u.User != nil {
			if _, ok := u.User.Password(); ok {
				u.User = url.UserPassword(u.User.Username(), "redacted")
			}
		}
		q := u.Query()
		changed := false
		for k := range q {
			if strings.Contains(strings.ToLower(k), "password") {
				q.Set(k, "redacted")
				changed = true
			}
		}
		if changed {
			u.RawQuery = q.Encode()
		}
		return u.String()
	}
	return conninfoPasswordRe.ReplaceAllString(ci, "${1}"+dbtracker.RedactedValue)
}

// redactSetting returns a value of the setting safe to be shipped outside the agent.
func redactSetting(name, value string) string {
	if value == "" {
		return value
	}
	n := strings.ToLower(name)
	switch {
	case strings.Contains(n, "conninfo"): // primary_conninfo
		return redactConninfo(value)
	case strings.HasSuffix(n, "_command"): // archive_command, restore_command, archive_cleanup_command, ssl_passphrase_command, ...
		return dbtracker.RedactedValue
	case dbtracker.IsSensitiveSettingName(n):
		return dbtracker.RedactedValue
	}
	return value
}

func settingsToText(settings []Setting) string {
	var buf strings.Builder
	for _, s := range settings {
		switch s.Source {
		case "override", "session", "client":
			continue
		}
		if s.Context == "internal" {
			continue
		}
		fmt.Fprintf(&buf, "%s = %s\n", s.Name, redactSetting(s.Name, s.RawValue))
	}
	return buf.String()
}

func (c *Collector) trackSettingsChanges(settings []Setting) {
	curr := settingsToText(settings)
	if c.prevSettingsText != "" && curr != c.prevSettingsText {
		diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(c.prevSettingsText),
			B:        difflib.SplitLines(curr),
			FromFile: "pg_settings",
			ToFile:   "pg_settings",
			Context:  3,
		})
		c.emitter.Emit(schema.Change{
			Object: "pg_settings",
			Type:   schema.ChangeTypeChanged,
			Diff:   diff,
		}, "postgresql", c.targetAddr)
	}
	c.prevSettingsText = curr
}
