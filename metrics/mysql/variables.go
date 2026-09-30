package mysql

import "context"

func (c *Collector) queryVariables(ctx context.Context, query string) (map[string]string, error) {
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	dest := map[string]string{}
	defer rows.Close()
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			c.logger.Warning(err)
			continue
		}
		dest[name] = value
	}
	return dest, rows.Err()
}
