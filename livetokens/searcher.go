// Copyright 2026 Tomas Machalek <tomas.machalek@gmail.com>
// Copyright 2026 Charles University, Faculty of Arts,
//                Department of Linguistics
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package livetokens

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/czcorpus/vert-tagextract/v3/ud"
)

type SearchMatch struct {
	Attrs   map[string]string `json:"attrs"`
	UDFeats map[string]string `json:"udFeats"`
	Count   int               `json:"count"`
}

type SearchResult struct {
	Total   int           `json:"total"`
	Matches []SearchMatch `json:"matches"`
}

// AvailableValues holds all possible values for each attribute and UD feature
// given the current filter constraints.
type AvailableValues struct {
	Attrs   map[string][]string `json:"attrs"`
	UDFeats map[string][]string `json:"udFeats"`
}

type Searcher struct {
	Attrs AttrList
	DB    *sql.DB
}

func (s *Searcher) createUDFeatCondition(corpus string, feat ud.Feat) (string, []any) {
	sql := " AND EXISTS ( " +
		"SELECT 1 " +
		fmt.Sprintf("FROM %s_livetokens_udfeats f2 ", corpus) +
		"WHERE f2.token_id = t.id " +
		"AND f2.feat = ? AND f2.value = ? " +
		") "
	return sql, []any{feat[0], feat[1]}
}

func (s *Searcher) FilterTokens(ctx context.Context, corpus string, attrFilter []AttrAndVal, featFilter []ud.Feat) (SearchResult, error) {
	values := make([]any, 0, len(featFilter)*2+len(attrFilter))

	attrSQL := make([]string, 0, len(attrFilter))
	for _, f := range attrFilter {
		attrSQL = append(attrSQL, fmt.Sprintf("%s = ?", f.Name))
		values = append(values, f.Value)
	}

	var featFilterSQL strings.Builder
	for _, ff := range featFilter {
		s, v := s.createUDFeatCondition(corpus, ff)
		featFilterSQL.WriteString(s)
		values = append(values, v...)
	}
	sqlq := fmt.Sprintf("SELECT t.id, t.cnt, %s, ", s.Attrs.WithoutUDFeatsAsCommaDelimited()) +
		"GROUP_CONCAT( " +
		"CONCAT(f.feat, '=', f.value) " +
		"ORDER BY f.feat " +
		"SEPARATOR '|' " +
		") AS udfeats " +
		fmt.Sprintf("FROM %s_livetokens AS t ", corpus) +
		fmt.Sprintf("JOIN %s_livetokens_udfeats AS f ON f.token_id = t.id ", corpus) +
		"WHERE " +
		fmt.Sprintf("%s ", strings.Join(attrSQL, ", ")) +
		fmt.Sprintf("%s ", featFilterSQL.String()) +
		"GROUP BY t.id "
	rows, err := s.DB.QueryContext(ctx, sqlq, values...)
	if err != nil {
		return SearchResult{}, fmt.Errorf("failed to filter livetokens: %w", err)
	}
	defer rows.Close()

	// Build list of non-UDFeats attribute names for mapping scanned values
	attrNames := make([]string, 0, s.Attrs.LenWithoutUDFeats())
	for _, attr := range s.Attrs {
		if !attr.IsUDFeats {
			attrNames = append(attrNames, attr.Name)
		}
	}

	ans := make([]SearchMatch, 0, 100)
	for rows.Next() {
		var id int64
		var cnt int
		var udfeatsStr sql.NullString

		// Create scan destinations for dynamic attributes
		attrValues := make([]sql.NullString, len(attrNames))
		scanDest := make([]any, 0, 3+len(attrNames))
		scanDest = append(scanDest, &id, &cnt)
		for i := range attrValues {
			scanDest = append(scanDest, &attrValues[i])
		}
		scanDest = append(scanDest, &udfeatsStr)

		if err := rows.Scan(scanDest...); err != nil {
			return SearchResult{}, fmt.Errorf("failed to scan livetokens row: %w", err)
		}

		item := SearchMatch{
			Attrs:   make(map[string]string, len(attrNames)),
			UDFeats: make(map[string]string),
			Count:   cnt,
		}

		// Populate Attrs map
		for i, name := range attrNames {
			if attrValues[i].Valid {
				item.Attrs[name] = attrValues[i].String
			}
		}

		// Parse UDFeats from concatenated string (format: "feat1=val1|feat2=val2")
		if udfeatsStr.Valid && udfeatsStr.String != "" {
			for _, pair := range strings.Split(udfeatsStr.String, "|") {
				parts := strings.SplitN(pair, "=", 2)
				if len(parts) == 2 {
					item.UDFeats[parts[0]] = parts[1]
				}
			}
		}
		ans = append(ans, item)
	}

	if err := rows.Err(); err != nil {
		return SearchResult{}, fmt.Errorf("error iterating livetokens rows: %w", err)
	}

	return SearchResult{Total: len(ans), Matches: ans}, nil
}

// GetAvailableValues returns all possible values for each attribute and UD feature
// that are valid given the current filter constraints. This is useful for building
// interactive UIs where users progressively narrow down their search.
func (s *Searcher) GetAvailableValues(
	ctx context.Context,
	corpus string,
	attrFilter []AttrAndVal,
	featFilter []ud.Feat,
) (AvailableValues, error) {
	ans := AvailableValues{
		Attrs:   make(map[string][]string),
		UDFeats: make(map[string][]string),
	}

	// Build WHERE clause from filters
	values := make([]any, 0, len(featFilter)*2+len(attrFilter))
	whereClauses := make([]string, 0, len(attrFilter))
	for _, f := range attrFilter {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", f.Name))
		values = append(values, f.Value)
	}

	var featFilterSQL strings.Builder
	for _, ff := range featFilter {
		s, v := s.createUDFeatCondition(corpus, ff)
		featFilterSQL.WriteString(s)
		values = append(values, v...)
	}

	whereSQL := "1=1"
	if len(whereClauses) > 0 {
		whereSQL = strings.Join(whereClauses, " AND ")
	}

	// Query distinct values for each regular attribute
	for _, attr := range s.Attrs {
		if attr.IsUDFeats {
			continue
		}

		sqlq := fmt.Sprintf(
			"SELECT DISTINCT t.%s FROM %s_livetokens AS t WHERE %s %s ORDER BY t.%s",
			attr.Name, corpus, whereSQL, featFilterSQL.String(), attr.Name,
		)

		rows, err := s.DB.QueryContext(ctx, sqlq, values...)
		if err != nil {
			return ans, fmt.Errorf("failed to get available values for %s: %w", attr.Name, err)
		}

		var attrVals []string
		for rows.Next() {
			var val sql.NullString
			if err := rows.Scan(&val); err != nil {
				rows.Close()
				return ans, fmt.Errorf("failed to scan value for %s: %w", attr.Name, err)
			}
			if val.Valid {
				attrVals = append(attrVals, val.String)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return ans, fmt.Errorf("error iterating values for %s: %w", attr.Name, err)
		}
		ans.Attrs[attr.Name] = attrVals
	}

	// Query distinct UD feature name/value pairs
	sqlq := fmt.Sprintf(
		"SELECT DISTINCT f.feat, f.value "+
			"FROM %s_livetokens AS t "+
			"JOIN %s_livetokens_udfeats AS f ON f.token_id = t.id "+
			"WHERE %s %s "+
			"ORDER BY f.feat, f.value",
		corpus, corpus, whereSQL, featFilterSQL.String(),
	)

	rows, err := s.DB.QueryContext(ctx, sqlq, values...)
	if err != nil {
		return ans, fmt.Errorf("failed to get available UD features: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var feat, val string
		if err := rows.Scan(&feat, &val); err != nil {
			return ans, fmt.Errorf("failed to scan UD feature: %w", err)
		}
		ans.UDFeats[feat] = append(ans.UDFeats[feat], val)
	}
	if err := rows.Err(); err != nil {
		return ans, fmt.Errorf("error iterating UD features: %w", err)
	}

	return ans, nil
}
