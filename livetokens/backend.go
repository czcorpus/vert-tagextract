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
	"time"

	"github.com/czcorpus/vert-tagextract/v3/db"
	"github.com/go-sql-driver/mysql"
)

var tableTpl = `
CREATE TABLE %s_livetokens (
	id INT NOT NULL PRIMARY KEY auto_increment,
	cnt INT NOT NULL DEFAULT 0,
	%s
)
`

var UDFeatsTableTpl = `
CREATE TABLE %s_livetokens_udfeats (
	id INT NOT NULL PRIMARY KEY auto_increment,
	token_id INT NOT NULL REFERENCES %s_livetokens(id),
	feat VARCHAR(100),
	value VARCHAR(100)
)
`

func generateAttrEntrySQL(attrName string) string {
	return fmt.Sprintf("%s VARCHAR(100) NOT NULL", attrName)
}

func CreateTable(ctx context.Context, db *sql.DB, corpusID string, attrs []Attr) error {
	var hasUDFeats bool
	cols := make([]string, 0, len(attrs)+1)
	for _, attr := range attrs {
		if attr.IsUDFeats {
			hasUDFeats = true

		} else {
			cols = append(cols, generateAttrEntrySQL(attr.Name))
		}
	}

	if hasUDFeats {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s_livetokens_udfeats", corpusID)); err != nil {
			return fmt.Errorf("failed to create _livetokens_udfeats table: %w", err)
		}
	}

	_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s_livetokens", corpusID))
	if err != nil {
		return fmt.Errorf("failed to create livetokens table: %w", err)
	}
	sql := fmt.Sprintf(tableTpl, corpusID, strings.Join(cols, ", "))
	_, err = db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create livetokens table: %w", err)
	}

	if hasUDFeats {
		fmt.Println("SQL: ", fmt.Sprintf(UDFeatsTableTpl, corpusID, corpusID))
		if _, err := db.ExecContext(ctx, fmt.Sprintf(UDFeatsTableTpl, corpusID, corpusID)); err != nil {
			return fmt.Errorf("failed to create _livetokens_udfeats table: %w", err)
		}
	}

	return nil
}

func InsertFeats(ctx context.Context, tx *sql.Tx, corpus string, values [][]any) error {
	if len(values) == 0 {
		return nil
	}
	flatValues := make([]any, 0, len(values)*len(values[0]))
	groupedPlaceholders := make([]string, len(values))
	for i, v := range values {
		flatValues = append(flatValues, v...)
		groupedPlaceholders[i] = "(?, ?, ?)"
	}
	_, err := tx.ExecContext(
		ctx,
		fmt.Sprintf(
			"INSERT INTO %s_livetokens_udfeats (feat, value, token_id) VALUES %s",
			corpus,
			strings.Join(groupedPlaceholders, ", "),
		),
		flatValues...,
	)
	if err != nil {
		return fmt.Errorf("failed to insert UD feats: %w", err)
	}
	return nil
}

func InsertTokens(ctx context.Context, tx *sql.Tx, corpus string, attrs AttrList, values [][]any) ([2]int64, error) {
	flatValues := make([]any, 0, len(values)*len(values[0]))
	groupedPlaceholders := make([]string, len(values))
	for i, v := range values {
		flatValues = append(flatValues, v...)
		groupedPlaceholders[i] = "(?" + strings.Repeat(", ?", attrs.LenWithoutUDFeats()) + ")"
	}
	res, err := tx.ExecContext(
		ctx,
		fmt.Sprintf(
			"INSERT INTO %s_livetokens (%s, cnt) VALUES %s",
			corpus,
			attrs.WithoutUDFeatsAsCommaDelimited(),
			strings.Join(groupedPlaceholders, ", "),
		),
		flatValues...,
	)
	if err != nil {
		return [2]int64{}, fmt.Errorf("failed to insert livetokens: %w", err)
	}
	insID, err := res.LastInsertId()
	if err != nil {
		return [2]int64{}, fmt.Errorf("failed to insert livetokens: %w", err)
	}
	return [2]int64{insID, insID + int64(len(values)-1)}, nil
}

func OpenDB(conf db.Conf) (*sql.DB, error) {
	mconf := mysql.NewConfig()
	mconf.Net = "tcp"
	mconf.Addr = conf.Host
	mconf.User = conf.User
	mconf.Passwd = conf.Password
	mconf.DBName = conf.Name
	mconf.ParseTime = true
	mconf.Loc = time.Local
	db, err := sql.Open("mysql", mconf.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open liveattrs db: %w", err)
	}
	return db, nil
}
