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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/czcorpus/vert-tagextract/v3/cnf"
	"github.com/czcorpus/vert-tagextract/v3/db"
	"github.com/czcorpus/vert-tagextract/v3/livetokens"
	"github.com/czcorpus/vert-tagextract/v3/ud"
	"github.com/rs/zerolog/log"
)

// frodoDBConf is a bit of a reverse dependency but for sake
// of the ltgen tool, it is ok. We need this to be able to
// load database configuration from Frodo's main configuration
// so we don't have to extract and save the config somewhere
// manually.
type frodoDBConf struct {
	LiveAttrs struct {
		DB db.Conf `json:"db"`
	} `json:"liveAttrs"`
}

func loadConfig(path, frodoConfPath string) (ltgConf, error) {

	var conf ltgConf
	// first, let's try vte conf:
	tmpConf, err := cnf.LoadConf(path)
	if err != nil {
		return conf, fmt.Errorf("failed to load config: %w", err)
	}
	conf.CorpusID = tmpConf.Corpus
	conf.Attrs = tmpConf.LiveTokens
	conf.DB = tmpConf.DB
	conf.VerticalPath = tmpConf.VerticalFile
	if conf.VerticalPath == "" && len(tmpConf.VerticalFiles) > 0 {
		conf.VerticalPath = tmpConf.VerticalFiles[0]
	}
	if len(conf.VerticalPath) > 1 {
		log.Warn().Msg("vte conf defines more than one vertical file - only the first will be used")
	}
	if conf.CorpusID == "" { // probably a bad/empty conf, let's try custom subconf
		tmpConf, err := LoadConf(path)
		if err != nil {
			return conf, fmt.Errorf("failed to load config: %w", err)
		}
		conf = tmpConf
	}
	if frodoConfPath != "" {
		data, err := os.ReadFile(frodoConfPath)
		if err != nil {
			return conf, fmt.Errorf("failed to load config: %w", err)
		}
		var fConf frodoDBConf
		if err := json.Unmarshal(data, &fConf); err != nil {
			return conf, fmt.Errorf("failed to load config: %w", err)
		}
		conf.DB = fConf.LiveAttrs.DB
	}
	log.Info().
		Str("hostname", conf.DB.Host).
		Str("user", conf.DB.User).
		Str("database", conf.DB.Name).
		Msg("configured database")
	return conf, nil
}

func runImport(args []string) {
	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
	frodoConf := importCmd.String("frodo-conf", "", "a path to frodo configuration (used for db credentials)")
	vertFile := importCmd.String("vert-file", "", "a custom path to vertical file (normally, it is defined in vte conf)")
	importCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s import [options] <config-file>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Import tokens from a vertical file into the database.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		importCmd.PrintDefaults()
	}
	importCmd.Parse(args)

	if importCmd.NArg() < 1 {
		importCmd.Usage()
		os.Exit(1)
	}

	conf, err := loadConfig(importCmd.Arg(0), *frodoConf)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to run")
		return
	}

	if *vertFile != "" {
		conf.VerticalPath = *vertFile
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := livetokens.OpenDB(conf.DB)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to run")
		return
	}

	if err := livetokens.CreateTable(ctx, db, conf.CorpusID, conf.Attrs); err != nil {
		log.Fatal().Err(err).Msg("failed to run")
		return
	}

	if err := ParseFileUD(ctx, conf, db); err != nil {
		log.Fatal().Err(err).Msg("failed to run")
		return
	}
}

func runSearch(args []string) {
	searchCmd := flag.NewFlagSet("search", flag.ExitOnError)
	attrFilter := searchCmd.String("attr", "", "Attribute filters in format: name=value,name2=value2")
	featFilter := searchCmd.String("feat", "", "UD feature filters in format: feat=value,feat2=value2")
	searchCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s search [options] <config-file>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Search for tokens matching the specified filters.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		searchCmd.PrintDefaults()
	}
	searchCmd.Parse(args)

	if searchCmd.NArg() < 1 {
		searchCmd.Usage()
		os.Exit(1)
	}

	conf, err := LoadConf(searchCmd.Arg(0))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := livetokens.OpenDB(conf.DB)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open database")
	}
	defer db.Close()

	// Parse attribute filters
	var attrFilters []livetokens.AttrAndVal
	if *attrFilter != "" {
		for _, pair := range strings.Split(*attrFilter, ",") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				log.Fatal().Msgf("invalid attr filter format: %s", pair)
			}
			attrFilters = append(attrFilters, livetokens.AttrAndVal{
				Name:  parts[0],
				Value: parts[1],
			})
		}
	}

	// Parse UD feature filters
	var featFilters []ud.Feat
	if *featFilter != "" {
		for _, pair := range strings.Split(*featFilter, ",") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				log.Fatal().Msgf("invalid feat filter format: %s", pair)
			}
			featFilters = append(featFilters, ud.Feat{parts[0], parts[1]})
		}
	}

	searcher := &livetokens.Searcher{
		Attrs: conf.Attrs,
		DB:    db,
	}

	results, err := searcher.FilterTokens(ctx, conf.CorpusID, attrFilters, featFilters)
	if err != nil {
		log.Fatal().Err(err).Msg("search failed")
	}

	// Output results as JSON
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		log.Fatal().Err(err).Msg("failed to encode results")
	}
}

func runValues(args []string) {
	valuesCmd := flag.NewFlagSet("values", flag.ExitOnError)
	attrFilter := valuesCmd.String("attr", "", "Attribute filters in format: name=value,name2=value2")
	featFilter := valuesCmd.String("feat", "", "UD feature filters in format: feat=value,feat2=value2")
	valuesCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s values [options] <config-file>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Get all available values for attributes and UD features given current filters.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		valuesCmd.PrintDefaults()
	}
	valuesCmd.Parse(args)

	if valuesCmd.NArg() < 1 {
		valuesCmd.Usage()
		os.Exit(1)
	}

	conf, err := LoadConf(valuesCmd.Arg(0))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := livetokens.OpenDB(conf.DB)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open database")
	}
	defer db.Close()

	// Parse attribute filters
	var attrFilters []livetokens.AttrAndVal
	if *attrFilter != "" {
		for _, pair := range strings.Split(*attrFilter, ",") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				log.Fatal().Msgf("invalid attr filter format: %s", pair)
			}
			attrFilters = append(attrFilters, livetokens.AttrAndVal{
				Name:   parts[0],
				Values: []string{parts[1]},
			})
		}
	}

	// Parse UD feature filters
	var featFilters []livetokens.AttrAndVal
	if *featFilter != "" {
		for _, pair := range strings.Split(*featFilter, ",") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				log.Fatal().Msgf("invalid feat filter format: %s", pair)
			}
			featFilters = append(featFilters, livetokens.AttrAndVal{
				Name:   parts[0],
				Values: []string{parts[1]},
			})
		}
	}

	searcher := &livetokens.Searcher{
		Attrs: conf.Attrs,
		DB:    db,
	}

	results, err := searcher.GetAvailableValues(ctx, conf.CorpusID, attrFilters, featFilters)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get available values")
	}

	// Output results as JSON
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		log.Fatal().Err(err).Msg("failed to encode results")
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <command> [options]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  import    Import tokens from a vertical file into the database\n")
	fmt.Fprintf(os.Stderr, "  search    Search for tokens matching specified filters\n")
	fmt.Fprintf(os.Stderr, "  values    Get available values for attributes and features given filters\n")
	fmt.Fprintf(os.Stderr, "\nRun '%s <command> -h' for more information about a command.\n", os.Args[0])
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "import":
		runImport(os.Args[2:])
	case "search":
		runSearch(os.Args[2:])
	case "values":
		runValues(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}
