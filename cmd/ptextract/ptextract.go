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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
)

func main() {
	col := flag.Int(
		"col", -1,
		"vertical file column index containing the tagset value "+
			"(0 = word, 1 = first positional attribute after the word, ...)",
	)
	registry := flag.String(
		"registry", "",
		"path to a corpus registry file; if set, the column index of the "+
			"\"tag\" attribute is auto-detected from it and -col must not be set",
	)
	output := flag.String(
		"output", "", "path to an output file (if not set, output goes to stdout)")
	encoding := flag.String("encoding", "utf-8", "input vertical file encoding")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <vertical-file>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Extract unique positional tagset values from a corpus vertical file.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}
	if *col >= 0 && *registry != "" {
		fmt.Fprintln(os.Stderr, "-col and -registry cannot be used together")
		os.Exit(1)
	}

	var tagColIdx int
	if *registry != "" {
		idx, err := FindAttrColumn(*registry, "tag")
		if err != nil {
			log.Fatal().Err(err).Msg("failed to auto-detect tag column from registry")
		}
		log.Info().Int("col", idx).Msg("auto-detected tag column from registry")
		tagColIdx = idx

	} else if *col >= 0 {
		tagColIdx = *col

	} else {
		fmt.Fprintln(os.Stderr, "either -col or -registry must be set")
		os.Exit(1)
	}
	vertPath := flag.Arg(0)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	proc, err := ParseFile(ctx, vertPath, tagColIdx, *encoding)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to process vertical file")
	}

	out := os.Stdout
	if *output != "" {
		f, err := os.Create(*output)
		if err != nil {
			log.Fatal().Err(err).Msg("failed to create output file")
		}
		defer f.Close()
		out = f
	}

	if err := proc.WriteValues(out); err != nil {
		log.Fatal().Err(err).Msg("failed to write output")
	}
}
