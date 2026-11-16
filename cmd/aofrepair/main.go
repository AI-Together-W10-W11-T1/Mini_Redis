package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("aofrepair", flag.ContinueOnError)
	input := flags.String("input", "data/appendonly.aof", "source AOF; server must be stopped")
	output := flags.String("output", "", "new recovered file; never overwrites source or existing file")
	allow := flags.Bool("allow-data-loss", false, "acknowledge discarding all bytes after last valid record")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	var report store.AOFInspection
	var err error
	if *output == "" {
		report, err = store.InspectAOF(*input)
	} else {
		report, err = store.RecoverAOF(*input, *output, *allow)
	}
	if err != nil {
		return err
	}
	if err = json.NewEncoder(out).Encode(report); err != nil {
		return err
	}
	if *output == "" && report.Problem != "" {
		return fmt.Errorf("AOF damage found; original preserved")
	}
	return nil
}
