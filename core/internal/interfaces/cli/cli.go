package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/interfaces/command"
)

func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	err := run(ctx, args, stdin, stdout, stderr)
	if err != nil {
		_ = json.NewEncoder(stderr).Encode(command.PublicFault(err))
	}
	return err
}
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return c.Err("invalid", "Usage: roster <command> --file input.json [--url http://127.0.0.1:7350]")
	}
	name := args[0]
	flags := flag.NewFlagSet("roster", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	file := flags.String("file", "", "JSON input file, or - for stdin")
	base := flags.String("url", "http://127.0.0.1:7350", "Server URL")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 {
		return c.Err("invalid", "Invalid command arguments")
	}
	u, err := url.Parse(*base)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return c.Err("invalid", "Server URL must be a local HTTP URL")
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return c.Err("invalid", "Server URL must use loopback")
	}
	var body io.Reader
	method := http.MethodGet
	path := "/api/catalog"
	if name != "catalog" {
		if !c.ValidID(name) {
			return c.Err("invalid", "Invalid command name")
		}
		method = http.MethodPost
		path = "/api/commands/" + url.PathEscape(name)
		var input io.Reader = stdin
		if *file != "" && *file != "-" {
			f, err := os.Open(*file)
			if err != nil {
				return c.Err("invalid", "Cannot open input file")
			}
			defer f.Close()
			input = f
		}
		if input == nil {
			return c.Err("invalid", "JSON input is required")
		}
		raw, err := io.ReadAll(io.LimitReader(input, command.MaxInputBytes+1))
		if err != nil {
			return c.Err("invalid", "Cannot read input")
		}
		if len(raw) > command.MaxInputBytes {
			return c.Err("invalid", "Input exceeds 64 KiB")
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(*base, "/")+path, body)
	if err != nil {
		return c.Err("invalid", "Invalid request")
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 65 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return c.Err("unavailable", "Cannot reach local server")
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, (16<<20)+1))
	if err != nil || len(raw) > 16<<20 || !json.Valid(raw) {
		return c.Err("internal", "Invalid server response")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var fault c.Fault
		if json.Unmarshal(raw, &fault) != nil || fault.Code == "" {
			return c.Err("internal", "Command failed")
		}
		return &fault
	}
	_, err = fmt.Fprintln(stdout, string(bytes.TrimSpace(raw)))
	return err
}
