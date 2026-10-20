package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/roster-framework/roster-server/core/internal/authority"
	c "github.com/roster-framework/roster-server/core/internal/contracts"
	"github.com/roster-framework/roster-server/core/internal/host"
	"github.com/roster-framework/roster-server/core/internal/interfaces/cli"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() (runErr error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		return cli.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	}
	f := flag.NewFlagSet("serve", flag.ContinueOnError)
	listen := f.String("listen", "127.0.0.1:7350", "Local listen address")
	config := f.String("config", "", "Optional node/subscription/conversation configuration")
	authorityFile := f.String("authority", "", "Optional immutable call authority policy")
	if err := f.Parse(os.Args[2:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected serve arguments")
	}
	hostname, _, err := net.SplitHostPort(*listen)
	if err != nil {
		return err
	}
	ip := net.ParseIP(hostname)
	if hostname != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return errors.New("this initial core only listens on loopback")
	}
	var authPolicy *authority.Service
	if *authorityFile != "" {
		file, openErr := os.Open(*authorityFile)
		if openErr != nil {
			return errors.New("could not read authority policy")
		}
		policyBytes, readErr := io.ReadAll(io.LimitReader(file, authority.MaxPolicyBytes+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return errors.New("could not read authority policy")
		}
		authPolicy, err = authority.Parse(policyBytes)
		if err != nil {
			return errors.New("invalid authority policy")
		}
	}
	var initialConfig *host.Config
	if *config != "" {
		initialConfig, err = readInitialConfig(*config)
		if err != nil {
			return err
		}
	}
	core, err := host.New(ctx, host.Options{Authority: authPolicy, InitialConfig: initialConfig})
	if err != nil {
		return err
	}
	coreCloseCalled := false
	defer func() {
		if coreCloseCalled {
			return
		}
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		runErr = errors.Join(runErr, core.Close(shutdown))
	}()
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: core.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	fmt.Fprintln(os.Stderr, "roster ready at http://"+listener.Addr().String())
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		core.BeginDrain()
		closeErr := core.Close(shutdown)
		coreCloseCalled = true
		shutdownErr := server.Shutdown(shutdown)
		if shutdown.Err() != nil {
			_ = server.Close()
		}
		return errors.Join(closeErr, shutdownErr)
	}
}

func readInitialConfig(path string) (*host.Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, readErr := io.ReadAll(io.LimitReader(file, 65537))
	if readErr != nil {
		return nil, errors.New("could not read configuration")
	}
	if len(raw) > 65536 {
		return nil, errors.New("configuration exceeds 64 KiB")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("configuration must contain one JSON object")
	}
	if c.CheckJSONNumbers(raw) != nil {
		return nil, errors.New("configuration contains an unsupported JSON number")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var initial host.Config
	if err := decoder.Decode(&initial); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("configuration must contain one JSON object")
	}
	return &initial, nil
}
