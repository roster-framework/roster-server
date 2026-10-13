package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
	core, err := host.New(context.Background(), host.Options{})
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
	if *config != "" {
		file, err := os.Open(*config)
		if err != nil {
			return err
		}
		defer file.Close()
		decoder := json.NewDecoder(io.LimitReader(file, 65537))
		decoder.DisallowUnknownFields()
		var initial host.Config
		if err = decoder.Decode(&initial); err != nil {
			return err
		}
		if err = decoder.Decode(new(any)); err != io.EOF {
			return errors.New("configuration must contain one JSON object")
		}
		setup, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err = core.Configure(setup, initial); err != nil {
			return err
		}
	}
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
