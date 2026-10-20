// Integration-only Docker bridge; production listeners still require TLS outside loopback.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

func proxy(address, target string) (*http.Server, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: httputil.NewSingleHostReverseProxy(u), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	return server, nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	port := os.Getenv("PORT")
	if port == "" {
		return errors.New("verification port required")
	}
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}
	ip := ""
	for _, address := range addresses {
		if network, ok := address.(*net.IPNet); ok && !network.IP.IsLoopback() && network.IP.To4() != nil {
			ip = network.IP.String()
			break
		}
	}
	if ip == "" {
		return errors.New("container IPv4 unavailable")
	}
	edge, err := proxy(net.JoinHostPort(ip, port), "http://127.0.0.1:"+port)
	if err != nil {
		return err
	}
	defer edge.Close()
	child := exec.Command("go", "run", "./cmd/roster-log-server")
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = child.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	select {
	case err = <-done:
		return err
	case <-ctx.Done():
		_ = syscall.Kill(-child.Process.Pid, syscall.SIGTERM)
		select {
		case <-done:
			return nil
		case <-time.After(2 * time.Second):
			_ = syscall.Kill(-child.Process.Pid, syscall.SIGKILL)
			<-done
			return nil
		}
	}
}
