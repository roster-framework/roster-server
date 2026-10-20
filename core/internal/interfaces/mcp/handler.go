package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/roster-framework/roster-server/core/internal/interfaces/command"
	httpapi "github.com/roster-framework/roster-server/core/internal/interfaces/http"
)

func Handler(registry *command.Registry) http.Handler {
	server := sdk.NewServer(&sdk.Implementation{Name: "roster", Version: "0.1.0"}, nil)
	for _, metadata := range registry.List() {
		server.AddTool(&sdk.Tool{Name: strings.ReplaceAll(metadata.Name, ".", "_"), Description: metadata.Description, InputSchema: metadata.InputSchema, OutputSchema: metadata.OutputSchema}, func(ctx context.Context, req *sdk.CallToolRequest) (*sdk.CallToolResult, error) {
			out, err := registry.Execute(ctx, metadata.Name, req.Params.Arguments)
			if err != nil {
				raw, _ := json.Marshal(command.PublicFault(err))
				return &sdk.CallToolResult{IsError: true, Content: []sdk.Content{&sdk.TextContent{Text: string(raw)}}}, nil
			}
			raw, err := json.Marshal(out)
			if err != nil {
				return &sdk.CallToolResult{IsError: true, Content: []sdk.Content{&sdk.TextContent{Text: `{"code":"internal","message":"Operation failed"}`}}}, nil
			}
			return &sdk.CallToolResult{StructuredContent: out, Content: []sdk.Content{&sdk.TextContent{Text: string(raw)}}}, nil
		})
	}
	transport := sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return server }, &sdk.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	return httpapi.LocalOnly(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, command.MaxInputBytes+(16<<10))
		transport.ServeHTTP(w, r)
	}))
}
