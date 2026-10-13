package architecture_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestProductionImportBoundaries(t *testing.T) {
	root := filepath.Join("..", "..", "internal")
	const prefix = "github.com/roster-framework/roster-server/core/internal/"
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, _ := filepath.Rel(root, path)
		module := strings.Split(filepath.ToSlash(relative), "/")[0]
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range file.Imports {
			name, _ := strconv.Unquote(imp.Path.Value)
			local := strings.TrimPrefix(name, prefix)
			forbidden := false
			switch module {
			case "contracts":
				forbidden = strings.HasPrefix(name, prefix) || strings.Contains(name, ".") || name == "net" || strings.HasPrefix(name, "net/") || name == "database/sql" || name == "os" || name == "io/fs"
			case "lifecycle":
				forbidden = strings.Contains(name, ".") && name != prefix+"contracts" && name != prefix+"observation"
			case "invocation":
				forbidden = strings.HasPrefix(name, prefix) && (local == "event" || local == "conversation" || strings.HasPrefix(local, "adapters/") || strings.HasPrefix(local, "interfaces/"))
			case "event", "conversation":
				forbidden = strings.HasPrefix(name, prefix) && (strings.HasPrefix(local, "adapters/") || strings.HasPrefix(local, "interfaces/"))
			}
			if forbidden {
				t.Errorf("%s must not import %s", relative, name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
