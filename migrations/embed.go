package migrations

import "embed"

// FS embeds SQL migration files for Goose execution at startup or via CLI.
//
//go:embed *.sql
var FS embed.FS
