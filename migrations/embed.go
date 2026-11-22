package migrations

import "embed"

// FS 嵌入供 Goose 在启动或 CLI 执行时使用的 SQL 迁移脚本。
//
//go:embed *.sql
var FS embed.FS
