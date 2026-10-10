package migrations

import "embed"

// FS embeds all SQL migration files in this package.
//
//go:embed *.sql
var FS embed.FS
