package postgres

import (
	"testing"
)

func TestParseMigrationFilename(t *testing.T) {
	tests := []struct {
		filename    string
		wantVersion int
		wantName    string
		wantDir     string
		wantError   bool
	}{
		{
			filename:    "000001_create_jobs_and_attempts.up.sql",
			wantVersion: 1,
			wantName:    "create_jobs_and_attempts",
			wantDir:     "up",
			wantError:   false,
		},
		{
			filename:    "000001_create_jobs_and_attempts.down.sql",
			wantVersion: 1,
			wantName:    "create_jobs_and_attempts",
			wantDir:     "down",
			wantError:   false,
		},
		{
			filename:    "invalid_name.sql",
			wantError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			m, err := parseMigrationFilename(tt.filename)
			if (err != nil) != tt.wantError {
				t.Fatalf("parseMigrationFilename() error = %v, wantError %v", err, tt.wantError)
			}
			if !tt.wantError {
				if m.Version != tt.wantVersion {
					t.Errorf("version = %d, want %d", m.Version, tt.wantVersion)
				}
				if m.Name != tt.wantName {
					t.Errorf("name = %s, want %s", m.Name, tt.wantName)
				}
				if m.Direction != tt.wantDir {
					t.Errorf("direction = %s, want %s", m.Direction, tt.wantDir)
				}
			}
		})
	}
}
