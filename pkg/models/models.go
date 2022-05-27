package models

import "fmt"

type MigrationVersion struct {
	Version          string
	Desc             string
	ExistsInDB       bool
	UpFileExists     bool
	DownFileExists 	 bool
}

func (mv MigrationVersion) Filename(migrationDirection string) string {
	return fmt.Sprintf("%s_%s.%s.sql", mv.Version, mv.Desc, migrationDirection)
}

func (mv MigrationVersion) FileExists(migrationDirection string) bool {
	if migrationDirection == "up" {
		return mv.UpFileExists
	} else if migrationDirection == "down" {
		return mv.DownFileExists
	} else {
		return false
	}
}