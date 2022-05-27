package dbrepo

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/jackc/pgconn"
	_ "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
)

type PostgresDBDriver struct {

}

func (d *PostgresDBDriver) Open(dbConnData DBConnectionData) (*sql.DB, error){
	dataSourceName := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s", dbConnData.DBHost, dbConnData.DBPort, dbConnData.DBName, dbConnData.DBUser, dbConnData.DBPassword, dbConnData.DBSSL)
	myDB, err := sql.Open("pgx", dataSourceName)
	if err != nil {
	  return nil, err
	}

	return myDB, nil
}

func (d *PostgresDBDriver) SetupMigrationTableSQL() string {
	return `CREATE TABLE IF NOT EXISTS public.schema_migration (
		"version" varchar(15) NOT NULL
	);
	CREATE UNIQUE INDEX IF NOT EXISTS schema_migration_version_idx ON public.schema_migration USING btree (version);`
}

func (d *PostgresDBDriver) MigrateDBSQL(migrationDirection string) (string, error) {
  switch strings.ToLower(migrationDirection) {
	case "up":
		return `insert into public.schema_migration (version) values ($1)`, nil
	case "down":
		return `delete from public.schema_migration where version = $1`, nil
	default:
		return "", errors.New("migrationDirection  - migration direction must be up or down")
	}
}

func (d *PostgresDBDriver) CurrentVersionSQL() string {
	return `select coalesce(max(version), '') as version from public.schema_migration`
}

func (d *PostgresDBDriver) MigratedVersionsSQL() string {
	return `select version from public.schema_migration order by version`
}