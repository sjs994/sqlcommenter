package pgdb

import (
	"database/sql"
	"log"

	"github.com/google/sqlcommenter/go/core"
	gosql "github.com/google/sqlcommenter/go/database/sql"
	_ "github.com/lib/pq"
)

func ConnectPG(connection string) *sql.DB {
	db, err := gosql.Open("postgres", connection,
		core.CommenterOptions{
			Config: core.CommenterConfig{
				EnableRoute:       true,
				EnableTraceparent: true,
				EnableController:  true,
				EnableApplication: true,
				EnableAction:      true,
				EnableDBDriver:    true,
			},
		})
	if err != nil {
		log.Fatalf("Failed to connect to PG(%q), error: %v", connection, err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping the database, error: %v", err)
	}

	return db
}
