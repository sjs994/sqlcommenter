package pgdb

import (
	"log"
	"database/sql"

	gosql "github.com/google/sqlcommenter/go/database/sql"
	_ "github.com/lib/pq"
)

func ConnectPG(connection string) *sql.DB {
	db, err := gosql.Open("postgres", connection)
	if err != nil {
		log.Fatalf("Failed to connect to PG(%q), error: %v", connection, err)
	}

	// err = db.Ping()
	// if err != nil {
	// 	log.Fatalf("Failed to ping the database, error: %v", err)
	// }

	return db
}
