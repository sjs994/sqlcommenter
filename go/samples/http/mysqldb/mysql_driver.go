package mysqldb

import (
	"log"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	gosql "github.com/google/sqlcommenter/go/database/sql"
)

func ConnectMySQL(connection string) *sql.DB {
	db, err := gosql.Open("mysql", connection)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL(%q), error: %v", connection, err)
	}

	// err = db.Ping()
	// if err != nil {
	// 	log.Fatalf("Failed to ping the database, error: %v", err)
	// }

	return db
}
