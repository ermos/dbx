package dbx

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
)

type database struct {
	Name					string
	Object 					*sqlx.DB
}

var databases []database

// Create a new connector for instanciate easily database connection
func New (ctx context.Context, dbName string, driver string, user string, password string, host string, port string) error {
	dataSourceName := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true",
		user, password, host, port, dbName,
	)

	db, err := sqlx.ConnectContext(ctx, driver, dataSourceName)
	if  err != nil {
		log.Fatal(err)
	}

	err = db.PingContext(ctx)
	if  err != nil {
		log.Fatal(err)
	}

	db.SetMaxIdleConns(0)

	databases = append(databases, database{
		Name: dbName,
		Object: db,
	})

	return nil
}

// Create a new connection to database
func Conn (ctx context.Context, dbName string) *Instance {
	db := _findConnector(dbName)

	conn, err := db.Object.Connx(ctx)
	if err != nil {
		log.Fatal(err)
	}

	return &Instance{
		db: conn,
	}
}

// Create a new connection to database and start a transaction
func ConnTx (ctx context.Context, dbName string, opts *sql.TxOptions) *Instance {
	db := _findConnector(dbName)

	conn, err := db.Object.Connx(ctx)
	if err != nil {
		log.Fatal(err)
	}

	i :=  &Instance{
		db: conn,
	}

	err = i.Begin(ctx, opts)
	if err != nil {
		log.Fatal(err)
	}

	return i
}

func _findConnector (dbName string) database {
	for _, db := range databases {
		if db.Name == dbName {
			return db
		}
	}

	log.Fatalf("the connector %s is not initialize", dbName)

	return database{}
}