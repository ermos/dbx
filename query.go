package dbx

import (
	"context"
	"database/sql"
	"errors"
	"github.com/jmoiron/sqlx"
)

type Instance struct {
	db 				*sqlx.Conn
	tx 				*sqlx.Tx
	Closed			bool
}

// Begins a transaction
func (i *Instance) Begin (ctx context.Context, opts *sql.TxOptions) error {
	var err error

	i.tx, err = i.db.BeginTxx(ctx, opts)

	return err
}

// Commit a transaction
func (i *Instance) Commit () error {
	if i.tx == nil {
		return errors.New("cannot commit an instance without transaction started")
	}

	err := i.tx.Commit()

	if err == nil {
		i.tx = nil
	}

	return err
}

// Rollback a transaction
func (i *Instance) Rollback () error {
	if i.tx == nil {
		return errors.New("cannot rollback an instance without transaction started")
	}

	err := i.tx.Rollback()

	if err == nil {
		i.tx = nil
	}

	return err
}

// Close current instance
func (i *Instance) Close () error {
	if i.Closed {
		return nil
	}

	err := i.db.Close()

	if err == nil {
		i.Closed = true
	}

	return err
}

/////////////////////////////////////////////////////////////////
// Overlayer without extra, just a simple manage for tx or not //
/////////////////////////////////////////////////////////////////
//  v   v   v   v   v   v   v   v   v   v   v   v   v   v   v

// Get executes a query that is expected to return at most one row in a structure.
func (i *Instance) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if i.tx != nil {
		return i.tx.GetContext(ctx, dest, query, args...)
	} else {
		return i.db.GetContext(ctx, dest, query, args...)
	}
}

// GetAll executes a query that is expected to return one or many rows in a array of structure.
func (i *Instance) GetAll(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	if i.tx != nil {
		return i.tx.SelectContext(ctx, dest, query, args...)
	} else {
		return i.db.SelectContext(ctx, dest, query, args...)
	}
}

/*
	Query executes a query that returns rows, typically a SELECT.
	The args are for any placeholder parameters in the query.
*/
func (i *Instance) Query(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	if i.tx != nil {
		return i.tx.QueryxContext(ctx, query, args...)
	} else {
		return i.db.QueryxContext(ctx, query, args...)
	}
}

/*
	QueryRow executes a query that is expected to return at most one row.
	QueryRow always returns a non-nil value. Errors are deferred until
	Row's Scan method is called.
	If the query selects no rows, the *sql.Row's Scan will return sql.ErrNoRows.
	Otherwise, the *sql.Row's Scan scans the first selected row and discards
	the rest.
*/
func (i *Instance) QueryRow(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	if i.tx != nil {
		return i.tx.QueryRowxContext(ctx, query, args...)
	} else {
		return i.db.QueryRowxContext(ctx, query, args...)
	}
}

/*
	Exec executes a query that doesn't return rows.
	For example: an INSERT and UPDATE.
*/
func (i *Instance) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if i.tx != nil {
		return i.tx.ExecContext(ctx, query, args...)
	} else {
		return i.db.ExecContext(ctx, query, args...)
	}
}