package dbx

import (
	"context"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

type databaseReader struct {
	Driver 		string	`json:"driver"`
	Name		string	`json:"name"`
	Host		string	`json:"host"`
	Port		string	`json:"port"`
	User 		string	`json:"user"`
	Password 	string	`json:"password"`
}

// Load all database schema contain in a folder
func LoadDatabases (ctx context.Context, path string) {
	var files []string
	var rgx = regexp.MustCompile(`(?i)\.json$`)

	err := filepath.Walk(path, func(file string, info os.FileInfo, err error) error {
		files = append(files, file)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if len(rgx.FindStringIndex(file)) == 0 {
			continue
		}

		_loadDatabase(ctx, file)
	}
}

// Load one database schema
func LoadDatabase (ctx context.Context, file string) {
	_loadDatabase(ctx, file)
}

func _loadDatabase (ctx context.Context, file string) {
	var dr databaseReader

	f, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(f, &dr)
	if err != nil {
		log.Fatal(err)
	}

	err = New(ctx, dr.Name, dr.Driver, dr.User, dr.Password, dr.Host, dr.Port)
	if err != nil {
		log.Fatal(err)
	}
}
