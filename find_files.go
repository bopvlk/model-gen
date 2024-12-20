package main

import (
	"log"
	"os"
	"path/filepath"
	"runtime"
)

func findFilePaths() []string {
	// pwd of the directory
	_, filename, _, _ := runtime.Caller(1)
	dir := filepath.Dir(filename)

	// Slice to hold paths of .sql files
	var sqlFiles []string

	// Walk through the directory to find .sql files
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Check if the file has a .sql extension
		if !d.IsDir() && filepath.Ext(path) == ".sql" {
			sqlFiles = append(sqlFiles, path)
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
	return sqlFiles
}
