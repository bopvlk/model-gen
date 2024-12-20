package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func getModuleName() (string, error) {
	// Open the go.mod file
	file, err := os.Open("go.mod")
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Look for the line starting with "module"
		if strings.HasPrefix(line, "module ") {
			// Extract and return the module name
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		return "", err
	}

	return "", fmt.Errorf("module name not found")
}
