package main

import (
	"bufio"
	"bytes"
	"fmt"
	"go/format"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
)

var spannerTypeMapping = map[string]string{
	"INT64 NOT NULL":     "int64",
	"STRING NOT NULL":    "string",
	"TIMESTAMP NOT NULL": "time.Time",
	"DATE NOT NULL":      "time.Time",
	"BOOL NOT NULL":      "bool",
	"FLOAT64 NOT NULL":   "float64",
	"NUMERIC NOT NULL":   "big.Rat",
	"INT64":              "spanner.NullInt64",
	"STRING":             "spanner.NullString",
	"BYTES":              "[]byte",
	"TIMESTAMP":          "spanner.NullTime",
	"BOOL":               "spanner.NullBool",
	"FLOAT64":            "spanner.NullFloat64",
	"DATE":               "spanner.NullDate",
	"FLOAT32":            "spanner.NullFloat32",
	"JSON":               "spanner.NullJSON",
	"NUMERIC":            "spanner.NullNumeric",
	"STRUCT":             "interface{}",
}

var spannerArrTypeMapping = map[string]string{
	"ARRAY":     "[]",
	"INT64":     "int64",
	"STRING":    "string",
	"BYTES":     "[]byte",
	"BOOL":      "bool",
	"FLOAT64":   "float64",
	"TIMESTAMP": "time.Time",
	"DATE":      "time.Time",
	"NUMERIC":   "big.Rat",
	"JSON":      "spanner.NullJSON",
}

type Field struct {
	Name  string
	Type  string
	Snake string
}

type PrimaryKeys struct {
	Snake       string
	Camel       string
	CamelFileld string
}

type StructTemplateData struct {
	StructName  string
	Fields      []Field
	PackageName string
	ModuleName  string
	TableName   string
	ProjectName string
	PrimaryKeys []PrimaryKeys
	ID          string
}

func main() {
	moduleName, err := getModuleName()
	if err != nil {
		log.Fatalln("Error getting module name:", err)
	}
	for _, path := range findFilePaths() {
		file, err := os.Open(path)
		if err != nil {
			log.Fatalln("Error opening file:", err)
		}
		defer file.Close()

		// Regex patterns
		createTableRegex := regexp.MustCompile(`(?i)^CREATE TABLE (\w+)`)
		columnRegex := regexp.MustCompile(`(?i)^\s*(\w+)\s+(\w+)(?:\((\d+|MAX)\))?(?:\s+NOT\s+NULL)?`)

		primartKeysRegex := regexp.MustCompile(`PRIMARY KEY\s*\(([^)]+)\)`)

		var structName string
		var fields []Field
		var primaryKeys []PrimaryKeys
		var id string
		// Read file line by line
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())

			// Match table name
			if matches := createTableRegex.FindStringSubmatch(line); matches != nil {
				structName = toSnakeCase(matches[1])
				continue
			}

			// Match column definitions
			if matches := columnRegex.FindStringSubmatch(line); matches != nil {
				if strings.Contains(matches[1], "allow_commit_timestamp") {
					continue
				}
				var goType string
				columnName := matches[1]
				sqlType := matches[2]
				if strings.Contains(matches[0], "NOT NULL") {
					sqlType += " NOT NULL"
				}

				if strings.Contains(sqlType, "ARRAY") {
					goType = parceArray(sqlType)
				} else {
					t, ok := spannerTypeMapping[strings.ToUpper(sqlType)]
					if !ok {
						goType = "interface{}" // Default to interface{} if unknown type
					} else {
						goType = t
					}
				}

				fields = append(fields, Field{
					Name:  toCamelCase(columnName),
					Type:  goType,
					Snake: columnName,
				})
			}

			primaryKeysRegex := primartKeysRegex.FindStringSubmatch(line)
			if len(primaryKeysRegex) > 0 {
				pk := strings.Split(primaryKeysRegex[1], ",")
				primaryKeys = make([]PrimaryKeys, len(pk))
				id = strings.TrimSpace(pk[len(pk)-1])
				for i, key := range pk {
					key = strings.TrimSpace(key)
					primaryKeys[i].Snake = key
					key = toCamelCase(key)
					primaryKeys[i].CamelFileld = key
					key = firstLetterToLower(key)
					primaryKeys[i].Camel = key
				}
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatalln("Error reading file:", err)
		}

		packageName := strings.ToLower(filepath.Base(filepath.Dir(path)))
		// Generate Go struct
		data := StructTemplateData{
			Fields:      fields,
			PackageName: packageName,
			ModuleName:  moduleName,
			TableName:   structName,
			ProjectName: filepath.Dir(moduleName),
			PrimaryKeys: primaryKeys,
			ID:          id,
		}

		t, err := template.New("structTemplate").Parse(templateString)
		if err != nil {
			log.Fatalln("Error parsing template:", err)
		}
		var output bytes.Buffer
		err = t.Execute(&output, data)
		if err != nil {
			log.Fatalln("Error executing template:", err)
		}

		formattedOutput, err := format.Source(output.Bytes())
		if err != nil {
			log.Fatalln("Error formatting output:", err)
		}

		filePath := fmt.Sprintf("%s/%s.go", filepath.Dir(path), packageName)
		err = os.WriteFile(filePath, formattedOutput, 0644)
		if err != nil {
			log.Fatalln("Error writing file:", err)
		}

	}
}

func parceArray(s string) string {
	s = strings.Replace(s, "<", " ", -1)
	s = strings.Replace(s, ">", " ", -1)
	arr := strings.Fields(s)
	var goType string
	for _, a := range arr {
		for i, t := range a {
			if t == '(' {
				a = a[:i]
				break
			}
		}
		t, ok := spannerArrTypeMapping[strings.ToUpper(a)]
		if !ok {
			goType = "interface{}" // Default to interface{} if unknown type
		} else {
			goType += t
		}
	}
	return goType
}
