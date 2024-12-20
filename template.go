package main

var templateString = `package {{.PackageName}}

import (
    "context"
	"fmt"
    "strings"
	"time"

	"cloud.google.com/go/spanner"
    "{{.ModuleName}}/m_options"
    "{{.ProjectName}}/log"
    "{{.ProjectName}}/utils"

)

const (
    Package = "{{.PackageName}}"
    Table = "{{.TableName}}"
    ID = "{{.ID}}"
)

type Facade struct {
	log *log.Logger
	db  *spanner.Client
}

func New(o *m_options.Options) *Facade {
	return &Facade{
		log: o.Log,
		db:  o.DB,
	}
}

func (c *Facade) logError(functionName string, msg string, h log.H) {
	c.log.Error(fmt.Sprintf("[%s.%s - %s] %s", Package, functionName, Table, msg), h)
}

type Data struct {
{{- range .Fields}}
	{{.Name}} {{.Type}}
{{- end}}
}

type Field string

const (
{{- range .Fields}}
    {{.Name}} Field = "{{.Snake}}"
{{- end}}
)

var allFieldsList = []Field{
{{- range .Fields}}
    {{.Name}},
{{- end}}
}

func (f Field) String() string {
    return string(f)
}

func (data *Data) fieldPtrs(fields []Field) []interface{} {
	var ptrs []interface{}
	for _, field := range fields {
		fieldMap := map[Field]interface{}{
{{- range .Fields}}
            {{.Name}}: &data.{{.Name}},
{{- end}}
        }
        ptrs = append(ptrs, fieldMap[field])
    }
    return ptrs
}

func (c *Facade) CreateMut(data *Data) *spanner.Mutation {
	columns := []string{
{{- range .Fields}}
        {{.Name}}.String(),
{{- end}}
    }
   
   values := []interface{}{
{{- range .Fields}}
        data.{{.Name}},
{{- end}}
    }
    
    return spanner.Insert(Table, columns, values)
}


func (c *Facade) Create(ctx context.Context, data *Data) error {
	mutation := c.CreateMut(data)

	if _, err := c.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		c.logError("Create", "Failed to Apply", log.H{
			"error": err,
			"data":  data,
		})
		return fmt.Errorf("failed to create file record: %w", err)
	}

	return nil
}

func (c *Facade) Exists(
    ctx context.Context, 
{{- range .PrimaryKeys }}
    {{.Camel }} string,
{{- end }}
) bool {
	_, err := c.db.Single().ReadRow(
		ctx,
		Table,
		spanner.Key{
			{{- range .PrimaryKeys }}
			{{ .Camel}},
			{{- end }}
		},
		[]string{string(ID)},
	)
	return err == nil
}

func (c *Facade) ExistsRtx(
	ctx context.Context,
	tx *spanner.ReadOnlyTransaction,
{{- range .PrimaryKeys }}
    {{.Camel }} string,
{{- end }}
) bool {
    _, err := tx.ReadRow(
        ctx,
        Table,
        spanner.Key{
            {{- range .PrimaryKeys }}
            {{ .Camel }},
            {{- end }}
        },
        []string{string(ID)},
    )
    return err == nil
}

type QueryParam struct {
	Field    Field
	Operator string
	Value    interface{}
}

func (c *Facade) Get(
	ctx context.Context,
	queryParams []QueryParam,
	fields []Field,
) ([]*Data, error) {
	var whereClauses []string
	var params = map[string]interface{}{}
	for i, qp := range queryParams {
		paramName := fmt.Sprintf("param%d", i)
		param := fmt.Sprintf("@%s", paramName)
		if qp.Operator == "IN" {
			param = fmt.Sprintf("UNNEST(%s)", param)
		}
		whereClause := fmt.Sprintf("%s %s %s", qp.Field, qp.Operator, param)
		whereClauses = append(whereClauses, whereClause)
		params[paramName] = qp.Value
	}

	queryString := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(utils.ToString(fields), ", "), Table)
	if len(whereClauses) > 0 {
		queryString += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	stmt := spanner.Statement{
		SQL:    queryString,
		Params: params,
	}
	iter := c.db.Single().Query(ctx, stmt)
	defer iter.Stop()

	res := []*Data{}

	err := iter.Do(func(row *spanner.Row) error {
		var data Data

		if err := row.Columns(data.fieldPtrs(fields)...); err != nil {
			c.logError("Get", "Failed to Scan", log.H{
				"error":        err,
				"query_params": queryParams,
				"fields":       fields,
			})
			return err
		}

		res = append(res, &data)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}


func (c *Facade) Find(
	ctx context.Context,
{{- range .PrimaryKeys }}
    {{.Camel }} string,
{{- end }}
    fields []Field,
) (*Data, error) {
	row, err := c.db.Single().ReadRow(
        ctx,
        Table,
        spanner.Key{
            {{- range .PrimaryKeys }}
            {{ .Camel }},
            {{- end }}
        },
        utils.ToString(fields),
    )
    if err != nil {
		c.logError("Find", "Failed to ReadRow", log.H{
			"error":           err,
		{{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
        {{- end }}
			"fields":          fields,
		})
		return nil, err
	}

	var data Data

	err = row.Columns(data.fieldPtrs(fields)...)
	if err != nil {
		c.logError("Find", "Failed to Scan", log.H{
			"error":  err,
            {{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
            {{- end }}
			"fields": fields,
		})
		return nil, err
	}

	return &data, nil
}

func (c *Facade) FindRtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
{{- range .PrimaryKeys }}
    {{.Camel }} string,
{{- end }}
    fields []Field,
) (*Data, error) {
    row, err := rtx.ReadRow(
        ctx,
        Table,
        spanner.Key{
            {{- range .PrimaryKeys }}
            {{ .Camel }},
            {{- end }}
        },
        utils.ToString(fields),
    )
    if err != nil {
        c.logError("Find", "Failed to ReadRow", log.H{
            "error":           err,
        {{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
        {{- end }}
            "fields":          fields,
        })
        return nil, err
    }

    var data Data

    err = row.Columns(data.fieldPtrs(fields)...)
    if err != nil {
        c.logError("Find", "Failed to Scan", log.H{
            "error":  err,
            {{- range .PrimaryKeys }}
            "{{.Snake}}": {{.Camel}},
            {{- end }}
            "fields": fields,
        })
        return nil, err
    }

    return &data, nil
}

type UpdateFields map[Field]interface{}

func (c *Facade) UpdateMut(
	{{- range .PrimaryKeys }}
	{{.Camel }} string,
	{{- end }}
	data UpdateFields,
) *spanner.Mutation {
	mutationData := map[string]interface{}{
	{{- range .PrimaryKeys }}
		{{.CamelFileld}}.String(): {{.Camel}},
	{{- end }}
	}
	for field, value := range data {
		mutationData[field.String()] = value
	}

	return spanner.UpdateMap(Table, mutationData)
}


func (c *Facade) Update(
	ctx context.Context,
	{{- range .PrimaryKeys }}
	{{.Camel }} string,
	{{- end }}
	data UpdateFields,
) error {
	mutation := c.UpdateMut(
		{{- range .PrimaryKeys }}
		{{.Camel }},
		{{- end }}
		data,
	)

	if _, err := c.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		c.logError("Update", "Failed to Apply", log.H{
			"error": err,
			"data":  data,
		})
		return fmt.Errorf("failed to update file record: %w", err)
	}

	return nil
}

func (c *Facade) DeleteMut(
	{{- range .PrimaryKeys }}
	{{.Camel }} string,
	{{- end }}
) *spanner.Mutation {
	return spanner.Delete(Table, spanner.Key{
		{{- range .PrimaryKeys }}
		{{.Camel}},
		{{- end }}
	})
}

func (c *Facade) Delete(
	ctx context.Context,
	{{- range .PrimaryKeys }}
	{{.Camel }} string,
	{{- end }}
) error {
	mutation := c.DeleteMut(
		{{- range .PrimaryKeys }}
		{{.Camel }},
		{{- end }}
	)

	if _, err := c.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		c.logError("Delete", "Failed to Apply", log.H{
			"error": err,
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}`
