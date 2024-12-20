package m_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/bopvlk/log"
	"github.com/bopvlk/model-gen/m_options"
	"github.com/bopvlk/utils"
)

const (
	Package = "m_test"
	Table   = "assistant_resources"
	ID      = "resource_id"
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
	ProjectId   string
	AssistantId string
	ResourceId  string
	UpdatedAt   spanner.NullTime
	CreatedAt   time.Time
}

type Field string

const (
	ProjectId   Field = "project_id"
	AssistantId Field = "assistant_id"
	ResourceId  Field = "resource_id"
	UpdatedAt   Field = "updated_at"
	CreatedAt   Field = "created_at"
)

var allFieldsList = []Field{
	ProjectId,
	AssistantId,
	ResourceId,
	UpdatedAt,
	CreatedAt,
}

func (f Field) String() string {
	return string(f)
}

func (data *Data) fieldPtrs(fields []Field) []interface{} {
	var ptrs []interface{}
	for _, field := range fields {
		fieldMap := map[Field]interface{}{
			ProjectId:   &data.ProjectId,
			AssistantId: &data.AssistantId,
			ResourceId:  &data.ResourceId,
			UpdatedAt:   &data.UpdatedAt,
			CreatedAt:   &data.CreatedAt,
		}
		ptrs = append(ptrs, fieldMap[field])
	}
	return ptrs
}

func (c *Facade) CreateMut(data *Data) *spanner.Mutation {
	columns := []string{
		ProjectId.String(),
		AssistantId.String(),
		ResourceId.String(),
		UpdatedAt.String(),
		CreatedAt.String(),
	}

	values := []interface{}{
		data.ProjectId,
		data.AssistantId,
		data.ResourceId,
		data.UpdatedAt,
		data.CreatedAt,
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
	projectId string,
	assistantId string,
	resourceId string,
) bool {
	_, err := c.db.Single().ReadRow(
		ctx,
		Table,
		spanner.Key{
			projectId,
			assistantId,
			resourceId,
		},
		[]string{string(ID)},
	)
	return err == nil
}

func (c *Facade) ExistsRtx(
	ctx context.Context,
	tx *spanner.ReadOnlyTransaction,
	projectId string,
	assistantId string,
	resourceId string,
) bool {
	_, err := tx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			projectId,
			assistantId,
			resourceId,
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
	projectId string,
	assistantId string,
	resourceId string,
	fields []Field,
) (*Data, error) {
	row, err := c.db.Single().ReadRow(
		ctx,
		Table,
		spanner.Key{
			projectId,
			assistantId,
			resourceId,
		},
		utils.ToString(fields),
	)
	if err != nil {
		c.logError("Find", "Failed to ReadRow", log.H{
			"error":        err,
			"project_id":   projectId,
			"assistant_id": assistantId,
			"resource_id":  resourceId,
			"fields":       fields,
		})
		return nil, err
	}

	var data Data

	err = row.Columns(data.fieldPtrs(fields)...)
	if err != nil {
		c.logError("Find", "Failed to Scan", log.H{
			"error":        err,
			"project_id":   projectId,
			"assistant_id": assistantId,
			"resource_id":  resourceId,
			"fields":       fields,
		})
		return nil, err
	}

	return &data, nil
}

func (c *Facade) FindRtx(
	ctx context.Context,
	rtx *spanner.ReadOnlyTransaction,
	projectId string,
	assistantId string,
	resourceId string,
	fields []Field,
) (*Data, error) {
	row, err := rtx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			projectId,
			assistantId,
			resourceId,
		},
		utils.ToString(fields),
	)
	if err != nil {
		c.logError("Find", "Failed to ReadRow", log.H{
			"error":        err,
			"project_id":   projectId,
			"assistant_id": assistantId,
			"resource_id":  resourceId,
			"fields":       fields,
		})
		return nil, err
	}

	var data Data

	err = row.Columns(data.fieldPtrs(fields)...)
	if err != nil {
		c.logError("Find", "Failed to Scan", log.H{
			"error":        err,
			"project_id":   projectId,
			"assistant_id": assistantId,
			"resource_id":  resourceId,
			"fields":       fields,
		})
		return nil, err
	}

	return &data, nil
}

type UpdateFields map[Field]interface{}

func (c *Facade) UpdateMut(
	projectId string,
	assistantId string,
	resourceId string,
	data UpdateFields,
) *spanner.Mutation {
	mutationData := map[string]interface{}{
		ProjectId.String():   projectId,
		AssistantId.String(): assistantId,
		ResourceId.String():  resourceId,
	}
	for field, value := range data {
		mutationData[field.String()] = value
	}

	return spanner.Update(Table, mutationData, mutationData)
}

func (c *Facade) Update(
	ctx context.Context,
	projectId string,
	assistantId string,
	resourceId string,
	data UpdateFields,
) error {
	mutation := c.UpdateMut(
		projectId,
		assistantId,
		resourceId,
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
	projectId string,
	assistantId string,
	resourceId string,
) *spanner.Mutation {
	return spanner.Delete(Table, spanner.Key{
		projectId,
		assistantId,
		resourceId,
	})
}

func (c *Facade) Delete(
	ctx context.Context,
	projectId string,
	assistantId string,
	resourceId string,
) error {
	mutation := c.DeleteMut(
		projectId,
		assistantId,
		resourceId,
	)

	if _, err := c.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		c.logError("Delete", "Failed to Apply", log.H{
			"error": err,
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}
