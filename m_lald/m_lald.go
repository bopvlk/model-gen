package m_lald

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
	Package = "m_lald"
	Table   = "assistants"
	ID      = "assistant_id"
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
	ProjectId     string
	AssistantId   string
	Name          string
	Purpose       spanner.NullString
	Instructions  spanner.NullString
	AiModel       spanner.NullString
	OwnerUserId   spanner.NullString
	LogoType      spanner.NullString
	LogoKey       spanner.NullString
	LogoLightPath spanner.NullString
	LogoDarkPath  spanner.NullString
	Deleted       spanner.NullBool
	UpdatedAt     spanner.NullTime
	CreatedAt     time.Time
	IsDefault     spanner.NullBool
}

type Field string

const (
	ProjectId     Field = "project_id"
	AssistantId   Field = "assistant_id"
	Name          Field = "name"
	Purpose       Field = "purpose"
	Instructions  Field = "instructions"
	AiModel       Field = "ai_model"
	OwnerUserId   Field = "owner_user_id"
	LogoType      Field = "logo_type"
	LogoKey       Field = "logo_key"
	LogoLightPath Field = "logo_light_path"
	LogoDarkPath  Field = "logo_dark_path"
	Deleted       Field = "deleted"
	UpdatedAt     Field = "updated_at"
	CreatedAt     Field = "created_at"
	IsDefault     Field = "is_default"
)

var allFieldsList = []Field{
	ProjectId,
	AssistantId,
	Name,
	Purpose,
	Instructions,
	AiModel,
	OwnerUserId,
	LogoType,
	LogoKey,
	LogoLightPath,
	LogoDarkPath,
	Deleted,
	UpdatedAt,
	CreatedAt,
	IsDefault,
}

func (f Field) String() string {
	return string(f)
}

func (data *Data) fieldPtrs(fields []Field) []interface{} {
	var ptrs []interface{}
	for _, field := range fields {
		fieldMap := map[Field]interface{}{
			ProjectId:     &data.ProjectId,
			AssistantId:   &data.AssistantId,
			Name:          &data.Name,
			Purpose:       &data.Purpose,
			Instructions:  &data.Instructions,
			AiModel:       &data.AiModel,
			OwnerUserId:   &data.OwnerUserId,
			LogoType:      &data.LogoType,
			LogoKey:       &data.LogoKey,
			LogoLightPath: &data.LogoLightPath,
			LogoDarkPath:  &data.LogoDarkPath,
			Deleted:       &data.Deleted,
			UpdatedAt:     &data.UpdatedAt,
			CreatedAt:     &data.CreatedAt,
			IsDefault:     &data.IsDefault,
		}
		ptrs = append(ptrs, fieldMap[field])
	}
	return ptrs
}

func (c *Facade) CreateMut(data *Data) *spanner.Mutation {
	columns := []string{
		ProjectId.String(),
		AssistantId.String(),
		Name.String(),
		Purpose.String(),
		Instructions.String(),
		AiModel.String(),
		OwnerUserId.String(),
		LogoType.String(),
		LogoKey.String(),
		LogoLightPath.String(),
		LogoDarkPath.String(),
		Deleted.String(),
		UpdatedAt.String(),
		CreatedAt.String(),
		IsDefault.String(),
	}

	values := []interface{}{
		data.ProjectId,
		data.AssistantId,
		data.Name,
		data.Purpose,
		data.Instructions,
		data.AiModel,
		data.OwnerUserId,
		data.LogoType,
		data.LogoKey,
		data.LogoLightPath,
		data.LogoDarkPath,
		data.Deleted,
		data.UpdatedAt,
		data.CreatedAt,
		data.IsDefault,
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
) bool {
	_, err := c.db.Single().ReadRow(
		ctx,
		Table,
		spanner.Key{
			projectId,
			assistantId,
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
) bool {
	_, err := tx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			projectId,
			assistantId,
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
	fields []Field,
) (*Data, error) {
	row, err := c.db.Single().ReadRow(
		ctx,
		Table,
		spanner.Key{
			projectId,
			assistantId,
		},
		utils.ToString(fields),
	)
	if err != nil {
		c.logError("Find", "Failed to ReadRow", log.H{
			"error":        err,
			"project_id":   projectId,
			"assistant_id": assistantId,
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
	fields []Field,
) (*Data, error) {
	row, err := rtx.ReadRow(
		ctx,
		Table,
		spanner.Key{
			projectId,
			assistantId,
		},
		utils.ToString(fields),
	)
	if err != nil {
		c.logError("Find", "Failed to ReadRow", log.H{
			"error":        err,
			"project_id":   projectId,
			"assistant_id": assistantId,
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
	data UpdateFields,
) *spanner.Mutation {
	mutationData := map[string]interface{}{
		ProjectId.String():   projectId,
		AssistantId.String(): assistantId,
	}
	for field, value := range data {
		mutationData[field.String()] = value
	}

	return spanner.Update(Table, allFieldsList, mutationData)
}

func (c *Facade) Update(
	ctx context.Context,
	projectId string,
	assistantId string,
	data UpdateFields,
) error {
	mutation := c.UpdateMut(
		projectId,
		assistantId,
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
) *spanner.Mutation {
	return spanner.Delete(Table, spanner.Key{
		projectId,
		assistantId,
	})
}

func (c *Facade) Delete(
	ctx context.Context,
	projectId string,
	assistantId string,
) error {
	mutation := c.DeleteMut(
		projectId,
		assistantId,
	)

	if _, err := c.db.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
		c.logError("Delete", "Failed to Apply", log.H{
			"error": err,
		})
		return fmt.Errorf("failed to delete file record: %w", err)
	}

	return nil
}
