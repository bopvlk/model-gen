CREATE TABLE assistants (
  project_id STRING(36) NOT NULL,
  assistant_id STRING(36) NOT NULL,
  name STRING(256) NOT NULL,
  purpose STRING(MAX),
  instructions STRING(MAX),
  ai_model STRING(42),
  owner_user_id STRING(36),
  logo_type STRING(16),
  logo_key STRING(42),
  logo_light_path STRING(256),
  logo_dark_path STRING(256),
  deleted BOOL DEFAULT (FALSE),
  updated_at TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
  created_at TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
  is_default BOOL,
) PRIMARY KEY(project_id, assistant_id);