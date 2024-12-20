CREATE TABLE assistant_resources (
  project_id STRING(36) NOT NULL,
  assistant_id STRING(36) NOT NULL,
  resource_id STRING(36) NOT NULL,
  updated_at TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
  created_at TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(project_id, assistant_id, resource_id),
  INTERLEAVE IN PARENT assistants ON DELETE CASCADE;;