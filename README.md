# model-gen

## Overview

`model-gen` is a tool that automatically generates Go models and methods for interacting with Google Cloud Spanner databases based on SQL Spanner table definitions. It scans `.sql` schema files and generates Go code with essential database operations.

### Supported Operations

For each Spanner table, the following methods are generated:

- `Find`: Fetch a row by the primary key.
- `Exists`: Check if a row exists.
- `Get`: Retrieve a row with detailed information.
- `Create`: Insert a new record.
- `CreateMut`: Batch insert records.
- `Update`: Update an existing record.
- `UpdateMut`: Batch update records.
- `Delete`: Delete a record.
- `DeleteMut`: Batch delete records.

### Features

- **Automatic Code Generation**: Reads `.sql` files to generate Go models with essential operations.
- **CRUD + Mutations**: Supports basic CRUD operations and batch mutations.
- **Customizable**: Easily extendable to support additional methods.
- **Facade Child Structs**: You should manually add child `Facade` structs if they exist.
- **One SQL file per folder**: Each folder should contain only one SQL file. Having multiple files may cause errors.

## Installation

You can install `model-gen` globally using `go install`:

```bash
go install github.com/bopvlk/model-gen@latest
```