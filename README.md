# setup-harvest-action

Harvest NERC vocabulary collections from SPARQL endpoint to SQLite database.

## Usage

```yaml
- uses: marine-term-translations/setup-harvest-action@v1
  with:
    # URI of the collection to query (required)
    collection-uri: 'http://vocab.nerc.ac.uk/collection/P01/current/'
```

The database is always saved as `translations.db` in the root of the repository.

## Example Workflow

```yaml
name: Harvest Vocabulary
on:
  workflow_dispatch:
    inputs:
      collection:
        description: 'Collection URI'
        required: true
        default: 'http://vocab.nerc.ac.uk/collection/P01/current/'

jobs:
  harvest:
    runs-on: ubuntu-latest
    steps:
      - uses: marine-term-translations/setup-harvest-action@v1
        with:
          collection-uri: ${{ github.event.inputs.collection }}
      
      - name: Upload database
        uses: actions/upload-artifact@v4
        with:
          name: vocabulary-database
          path: translations.db
```

## Inputs

- `collection-uri` (required): URI of the NERC vocabulary collection to harvest

## Outputs

- `database-path`: Path to the generated SQLite database (always `translations.db`)

## Database Schema

The SQLite database uses a translation workflow schema with foreign key constraints:

### `terms` table
Stores vocabulary terms:
- `id`: Auto-increment primary key
- `uri`: Unique URI of the term
- `created_at`, `updated_at`: Timestamps

### `term_fields` table
Stores field values for each term:
- `id`: Auto-increment primary key
- `term_id`: Foreign key to terms
- `field_uri`: Full URI of the SKOS property
- `field_term`: Short form (e.g., `skos:prefLabel`)
- `original_value`: The original value from SPARQL
- `created_at`, `updated_at`: Timestamps

### `translations` table
Stores translations of term fields:
- `id`: Auto-increment primary key
- `term_field_id`: Foreign key to term_fields
- `language`: Language code (`nl`, `fr`, `de`, `es`, `it`, `pt`)
- `value`: Translated value
- `status`: `draft`, `review`, `approved`, `rejected`, `merged`
- `created_by`, `modified_by`, `reviewed_by`: User references

### `appeals` table
Stores translation appeals:
- `id`: Auto-increment primary key
- `translation_id`: Foreign key to translations
- `opened_by`: User who opened the appeal
- `status`: `open`, `closed`, `resolved`

### `appeal_messages` table
Stores messages on appeals:
- `id`: Auto-increment primary key
- `appeal_id`: Foreign key to appeals
- `author`: Message author
- `message`: Message content

### `users` table
Stores user information:
- `username`: Primary key
- `reputation`: User reputation score
- `joined_at`: Join timestamp

## SPARQL Query

The action queries the NERC SPARQL endpoint at `http://vocab.nerc.ac.uk/sparql/` using SKOS vocabulary properties to retrieve all concepts that belong to the specified collection.

## Versioning

This action uses [semantic versioning](https://semver.org/). You can pin to:

- A major version (e.g., `@v1`) - recommended for most users
- A specific version (e.g., `@v1.0.0`) - for maximum stability
- The `main` branch (e.g., `@main`) - for latest development (not recommended for production)

Example:
```yaml
# Use latest v1.x.x (recommended)
- uses: marine-term-translations/setup-harvest-action@v1

# Pin to specific version
- uses: marine-term-translations/setup-harvest-action@v1.0.0
```

### Creating a Release

To create a new release:

1. Create and push a semantic version tag:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

2. The release workflow will automatically:
   - Create a GitHub release with auto-generated release notes
   - Update the major version tag (e.g., `v1`) to point to the new release
