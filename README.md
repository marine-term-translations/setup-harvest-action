# setup-harvest-action

Harvest NERC vocabulary collections from SPARQL endpoint to SQLite database.

## Usage

```yaml
- uses: marine-term-translations/setup-harvest-action@v1
  with:
    # URI of the collection to query (required)
    collection-uri: 'http://vocab.nerc.ac.uk/collection/P01/current/'
    
    # Path where to save the SQLite database (optional, default: harvest.db)
    output-path: 'data/harvest.db'
```

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
          output-path: 'vocab.db'
      
      - name: Upload database
        uses: actions/upload-artifact@v3
        with:
          name: vocabulary-database
          path: vocab.db
```

## Inputs

- `collection-uri` (required): URI of the NERC vocabulary collection to harvest
- `output-path` (optional): Path for the output SQLite database (default: `harvest.db`)

## Outputs

- `database-path`: Path to the generated SQLite database

## Database Schema

The SQLite database contains:
- `concepts` table: All concepts with labels, definitions, and relationships
- `collection_metadata` table: Collection URI and harvest timestamp
