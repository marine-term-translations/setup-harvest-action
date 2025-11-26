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

### `concepts` table
Stores all concepts from the collection with the following columns:
- `id`: Auto-increment primary key
- `concept_uri`: Unique URI of the concept
- `pref_label`: Preferred label (skos:prefLabel)
- `alt_label`: Alternative label (skos:altLabel)
- `definition`: Concept definition (skos:definition)
- `notation`: Concept notation (skos:notation)
- `broader`: Broader concept URI (skos:broader)
- `narrower`: Narrower concept URI (skos:narrower)
- `related`: Related concept URI (skos:related)

### `collection_metadata` table
Stores metadata about the harvest:
- `id`: Auto-increment primary key
- `collection_uri`: URI of the harvested collection
- `harvested_at`: Timestamp of the harvest
- `concept_count`: Number of concepts harvested

## SPARQL Query

The action queries the NERC SPARQL endpoint at `http://vocab.nerc.ac.uk/sparql/` using SKOS vocabulary properties to retrieve all concepts that belong to the specified collection.
