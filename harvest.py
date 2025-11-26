#!/usr/bin/env python3
"""
Harvest NERC vocabulary collection from SPARQL endpoint to SQLite database.
"""

import sys
import sqlite3
import os
import re
from SPARQLWrapper import SPARQLWrapper, JSON

SPARQL_ENDPOINT = "http://vocab.nerc.ac.uk/sparql/"

# Field mappings: SPARQL variable name -> (field_uri, field_term)
FIELD_MAPPINGS = {
    "prefLabel": ("http://www.w3.org/2004/02/skos/core#prefLabel", "skos:prefLabel"),
    "altLabel": ("http://www.w3.org/2004/02/skos/core#altLabel", "skos:altLabel"),
    "definition": ("http://www.w3.org/2004/02/skos/core#definition", "skos:definition"),
    "notation": ("http://www.w3.org/2004/02/skos/core#notation", "skos:notation"),
    "broader": ("http://www.w3.org/2004/02/skos/core#broader", "skos:broader"),
    "narrower": ("http://www.w3.org/2004/02/skos/core#narrower", "skos:narrower"),
    "related": ("http://www.w3.org/2004/02/skos/core#related", "skos:related"),
}


def validate_collection_uri(uri):
    """
    Validate that the collection URI is properly formatted.
    
    Args:
        uri: Collection URI to validate
        
    Returns:
        bool: True if valid
        
    Raises:
        ValueError: If URI is invalid
    """
    # Basic URI validation - must start with http:// or https://
    if not re.match(r'^https?://', uri):
        raise ValueError(f"Invalid collection URI: {uri}. Must start with http:// or https://")
    
    # Additional validation: should contain vocab.nerc.ac.uk for this specific endpoint
    if 'vocab.nerc.ac.uk' not in uri:
        print(f"Warning: Collection URI does not contain 'vocab.nerc.ac.uk': {uri}")
    
    return True


def create_sparql_query(collection_uri):
    """
    Create SPARQL query to fetch all concepts from a collection.
    
    Args:
        collection_uri: URI of the collection to query (will be validated)
        
    Returns:
        SPARQL query string
    """
    # Validate URI before using in query
    validate_collection_uri(collection_uri)
    
    query = f"""
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX dc: <http://purl.org/dc/terms/>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    
    SELECT DISTINCT ?concept ?prefLabel ?altLabel ?definition ?notation ?broader ?narrower ?related
    WHERE {{
        <{collection_uri}> skos:member ?concept .
        OPTIONAL {{ ?concept skos:prefLabel ?prefLabel }}
        OPTIONAL {{ ?concept skos:altLabel ?altLabel }}
        OPTIONAL {{ ?concept skos:definition ?definition }}
        OPTIONAL {{ ?concept skos:notation ?notation }}
        OPTIONAL {{ ?concept skos:broader ?broader }}
        OPTIONAL {{ ?concept skos:narrower ?narrower }}
        OPTIONAL {{ ?concept skos:related ?related }}
    }}
    ORDER BY ?concept
    """
    return query


def query_sparql_endpoint(collection_uri):
    """
    Query the SPARQL endpoint for collection data.
    
    Args:
        collection_uri: URI of the collection to query
        
    Returns:
        Query results as JSON
        
    Raises:
        Exception: If SPARQL query fails
    """
    try:
        sparql = SPARQLWrapper(SPARQL_ENDPOINT)
        sparql.setQuery(create_sparql_query(collection_uri))
        sparql.setReturnFormat(JSON)
        
        print(f"Querying SPARQL endpoint for collection: {collection_uri}")
        results = sparql.query().convert()
        
        return results
    except Exception as e:
        raise Exception(f"SPARQL query failed: {str(e)}") from e


def create_database(db_path):
    """
    Create or open SQLite database with translation workflow schema.
    
    If the database already exists, it will be opened and any missing
    tables/indexes will be created. Existing data is preserved.
    
    Args:
        db_path: Path to the database file
        
    Returns:
        Database connection
    """
    db_exists = os.path.exists(db_path)
    
    if db_exists:
        print(f"Opening existing database: {db_path}")
    else:
        print(f"Creating new database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # Create terms table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS terms (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            uri         TEXT    NOT NULL UNIQUE,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create term_fields table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS term_fields (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            term_id       INTEGER NOT NULL REFERENCES terms(id) ON DELETE CASCADE,
            field_uri     TEXT    NOT NULL,
            field_term    TEXT    NOT NULL,
            original_value TEXT   NOT NULL,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(term_id, field_uri, original_value)
        )
    """)
    
    # Create translations table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS translations (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            term_field_id  INTEGER NOT NULL REFERENCES term_fields(id) ON DELETE CASCADE,
            language       TEXT    NOT NULL CHECK(language IN ('nl','fr','de','es','it','pt')),
            value          TEXT    NOT NULL,
            status         TEXT    NOT NULL DEFAULT 'draft' CHECK(status IN ('draft', 'review', 'approved', 'rejected', 'merged')),
            created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
            created_by     TEXT    NOT NULL,
            modified_at    DATETIME,
            modified_by    TEXT,
            reviewed_by    TEXT,
            UNIQUE(term_field_id, language)
        )
    """)
    
    # Create appeals table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS appeals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            translation_id  INTEGER NOT NULL REFERENCES translations(id) ON DELETE CASCADE,
            opened_by       TEXT    NOT NULL,
            opened_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
            closed_at       DATETIME,
            status          TEXT    NOT NULL DEFAULT 'open' CHECK(status IN ('open', 'closed', 'resolved')),
            resolution      TEXT,
            UNIQUE(translation_id, status)
        )
    """)
    
    # Create appeal_messages table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS appeal_messages (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            appeal_id   INTEGER NOT NULL REFERENCES appeals(id) ON DELETE CASCADE,
            author      TEXT    NOT NULL,
            message     TEXT    NOT NULL,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username    TEXT PRIMARY KEY,
            reputation  INTEGER DEFAULT 0,
            joined_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
            extra       TEXT
        )
    """)
    
    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_translations_status ON translations(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_translations_lang ON translations(language)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_appeals_status ON appeals(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_term_fields_term_id ON term_fields(term_id)")
    
    conn.commit()
    return conn


def insert_results(conn, collection_uri, results):
    """
    Insert or update query results into the SQLite database.
    
    For existing databases:
    - Existing terms are updated with new `updated_at` timestamp
    - New term fields are added, duplicates are ignored
    - Existing translations, appeals, etc. are preserved
    
    Args:
        conn: Database connection
        collection_uri: URI of the collection
        results: SPARQL query results
    """
    cursor = conn.cursor()
    
    bindings = results.get("results", {}).get("bindings", [])
    
    print(f"Processing {len(bindings)} results...")
    
    # Track statistics
    terms_inserted = 0
    terms_updated = 0
    term_fields_inserted = 0
    
    # Track processed terms to avoid redundant updates
    terms_processed = set()
    
    for binding in bindings:
        concept_uri = binding.get("concept", {}).get("value", "")
        if not concept_uri:
            continue
        
        # Check if term already exists
        if concept_uri not in terms_processed:
            cursor.execute("SELECT id FROM terms WHERE uri = ?", (concept_uri,))
            existing_term = cursor.fetchone()
            
            if existing_term:
                # Update existing term's updated_at timestamp
                cursor.execute("""
                    UPDATE terms SET updated_at = CURRENT_TIMESTAMP WHERE uri = ?
                """, (concept_uri,))
                terms_updated += 1
            else:
                # Insert new term
                cursor.execute("""
                    INSERT INTO terms (uri) VALUES (?)
                """, (concept_uri,))
                terms_inserted += 1
            
            terms_processed.add(concept_uri)
        
        # Get term_id
        cursor.execute("SELECT id FROM terms WHERE uri = ?", (concept_uri,))
        term_row = cursor.fetchone()
        if not term_row:
            continue
        term_id = term_row[0]
        
        # Insert term fields for each SKOS property
        for field_name, (field_uri, field_term) in FIELD_MAPPINGS.items():
            field_value = binding.get(field_name, {}).get("value")
            if field_value:
                # Try to insert, ignore if duplicate (preserves existing translations)
                cursor.execute("""
                    INSERT OR IGNORE INTO term_fields 
                    (term_id, field_uri, field_term, original_value)
                    VALUES (?, ?, ?, ?)
                """, (term_id, field_uri, field_term, field_value))
                if cursor.rowcount > 0:
                    term_fields_inserted += 1
    
    conn.commit()
    
    # Print summary
    print(f"Harvest summary:")
    print(f"  - New terms inserted: {terms_inserted}")
    print(f"  - Existing terms updated: {terms_updated}")
    print(f"  - New term fields inserted: {term_fields_inserted}")


def main():
    """Main execution function."""
    if len(sys.argv) < 2:
        print("Error: Collection URI is required")
        print("Usage: python harvest.py <collection-uri>")
        sys.exit(1)
    
    collection_uri = sys.argv[1]
    # Database path is fixed for standardized usage across all consumers
    output_path = "translations.db"
    
    print(f"Starting harvest for collection: {collection_uri}")
    print(f"Output database: {output_path}")
    
    try:
        # Validate collection URI
        validate_collection_uri(collection_uri)
        
        # Query SPARQL endpoint
        results = query_sparql_endpoint(collection_uri)
        
        # Create database
        conn = create_database(output_path)
        
        # Insert results
        insert_results(conn, collection_uri, results)
        
        # Close connection
        conn.close()
        
        print(f"Harvest completed successfully!")
        print(f"Database saved to: {output_path}")
        
        # Set output for GitHub Actions
        if "GITHUB_OUTPUT" in os.environ:
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"database-path={output_path}\n")
    
    except ValueError as e:
        print(f"Invalid input: {e}")
        sys.exit(1)
    except sqlite3.Error as e:
        print(f"Database error during harvest: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error during harvest: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
