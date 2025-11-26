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
        ?concept skos:inScheme <{collection_uri}> .
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
    Create SQLite database schema.
    
    Args:
        db_path: Path to the database file
        
    Returns:
        Database connection
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create concepts table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS concepts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            concept_uri TEXT UNIQUE NOT NULL,
            pref_label TEXT,
            alt_label TEXT,
            definition TEXT,
            notation TEXT,
            broader TEXT,
            narrower TEXT,
            related TEXT
        )
    """)
    
    # Create collection metadata table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS collection_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_uri TEXT UNIQUE NOT NULL,
            harvested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            concept_count INTEGER
        )
    """)
    
    conn.commit()
    return conn


def insert_results(conn, collection_uri, results):
    """
    Insert query results into the SQLite database.
    
    Args:
        conn: Database connection
        collection_uri: URI of the collection
        results: SPARQL query results
    """
    cursor = conn.cursor()
    
    bindings = results.get("results", {}).get("bindings", [])
    
    print(f"Inserting {len(bindings)} results into database...")
    
    # Prepare batch data for insertion
    concepts_data = []
    for binding in bindings:
        concept_uri = binding.get("concept", {}).get("value", "")
        pref_label = binding.get("prefLabel", {}).get("value", None)
        alt_label = binding.get("altLabel", {}).get("value", None)
        definition = binding.get("definition", {}).get("value", None)
        notation = binding.get("notation", {}).get("value", None)
        broader = binding.get("broader", {}).get("value", None)
        narrower = binding.get("narrower", {}).get("value", None)
        related = binding.get("related", {}).get("value", None)
        
        concepts_data.append((concept_uri, pref_label, alt_label, definition, 
                            notation, broader, narrower, related))
    
    # Use executemany for better performance
    cursor.executemany("""
        INSERT OR REPLACE INTO concepts 
        (concept_uri, pref_label, alt_label, definition, notation, broader, narrower, related)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, concepts_data)
    
    # Insert collection metadata
    cursor.execute("""
        INSERT OR REPLACE INTO collection_metadata (collection_uri, concept_count)
        VALUES (?, ?)
    """, (collection_uri, len(bindings)))
    
    conn.commit()
    print(f"Successfully inserted {len(bindings)} concepts")


def main():
    """Main execution function."""
    if len(sys.argv) < 2:
        print("Error: Collection URI is required")
        print("Usage: python harvest.py <collection-uri> [output-path]")
        sys.exit(1)
    
    collection_uri = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "harvest.db"
    
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
