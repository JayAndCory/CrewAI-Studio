#!/usr/bin/env python3
"""
Migration script to convert the 'data' column in the 'entities' table from TEXT to JSONB.
This script should be run after updating db_utils.py to support JSON type.
"""

import os
import json
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get database URL from environment or use default
DEFAULT_DB_URL = 'postgresql://postgres:postgres@localhost:5432/crewai'
DB_URL = os.getenv('DB_URL', DEFAULT_DB_URL)
logging.info(f"Using DB_URL: {DB_URL}")

# Create a SQLAlchemy Engine
engine = create_engine(DB_URL, echo=False)

def get_db_connection():
    """Return a context-managed connection from the SQLAlchemy engine."""
    return engine.connect()

def backup_data():
    """Backup all data from the entities table to a JSON file."""
    logging.info("Backing up data...")
    
    with get_db_connection() as conn:
        # Get all data from the entities table
        query = text('SELECT * FROM entities')
        result = conn.execute(query)
        
        # Convert to list of dictionaries
        rows = []
        for row in result:
            try:
                data = json.loads(row.data)
                rows.append({
                    'id': row.id,
                    'entity_type': row.entity_type,
                    'data': data
                })
            except Exception as e:
                logging.error(f"Error processing row {row.id}: {e}")
                continue
        
        # Write to backup file
        backup_file = 'entities_backup.json'
        with open(backup_file, 'w') as f:
            json.dump(rows, f, indent=4)
        
        logging.info(f"Backup completed: {len(rows)} entities saved to {backup_file}")
        return backup_file

def migrate_to_jsonb():
    """Migrate the 'data' column from TEXT to JSONB in PostgreSQL."""
    # Check if we're using PostgreSQL
    is_postgres = 'postgresql' in DB_URL.lower()
    
    if not is_postgres:
        logging.info("Not using PostgreSQL, no migration needed.")
        return False
    
    logging.info("Starting migration to JSONB...")
    
    with get_db_connection() as conn:
        try:
            # Create a temporary table with JSONB column
            conn.execute(text('''
                CREATE TABLE entities_new (
                    id TEXT PRIMARY KEY,
                    entity_type TEXT,
                    data JSONB
                )
            '''))
            
            # Copy data from old table to new table, converting TEXT to JSONB
            conn.execute(text('''
                INSERT INTO entities_new (id, entity_type, data)
                SELECT id, entity_type, data::jsonb FROM entities
            '''))
            
            # Drop the old table
            conn.execute(text('DROP TABLE entities'))
            
            # Rename the new table to the original name
            conn.execute(text('ALTER TABLE entities_new RENAME TO entities'))
            
            conn.commit()
            logging.info("Migration completed successfully!")
            return True
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Migration failed: {e}")
            return False

def restore_from_backup(backup_file):
    """Restore data from backup file if migration fails."""
    logging.info(f"Restoring from backup: {backup_file}")
    
    try:
        with open(backup_file, 'r') as f:
            data = json.load(f)
        
        # Check if we're using PostgreSQL
        is_postgres = 'postgresql' in DB_URL.lower()
        
        with get_db_connection() as conn:
            # Recreate the table if it doesn't exist
            if is_postgres:
                conn.execute(text('''
                    CREATE TABLE IF NOT EXISTS entities (
                        id TEXT PRIMARY KEY,
                        entity_type TEXT,
                        data JSONB
                    )
                '''))
            else:
                conn.execute(text('''
                    CREATE TABLE IF NOT EXISTS entities (
                        id TEXT PRIMARY KEY,
                        entity_type TEXT,
                        data TEXT
                    )
                '''))
            
            # Insert data from backup
            for entity in data:
                upsert_sql = text('''
                    INSERT INTO entities (id, entity_type, data)
                    VALUES (:id, :etype, :data)
                    ON CONFLICT(id) DO UPDATE
                        SET entity_type = EXCLUDED.entity_type,
                            data = EXCLUDED.data
                ''')
                
                # For PostgreSQL with JSONB, we can pass the data directly
                # For other databases, we need to serialize to JSON string
                json_data = entity['data'] if is_postgres else json.dumps(entity['data'])
                
                conn.execute(
                    upsert_sql,
                    {
                        "id": entity['id'],
                        "etype": entity['entity_type'],
                        "data": json_data
                    }
                )
            
            conn.commit()
            logging.info(f"Restored {len(data)} entities from backup")
            return True
            
    except Exception as e:
        logging.error(f"Restore failed: {e}")
        return False

def main():
    """Main migration function."""
    logging.info("Starting database migration...")
    
    # Backup data first
    backup_file = backup_data()
    
    # Attempt migration
    success = migrate_to_jsonb()
    
    if not success:
        logging.warning("Migration failed, attempting to restore from backup...")
        restore_success = restore_from_backup(backup_file)
        
        if restore_success:
            logging.info("Restore completed successfully.")
        else:
            logging.error("Restore failed. Please check the database manually.")
    
    logging.info("Migration process completed.")

if __name__ == "__main__":
    main()