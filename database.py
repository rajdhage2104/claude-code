#!/usr/bin/env python3

\"\"\"
Database connection module for the application.
Provides functions to connect to different types of databases
and execute queries.
\"\"\"

import os
import logging
from typing import Dict, List, Any, Optional, Union, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection functions
def get_db_config(db_type: str = 'default') -> Dict[str, str]:
    \"\"\"
    Get database configuration from environment variables.
    
    Args:
        db_type (str): Type of database ('default', 'mysql', 'postgresql', 'sqlite')
        
    Returns:
        Dict[str, str]: Database configuration parameters
    \"\"\"
    configs = {
        'default': {
            'DB_HOST': os.environ.get('DB_HOST', 'localhost'),
            'DB_PORT': os.environ.get('DB_PORT', '3306'),
            'DB_NAME': os.environ.get('DB_NAME', 'mydb'),
            'DB_USER': os.environ.get('DB_USER', 'user'),
            'DB_PASSWORD': os.environ.get('DB_PASSWORD', 'password'),
        },
        'mysql': {
            'DB_HOST': os.environ.get('MYSQL_HOST', 'localhost'),
            'DB_PORT': os.environ.get('MYSQL_PORT', '3306'),
            'DB_NAME': os.environ.get('MYSQL_DB', 'mydb'),
            'DB_USER': os.environ.get('MYSQL_USER', 'user'),
            'DB_PASSWORD': os.environ.get('MYSQL_PASSWORD', 'password'),
        },
        'postgresql': {
            'DB_HOST': os.environ.get('PG_HOST', 'localhost'),
            'DB_PORT': os.environ.get('PG_PORT', '5432'),
            'DB_NAME': os.environ.get('PG_DB', 'mydb'),
            'DB_USER': os.environ.get('PG_USER', 'user'),
            'DB_PASSWORD': os.environ.get('PG_PASSWORD', 'password'),
        },
        'sqlite': {
            'DB_PATH': os.environ.get('SQLITE_PATH', 'database.db'),
        },
    }
    
    if db_type not in configs:
        logger.warning(f"Unknown database type: {db_type}. Using default configuration.")
        return configs['default']
    
    return configs[db_type]


def connect_mysql(config: Dict[str, str] = None) -> Any:
    \"\"\"
    Connect to a MySQL database.
    
    Args:
        config (Dict[str, str], optional): Configuration parameters.
            If None, values will be loaded from environment variables.
            
    Returns:
        Connection: MySQL database connection object
    
    Raises:
        ImportError: If MySQL connector is not installed
        Exception: If connection fails
    \"\"\"
    try:
        import mysql.connector
        from mysql.connector import Error
    except ImportError:
        logger.error("MySQL connector not installed. Run: pip install mysql-connector-python")
        raise ImportError("MySQL connector not installed. Run: pip install mysql-connector-python")
    
    if config is None:
        config = get_db_config('mysql')
    
    try:
        connection = mysql.connector.connect(
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            database=config['DB_NAME'],
            user=config['DB_USER'],
            password=config['DB_PASSWORD']
        )
        
        if connection.is_connected():
            logger.info(f"Connected to MySQL database: {config['DB_NAME']}")
            return connection
            
    except Error as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        raise Exception(f"Failed to connect to MySQL database: {e}")


def connect_postgresql(config: Dict[str, str] = None) -> Any:
    \"\"\"
    Connect to a PostgreSQL database.
    
    Args:
        config (Dict[str, str], optional): Configuration parameters.
            If None, values will be loaded from environment variables.
            
    Returns:
        Connection: PostgreSQL database connection object
    
    Raises:
        ImportError: If psycopg2 is not installed
        Exception: If connection fails
    \"\"\"
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")
    
    if config is None:
        config = get_db_config('postgresql')
    
    try:
        connection = psycopg2.connect(
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            database=config['DB_NAME'],
            user=config['DB_USER'],
            password=config['DB_PASSWORD']
        )
        
        logger.info(f"Connected to PostgreSQL database: {config['DB_NAME']}")
        return connection
            
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise Exception(f"Failed to connect to PostgreSQL database: {e}")


def connect_sqlite(db_path: str = None) -> Any:
    \"\"\"
    Connect to an SQLite database.
    
    Args:
        db_path (str, optional): Path to the SQLite database file.
            If None, the path will be loaded from environment variables.
            
    Returns:
        Connection: SQLite database connection object
    
    Raises:
        ImportError: If sqlite3 is not available
        Exception: If connection fails
    \"\"\"
    try:
        import sqlite3
    except ImportError:
        logger.error("sqlite3 module not available")
        raise ImportError("sqlite3 module not available")
    
    if db_path is None:
        config = get_db_config('sqlite')
        db_path = config['DB_PATH']
    
    try:
        connection = sqlite3.connect(db_path)
        logger.info(f"Connected to SQLite database: {db_path}")
        return connection
            
    except sqlite3.Error as e:
        logger.error(f"Error connecting to SQLite database: {e}")
        raise Exception(f"Failed to connect to SQLite database: {e}")


def execute_query(
    connection: Any, 
    query: str, 
    params: Union[List[Any], Dict[str, Any], Tuple[Any, ...], None] = None
) -> List[Dict[str, Any]]:
    \"\"\"
    Execute a SQL query and return the results.
    
    Args:
        connection: Database connection object
        query (str): SQL query to execute
        params (Union[List, Dict, Tuple, None]): Parameters for the query
        
    Returns:
        List[Dict[str, Any]]: Query results as a list of dictionaries
        
    Raises:
        Exception: If query execution fails
    \"\"\"
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        results = cursor.fetchall()
        return results
        
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise Exception(f"Failed to execute query: {e}")
        
    finally:
        if cursor:
            cursor.close()


def close_connection(connection: Any) -> None:
    \"\"\"
    Close a database connection.
    
    Args:
        connection: Database connection object to close
    \"\"\"
    if connection:
        connection.close()
        logger.info("Database connection closed")


# Example usage
if __name__ == \"__main__\":
    # Example: Connect to SQLite database
    try:
        # Create an in-memory SQLite database for testing
        conn = connect_sqlite(\":memory:\")
        
        # Create a sample table
        create_table_query = \"\"\"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        \"\"\"
        execute_query(conn, create_table_query)
        
        # Insert sample data
        insert_query = \"INSERT INTO users (name, email) VALUES (?, ?)\"
        execute_query(conn, insert_query, (\"John Doe\", \"john@example.com\"))
        execute_query(conn, insert_query, (\"Jane Smith\", \"jane@example.com\"))
        
        # Query the data
        select_query = \"SELECT * FROM users\"
        results = execute_query(conn, select_query)
        
        print(\"Sample query results:\")
        for row in results:
            print(f\"User: {row['name']}, Email: {row['email']}\")
            
        # Close the connection
        close_connection(conn)
        
    except Exception as e:
        print(f\"An error occurred: {e}\")
