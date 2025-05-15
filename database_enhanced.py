#!/usr/bin/env python3

"""
Enhanced Database connection module for the application.
Provides functions and classes to connect to different types of databases, 
manage connection pools, and execute queries.

Features:
- Support for MySQL, PostgreSQL, SQLite, and MongoDB
- Connection pooling for improved performance
- Object-oriented interface with Database class
- Asynchronous database operations
"""

import os
import logging
import time
from typing import Dict, List, Any, Optional, Union, Tuple, Callable
from functools import wraps
from contextlib import contextmanager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_POOL_SIZE = 5
DEFAULT_POOL_TIMEOUT = 30  # seconds
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# Database configuration functions
def get_db_config(db_type: str = 'default') -> Dict[str, str]:
    """
    Get database configuration from environment variables.
    
    Args:
        db_type (str): Type of database ('default', 'mysql', 'postgresql', 'sqlite', 'mongodb')
        
    Returns:
        Dict[str, str]: Database configuration parameters
    """
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
            'POOL_SIZE': int(os.environ.get('MYSQL_POOL_SIZE', DEFAULT_POOL_SIZE)),
            'POOL_TIMEOUT': int(os.environ.get('MYSQL_POOL_TIMEOUT', DEFAULT_POOL_TIMEOUT)),
        },
        'postgresql': {
            'DB_HOST': os.environ.get('PG_HOST', 'localhost'),
            'DB_PORT': os.environ.get('PG_PORT', '5432'),
            'DB_NAME': os.environ.get('PG_DB', 'mydb'),
            'DB_USER': os.environ.get('PG_USER', 'user'),
            'DB_PASSWORD': os.environ.get('PG_PASSWORD', 'password'),
            'POOL_SIZE': int(os.environ.get('PG_POOL_SIZE', DEFAULT_POOL_SIZE)),
            'POOL_TIMEOUT': int(os.environ.get('PG_POOL_TIMEOUT', DEFAULT_POOL_TIMEOUT)),
        },
        'sqlite': {
            'DB_PATH': os.environ.get('SQLITE_PATH', 'database.db'),
        },
        'mongodb': {
            'DB_HOST': os.environ.get('MONGO_HOST', 'localhost'),
            'DB_PORT': os.environ.get('MONGO_PORT', '27017'),
            'DB_NAME': os.environ.get('MONGO_DB', 'mydb'),
            'DB_USER': os.environ.get('MONGO_USER', ''),
            'DB_PASSWORD': os.environ.get('MONGO_PASSWORD', ''),
            'DB_AUTH_SOURCE': os.environ.get('MONGO_AUTH_SOURCE', 'admin'),
            'DB_URI': os.environ.get('MONGO_URI', ''),  # Full connection string if provided
            'POOL_SIZE': int(os.environ.get('MONGO_POOL_SIZE', DEFAULT_POOL_SIZE)),
        },
    }
    
    if db_type not in configs:
        logger.warning(f"Unknown database type: {db_type}. Using default configuration.")
        return configs['default']
    
    return configs[db_type]


# Utility decorator for retry logic
def retry_operation(max_attempts=RETRY_ATTEMPTS, delay=RETRY_DELAY):
    """
    Decorator to retry database operations if they fail.
    
    Args:
        max_attempts (int): Maximum number of retry attempts
        delay (int): Delay between retries in seconds
        
    Returns:
        Function: Decorated function with retry logic
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            last_exception = None
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    last_exception = e
                    
                    if attempts < max_attempts:
                        logger.warning(
                            f"Operation failed: {str(e)}. "
                            f"Retrying in {delay} seconds. "
                            f"Attempt {attempts}/{max_attempts}"
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"Operation failed after {max_attempts} attempts: {str(e)}")
            
            raise last_exception
        
        return wrapper
    
    return decorator


# Connection functions for different database types
@retry_operation()
def connect_mysql(config: Dict[str, str] = None) -> Any:
    """
    Connect to a MySQL database.
    
    Args:
        config (Dict[str, str], optional): Configuration parameters.
            If None, values will be loaded from environment variables.
            
    Returns:
        Connection: MySQL database connection object
    
    Raises:
        ImportError: If MySQL connector is not installed
        Exception: If connection fails
    """
    try:
        import mysql.connector
        from mysql.connector import Error
        from mysql.connector.pooling import MySQLConnectionPool
    except ImportError:
        logger.error("MySQL connector not installed. Run: pip install mysql-connector-python")
        raise ImportError("MySQL connector not installed. Run: pip install mysql-connector-python")
    
    if config is None:
        config = get_db_config('mysql')
    
    try:
        # Check if we're using connection pooling
        if 'POOL_SIZE' in config and int(config['POOL_SIZE']) > 0:
            pool_name = f"mysql_pool_{config['DB_NAME']}"
            dbconfig = {
                'host': config['DB_HOST'],
                'port': int(config['DB_PORT']),
                'database': config['DB_NAME'],
                'user': config['DB_USER'],
                'password': config['DB_PASSWORD'],
                'pool_size': int(config['POOL_SIZE']),
                'pool_reset_session': True,
            }
            
            pool = MySQLConnectionPool(pool_name=pool_name, pool_size=int(config['POOL_SIZE']), **dbconfig)
            logger.info(f"Created MySQL connection pool for database: {config['DB_NAME']}")
            return pool
        
        # Standard connection
        connection = mysql.connector.connect(
            host=config['DB_HOST'],
            port=int(config['DB_PORT']),
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


@retry_operation()
def connect_postgresql(config: Dict[str, str] = None) -> Any:
    """
    Connect to a PostgreSQL database.
    
    Args:
        config (Dict[str, str], optional): Configuration parameters.
            If None, values will be loaded from environment variables.
            
    Returns:
        Connection or ConnectionPool: PostgreSQL database connection object or pool
    
    Raises:
        ImportError: If psycopg2 is not installed
        Exception: If connection fails
    """
    try:
        import psycopg2
        from psycopg2 import pool
    except ImportError:
        logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")
    
    if config is None:
        config = get_db_config('postgresql')
    
    try:
        # Check if we're using connection pooling
        if 'POOL_SIZE' in config and int(config['POOL_SIZE']) > 0:
            connection_pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=int(config['POOL_SIZE']),
                host=config['DB_HOST'],
                port=config['DB_PORT'],
                database=config['DB_NAME'],
                user=config['DB_USER'],
                password=config['DB_PASSWORD']
            )
            
            logger.info(f"Created PostgreSQL connection pool for database: {config['DB_NAME']}")
            return connection_pool
        
        # Standard connection
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


@retry_operation()
def connect_sqlite(db_path: str = None) -> Any:
    """
    Connect to an SQLite database.
    
    Args:
        db_path (str, optional): Path to the SQLite database file.
            If None, the path will be loaded from environment variables.
            
    Returns:
        Connection: SQLite database connection object
    
    Raises:
        ImportError: If sqlite3 is not available
        Exception: If connection fails
    """
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
        
        # Enable dictionary cursor by default
        connection.row_factory = sqlite3.Row
        
        logger.info(f"Connected to SQLite database: {db_path}")
        return connection
            
    except sqlite3.Error as e:
        logger.error(f"Error connecting to SQLite database: {e}")
        raise Exception(f"Failed to connect to SQLite database: {e}")


@retry_operation()
def connect_mongodb(config: Dict[str, str] = None) -> Any:
    """
    Connect to a MongoDB database.
    
    Args:
        config (Dict[str, str], optional): Configuration parameters.
            If None, values will be loaded from environment variables.
            
    Returns:
        MongoClient: MongoDB client connection
    
    Raises:
        ImportError: If pymongo is not installed
        Exception: If connection fails
    """
    try:
        import pymongo
    except ImportError:
        logger.error("pymongo not installed. Run: pip install pymongo")
        raise ImportError("pymongo not installed. Run: pip install pymongo")
    
    if config is None:
        config = get_db_config('mongodb')
    
    try:
        # Use URI if provided, otherwise build connection string
        if config.get('DB_URI'):
            connection_string = config['DB_URI']
        else:
            auth_part = ""
            if config['DB_USER'] and config['DB_PASSWORD']:
                auth_part = f"{config['DB_USER']}:{config['DB_PASSWORD']}@"
            
            connection_string = f"mongodb://{auth_part}{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}"
            
            # Add auth source if credentials are provided
            if config['DB_USER'] and config['DB_PASSWORD']:
                connection_string += f"?authSource={config['DB_AUTH_SOURCE']}"
        
        # Connect with connection pooling
        client = pymongo.MongoClient(
            connection_string,
            maxPoolSize=int(config.get('POOL_SIZE', DEFAULT_POOL_SIZE))
        )
        
        # Verify connection is working by accessing server info
        client.server_info()
        
        logger.info(f"Connected to MongoDB database: {config['DB_NAME']}")
        return client
            
    except pymongo.errors.ConnectionFailure as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise Exception(f"Failed to connect to MongoDB: {e}")
    except pymongo.errors.ConfigurationError as e:
        logger.error(f"MongoDB configuration error: {e}")
        raise Exception(f"MongoDB configuration error: {e}")


# Context managers for database connections
@contextmanager
def mysql_connection(config: Dict[str, str] = None):
    """
    Context manager for MySQL database connections.
    
    Args:
        config (Dict[str, str], optional): Configuration parameters.
        
    Yields:
        Connection: MySQL database connection
    """
    connection = None
    try:
        connection = connect_mysql(config)
        yield connection
    finally:
        if connection and hasattr(connection, 'close'):
            connection.close()
            logger.info("MySQL connection closed")


@contextmanager
def postgresql_connection(config: Dict[str, str] = None):
    """
    Context manager for PostgreSQL database connections.
    
    Args:
        config (Dict[str, str], optional): Configuration parameters.
        
    Yields:
        Connection: PostgreSQL database connection
    """
    connection = None
    try:
        pool = connect_postgresql(config)
        
        # Check if we got a connection pool
        if hasattr(pool, 'getconn'):
            connection = pool.getconn()
            yield connection
        else:
            # We got a direct connection
            connection = pool
            yield connection
    finally:
        if connection:
            # If we're using a pool, return the connection to the pool
            if hasattr(pool, 'putconn'):
                pool.putconn(connection)
                logger.info("Returned PostgreSQL connection to pool")
            # Otherwise, close the connection
            elif hasattr(connection, 'close'):
                connection.close()
                logger.info("PostgreSQL connection closed")


@contextmanager
def sqlite_connection(db_path: str = None):
    """
    Context manager for SQLite database connections.
    
    Args:
        db_path (str, optional): Path to the SQLite database file.
        
    Yields:
        Connection: SQLite database connection
    """
    connection = None
    try:
        connection = connect_sqlite(db_path)
        yield connection
    finally:
        if connection:
            connection.close()
            logger.info("SQLite connection closed")


@contextmanager
def mongodb_connection(config: Dict[str, str] = None):
    """
    Context manager for MongoDB database connections.
    
    Args:
        config (Dict[str, str], optional): Configuration parameters.
        
    Yields:
        MongoClient: MongoDB client connection
    """
    client = None
    try:
        client = connect_mongodb(config)
        yield client
    finally:
        if client:
            client.close()
            logger.info("MongoDB connection closed")


# Database operation functions
def execute_query(
    connection: Any, 
    query: str, 
    params: Union[List[Any], Dict[str, Any], Tuple[Any, ...], None] = None,
    fetch_one: bool = False
) -> Union[List[Dict[str, Any]], Dict[str, Any], None]:
    """
    Execute a SQL query and return the results.
    
    Args:
        connection: Database connection object
        query (str): SQL query to execute
        params (Union[List, Dict, Tuple, None]): Parameters for the query
        fetch_one (bool): If True, fetch only one result
        
    Returns:
        Union[List[Dict[str, Any]], Dict[str, Any], None]: Query results
        
    Raises:
        Exception: If query execution fails
    """
    cursor = None
    try:
        if hasattr(connection, 'cursor'):
            cursor = connection.cursor(dictionary=True) if hasattr(connection, 'cursor') else connection.cursor()
        else:
            # For SQLite with row_factory
            cursor = connection.cursor()
            
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        if query.strip().upper().startswith(('SELECT', 'SHOW')):
            if fetch_one:
                result = cursor.fetchone()
                # Convert SQLite Row to dict if needed
                if result and hasattr(result, 'keys'):
                    return dict(result)
                return result
            else:
                results = cursor.fetchall()
                # Convert SQLite Row objects to dict if needed
                if results and hasattr(results[0], 'keys'):
                    return [dict(row) for row in results]
                return results
        else:
            connection.commit()
            return {'affected_rows': cursor.rowcount}
        
    except Exception as e:
        if hasattr(connection, 'rollback'):
            connection.rollback()
        logger.error(f"Error executing query: {e}")
        raise Exception(f"Failed to execute query: {e}")
        
    finally:
        if cursor:
            cursor.close()


def close_connection(connection: Any) -> None:
    """
    Close a database connection.
    
    Args:
        connection: Database connection object to close
    """
    if connection:
        if hasattr(connection, 'close'):
            connection.close()
            logger.info("Database connection closed")
        elif hasattr(connection, 'dispose'):  # For some pool implementations
            connection.dispose()
            logger.info("Database connection pool disposed")


# Database class for OOP approach
class Database:
    """
    Database class for object-oriented database interaction.
    """
    
    def __init__(self, db_type: str = 'default', config: Dict[str, str] = None):
        """
        Initialize a Database object.
        
        Args:
            db_type (str): Type of database ('mysql', 'postgresql', 'sqlite', 'mongodb')
            config (Dict[str, str], optional): Configuration parameters
        """
        self.db_type = db_type
        self.config = config or get_db_config(db_type)
        self.connection = None
        self._connect()
    
    def _connect(self):
        """
        Establish a connection to the database.
        
        Raises:
            Exception: If connection fails
        """
        if self.db_type == 'mysql':
            self.connection = connect_mysql(self.config)
        elif self.db_type == 'postgresql':
            self.connection = connect_postgresql(self.config)
        elif self.db_type == 'sqlite':
            self.connection = connect_sqlite(self.config.get('DB_PATH') if self.config else None)
        elif self.db_type == 'mongodb':
            self.connection = connect_mongodb(self.config)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def execute(self, query: str, params: Any = None, fetch_one: bool = False) -> Any:
        """
        Execute a SQL query.
        
        Args:
            query (str): SQL query to execute
            params (Any, optional): Parameters for the query
            fetch_one (bool): If True, fetch only one result
            
        Returns:
            Any: Query results
            
        Raises:
            Exception: If query execution fails
        """
        if not self.connection:
            self._connect()
            
        # For MongoDB we need special handling
        if self.db_type == 'mongodb':
            raise ValueError("For MongoDB, use the mongodb_* methods instead of execute")
            
        return execute_query(self.connection, query, params, fetch_one)
    
    def mongodb_insert_one(self, collection_name: str, document: Dict[str, Any]) -> str:
        """
        Insert a single document into a MongoDB collection.
        
        Args:
            collection_name (str): Name of the collection
            document (Dict[str, Any]): Document to insert
            
        Returns:
            str: ID of the inserted document
            
        Raises:
            Exception: If insert fails
        """
        if self.db_type != 'mongodb':
            raise ValueError("This method is only available for MongoDB connections")
            
        db = self.connection[self.config['DB_NAME']]
        collection = db[collection_name]
        
        result = collection.insert_one(document)
        return str(result.inserted_id)
    
    def mongodb_find(
        self, 
        collection_name: str, 
        query: Dict[str, Any] = None, 
        projection: Dict[str, Any] = None,
        limit: int = 0,
        skip: int = 0,
        sort: List[Tuple[str, int]] = None
    ) -> List[Dict[str, Any]]:
        """
        Find documents in a MongoDB collection.
        
        Args:
            collection_name (str): Name of the collection
            query (Dict[str, Any], optional): Query filter
            projection (Dict[str, Any], optional): Projection specification
            limit (int, optional): Maximum number of documents to return
            skip (int, optional): Number of documents to skip
            sort (List[Tuple[str, int]], optional): Sort specification
            
        Returns:
            List[Dict[str, Any]]: Matching documents
            
        Raises:
            Exception: If find fails
        """
        if self.db_type != 'mongodb':
            raise ValueError("This method is only available for MongoDB connections")
            
        db = self.connection[self.config['DB_NAME']]
        collection = db[collection_name]
        
        cursor = collection.find(query or {}, projection or {})
        
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        if sort:
            cursor = cursor.sort(sort)
            
        return list(cursor)
    
    def close(self):
        """Close the database connection."""
        close_connection(self.connection)
        self.connection = None
    
    def __enter__(self):
        """Support for context manager protocol."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection when exiting context."""
        self.close()


# Example usage
if __name__ == "__main__":
    # Example: Using the Database class with SQLite
    try:
        # Create an in-memory SQLite database for testing
        with Database(db_type='sqlite', config={'DB_PATH': ':memory:'}) as db:
            # Create a sample table
            create_table_query = """
            CREATE TABLE