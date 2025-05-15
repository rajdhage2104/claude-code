#!/usr/bin/env python3

"""
Database connection pool module for the application.
Extends the basic database functionality with connection pooling capabilities.
"""

import os
import logging
import time
from typing import Dict, Any, Optional, Union
from contextlib import contextmanager

# Import base database module
from database import get_db_config, close_connection

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Connection pool settings
DEFAULT_POOL_SIZE = 5
DEFAULT_MAX_OVERFLOW = 10
DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 2  # seconds

class DatabaseConnectionError(Exception):
    """Exception raised for database connection errors."""
    pass

class ConnectionPool:
    """Base connection pool class to be inherited by specific database implementations."""
    
    def __init__(
        self,
        db_type: str,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        timeout: int = DEFAULT_TIMEOUT,
        **kwargs
    ):
        """
        Initialize the connection pool.
        
        Args:
            db_type (str): Type of database ('mysql', 'postgresql', 'sqlite')
            pool_size (int): Initial size of the connection pool
            max_overflow (int): Maximum number of connections above pool_size
            timeout (int): Connection timeout in seconds
            **kwargs: Additional parameters specific to the database type
        """
        self.db_type = db_type
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.timeout = timeout
        self.config = get_db_config(db_type)
        self.config.update(kwargs)
        
        # Will be initialized by child classes
        self.pool = None
        self.active_connections = 0
        self.total_connections = 0
        
        logger.info(f"Initializing {db_type} connection pool with size {pool_size}")
    
    def _create_connection(self) -> Any:
        """Create a new database connection (to be implemented by subclasses)."""
        raise NotImplementedError("Subclasses must implement _create_connection method")
    
    def get_connection(self) -> Any:
        """Get a connection from the pool (to be implemented by subclasses)."""
        raise NotImplementedError("Subclasses must implement get_connection method")
    
    def release_connection(self, connection: Any) -> None:
        """Return a connection to the pool (to be implemented by subclasses)."""
        raise NotImplementedError("Subclasses must implement release_connection method")
    
    def close_all(self) -> None:
        """Close all connections in the pool (to be implemented by subclasses)."""
        raise NotImplementedError("Subclasses must implement close_all method")
    
    @contextmanager
    def connection(self):
        """
        Context manager for database connections.
        
        Usage:
            with pool.connection() as conn:
                # Use the connection
        """
        conn = None
        try:
            conn = self.get_connection()
            yield conn
        finally:
            if conn is not None:
                self.release_connection(conn)


class MySQLConnectionPool(ConnectionPool):
    """MySQL database connection pool."""
    
    def __init__(self, **kwargs):
        """Initialize MySQL connection pool."""
        super().__init__('mysql', **kwargs)
        
        try:
            import mysql.connector
            from mysql.connector import pooling
        except ImportError:
            logger.error("MySQL connector not installed. Run: pip install mysql-connector-python")
            raise ImportError("MySQL connector not installed. Run: pip install mysql-connector-python")
        
        try:
            # Create a connection pool
            self.pool = pooling.MySQLConnectionPool(
                pool_name="mysql_pool",
                pool_size=self.pool_size,
                pool_reset_session=True,
                host=self.config['DB_HOST'],
                port=self.config['DB_PORT'],
                database=self.config['DB_NAME'],
                user=self.config['DB_USER'],
                password=self.config['DB_PASSWORD'],
                **kwargs
            )
            logger.info(f"MySQL connection pool created with {self.pool_size} connections")
        except Exception as e:
            logger.error(f"Error creating MySQL connection pool: {e}")
            raise DatabaseConnectionError(f"Failed to create MySQL connection pool: {e}")
    
    def get_connection(self) -> Any:
        """
        Get a connection from the MySQL pool with retry logic.
        
        Returns:
            Connection: MySQL database connection
            
        Raises:
            DatabaseConnectionError: If unable to get a connection after retries
        """
        for attempt in range(DEFAULT_RETRY_ATTEMPTS):
            try:
                connection = self.pool.get_connection()
                if connection.is_connected():
                    logger.debug("Acquired connection from MySQL pool")
                    self.active_connections += 1
                    return connection
            except Exception as e:
                logger.warning(f"Connection attempt {attempt+1} failed: {e}")
                if attempt < DEFAULT_RETRY_ATTEMPTS - 1:
                    time.sleep(DEFAULT_RETRY_DELAY)
                else:
                    logger.error("Failed to get MySQL connection after multiple attempts")
                    raise DatabaseConnectionError(f"Failed to get MySQL connection: {e}")
    
    def release_connection(self, connection: Any) -> None:
        """
        Return a connection to the MySQL pool.
        
        Args:
            connection: MySQL connection to return to the pool
        """
        if connection:
            connection.close()
            self.active_connections -= 1
            logger.debug("Released connection back to MySQL pool")
    
    def close_all(self) -> None:
        """Close all connections in the MySQL pool."""
        # MySQL Connector/Python automatically closes connections when pool is garbage collected
        logger.info("Closing all MySQL connections in the pool")
        self.active_connections = 0


class PostgreSQLConnectionPool(ConnectionPool):
    """PostgreSQL database connection pool."""
    
    def __init__(self, **kwargs):
        """Initialize PostgreSQL connection pool."""
        super().__init__('postgresql', **kwargs)
        
        try:
            import psycopg2
            from psycopg2 import pool
        except ImportError:
            logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
            raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")
        
        try:
            # Create a thread-safe connection pool
            self.pool = pool.ThreadedConnectionPool(
                minconn=self.pool_size,
                maxconn=self.pool_size + self.max_overflow,
                host=self.config['DB_HOST'],
                port=self.config['DB_PORT'],
                dbname=self.config['DB_NAME'],
                user=self.config['DB_USER'],
                password=self.config['DB_PASSWORD'],
                **kwargs
            )
            logger.info(f"PostgreSQL connection pool created with {self.pool_size} min connections")
        except Exception as e:
            logger.error(f"Error creating PostgreSQL connection pool: {e}")
            raise DatabaseConnectionError(f"Failed to create PostgreSQL connection pool: {e}")
    
    def get_connection(self) -> Any:
        """
        Get a connection from the PostgreSQL pool with retry logic.
        
        Returns:
            Connection: PostgreSQL database connection
            
        Raises:
            DatabaseConnectionError: If unable to get a connection after retries
        """
        for attempt in range(DEFAULT_RETRY_ATTEMPTS):
            try:
                connection = self.pool.getconn()
                logger.debug("Acquired connection from PostgreSQL pool")
                self.active_connections += 1
                return connection
            except Exception as e:
                logger.warning(f"Connection attempt {attempt+1} failed: {e}")
                if attempt < DEFAULT_RETRY_ATTEMPTS - 1:
                    time.sleep(DEFAULT_RETRY_DELAY)
                else:
                    logger.error("Failed to get PostgreSQL connection after multiple attempts")
                    raise DatabaseConnectionError(f"Failed to get PostgreSQL connection: {e}")
    
    def release_connection(self, connection: Any) -> None:
        """
        Return a connection to the PostgreSQL pool.
        
        Args:
            connection: PostgreSQL connection to return to the pool
        """
        if connection:
            self.pool.putconn(connection)
            self.active_connections -= 1
            logger.debug("Released connection back to PostgreSQL pool")
    
    def close_all(self) -> None:
        """Close all connections in the PostgreSQL pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Closed all PostgreSQL connections in the pool")
            self.active_connections = 0


class SQLiteConnectionPool(ConnectionPool):
    """
    SQLite connection pool implementation.
    
    Note: SQLite is designed for single-user access, so this pool
    simulates a connection pool for consistency with other databases.
    """
    
    def __init__(self, **kwargs):
        """Initialize SQLite connection pool."""
        super().__init__('sqlite', **kwargs)
        
        try:
            import sqlite3
        except ImportError:
            logger.error("sqlite3 module not available")
            raise ImportError("sqlite3 module not available")
        
        # SQLite connections pool is simulated
        self.pool = []
        self.db_path = self.config.get('DB_PATH', 'database.db')
        
        # Initialize the pool with connections
        for _ in range(self.pool_size):
            try:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                conn.row_factory = sqlite3.Row  # Return rows as dictionaries
                self.pool.append(conn)
                self.total_connections += 1
            except sqlite3.Error as e:
                logger.error(f"Error creating SQLite connection: {e}")
        
        logger.info(f"SQLite connection pool initialized with {len(self.pool)} connections")
    
    def _create_connection(self) -> Any:
        """
        Create a new SQLite connection.
        
        Returns:
            Connection: SQLite database connection
        """
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            self.total_connections += 1
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error creating SQLite connection: {e}")
            raise DatabaseConnectionError(f"Failed to create SQLite connection: {e}")
    
    def get_connection(self) -> Any:
        """
        Get a connection from the SQLite pool.
        
        Returns:
            Connection: SQLite database connection
            
        Raises:
            DatabaseConnectionError: If unable to get a connection
        """
        # Try to get a connection from the pool
        if self.pool:
            connection = self.pool.pop()
            self.active_connections += 1
            logger.debug("Acquired connection from SQLite pool")
            return connection
        
        # If the pool is empty but we haven't reached max_connections, create a new one
        if self.total_connections < self.pool_size + self.max_overflow:
            connection = self._create_connection()
            self.active_connections += 1
            logger.debug("Created new SQLite connection (pool empty)")
            return connection
        
        # If we've reached the limit, wait for a connection to become available
        logger.warning("SQLite connection pool exhausted, waiting for available connection")
        raise DatabaseConnectionError("Connection pool exhausted")
    
    def release_connection(self, connection: Any) -> None:
        """
        Return a connection to the SQLite pool.
        
        Args:
            connection: SQLite connection to return to the pool
        """
        if connection:
            # If we have too many connections, close this one
            if len(self.pool) >= self.pool_size:
                connection.close()
                self.total_connections -= 1
                logger.debug("Closed excess SQLite connection")
            else:
                # Otherwise return to the pool
                self.pool.append(connection)
                logger.debug("Released connection back to SQLite pool")
            
            self.active_connections -= 1
    
    def close_all(self) -> None:
        """Close all connections in the SQLite pool."""
        for conn in self.pool:
            conn.close()
        
        self.pool = []
        self.total_connections = 0
        self.active_connections = 0
        logger.info("Closed all SQLite connections in the pool")


# Factory function to create the appropriate connection pool
def create_connection_pool(
    db_type: str = 'sqlite',
    pool_size: int = DEFAULT_POOL_SIZE,
    max_overflow: int = DEFAULT_MAX_OVERFLOW,
    timeout: int = DEFAULT_TIMEOUT,
    **kwargs
) -> ConnectionPool:
    """
    Create a database connection pool for the specified database type.
    
    Args:
        db_type (str): Type of database ('mysql', 'postgresql', 'sqlite')
        pool_size (int): Initial size of the connection pool
        max_overflow (int): Maximum number of connections above pool_size
        timeout (int): Connection timeout in seconds
        **kwargs: Additional parameters specific to the database type
        
    Returns:
        ConnectionPool: Database connection pool
        
    Raises:
        ValueError: If an unsupported database type is specified
    """
    if db_type == 'mysql':
        return MySQLConnectionPool(
            pool_size=pool_size,
            max_overflow=max_overflow,
            timeout=timeout,
            **kwargs
        )
    elif db_type == 'postgresql':
        return PostgreSQLConnectionPool(
            pool_size=pool_size,
            max_overflow=max_overflow,
            timeout=timeout,
            **kwargs
        )
    elif db_type == 'sqlite':
        return SQLiteConnectionPool(
            pool_size=pool_size,
            max_overflow=max_overflow,
            timeout=timeout,
            **kwargs
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


# Example usage
if __name__ == "__main__":
    # Example with SQLite (safest to run without configuration)
    try:
        # Create an in-memory SQLite connection pool
        pool = create_connection_pool(
            db_type='sqlite',
            pool_size=3,
            DB_PATH=":memory:"
        )
        
        # Use a connection from the pool
        with pool.connection() as conn:
            cursor = conn.cursor()
            
            # Create a sample table
            cursor.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Insert sample data
            cursor.execute(
                "INSERT INTO users (name, email) VALUES (?, ?)",
                ("John Doe", "john@example.com")
            )
            cursor.execute(
                "INSERT INTO users (name, email) VALUES (?, ?)",
                ("Jane Smith", "jane@example.com")
            )
            
            # Commit the changes
            conn.commit()
            
            # Query the data
            cursor.execute("SELECT * FROM users")
            results = cursor.fetchall()
            
            print("Sample query results:")
            for row in results:
                print(f"User: {row['name']}, Email: {row['email']}")
        
        # Close all connections in the pool
        pool.close_all()
        
    except Exception as e:
        print(f"An error occurred: {e}")
