import sqlite3
import pandas as pd
import datetime

class DatabaseManager:
    """
    A class for managing a SQLite database connection and performing database operations.

    Attributes:
        db_name (str): Name of the SQLite database.
        conn (sqlite3.Connection): SQLite database connection object.

    Example:
        db_manager = DatabaseManager("mydatabase.db")
        db_manager.connect()
        db_manager.create_table("mytable", [("id", "INTEGER"), ("name", "TEXT")])
    """

    def __init__(self, db_name):
        """
        Initializes a DatabaseManager object.

        Args:
            db_name (str): Name of the SQLite database file.

        Attributes:
            db_name (str): Name of the SQLite database.
            conn (sqlite3.Connection): SQLite database connection object.
        """
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        """
        Connects to the SQLite database.

        Returns:
            None
        """
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, columns):
        """
        Creates a table in the SQLite database if it doesn't already exist.

        Args:
            table_name (str): Name of the table to create.
            columns (list): List of column specifications for the table. Each column specification should be a tuple with two elements: the column name and the column type.

        Returns:
            None

        Raises:
            sqlite3.Error: If an error occurs while executing the SQL query.
        """
        try:
            # Construct the SQL query for table creation
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{column[0]} {column[1]}' for column in columns])})"
            # Execute the query
            self.cursor.execute(query)
        except sqlite3.Error as e:
            # Handle the error
            print(f"Error creating table: {e}")
            # Optionally, you can raise the exception to propagate it to the caller
            raise e
        
    def table_exists(self, table_name):
        """
        Checks if a table exists in the SQLite database.

        Args:
            table_name (str): Name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.

        Raises:
            sqlite3.Error: If an error occurs while executing the SQL query.
        """
        try:
            query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            result = self.conn.execute(query).fetchone()
            return result is not None
        except sqlite3.Error as e:
            # Handle the error
            print(f"Error checking table existence: {e}")
            # Optionally, you can raise the exception to propagate it to the caller
            raise e
        
    def drop_table(self, table_name):
        """
        Drops the specified table from the database.

        Args:
            table_name (str): Name of the table to drop.

        Returns:
            None

        Raises:
            sqlite3.Error: If an error occurs while executing the SQL query.
        """
        try:
            query = f"DROP TABLE IF EXISTS {table_name}"
            self.conn.execute(query)
            self.conn.commit()
        except sqlite3.Error as e:
            # Handle the error
            print(f"Error dropping table: {e}")
            # Optionally, you can raise the exception to propagate it to the caller
            raise e

    def get_newest_date(self, table_name):
        """
        Retrieves the date of the last added or newest item in the specified table.

        Args:
            table_name (str): Name of the table to query.

        Returns:
            str: Date of the last added or newest item in the table.

        Raises:
            sqlite3.Error: If an error occurs while executing the SQL query.
        """
        try:
            query = f"SELECT MAX(date) FROM {table_name}"
            result = self.conn.execute(query).fetchone()
            newest_date_str = result[0] if result[0] else None

            if newest_date_str:
                newest_date = datetime.datetime.strptime(newest_date_str, "%Y-%m-%d %H:%M:%S")
            else:
                newest_date = None

            return newest_date
        except sqlite3.Error as e:
            # Handle the error
            print(f"Error retrieving newest date: {e}")
            # Optionally, you can raise the exception to propagate it to the caller
            raise e
        
    def fetch_dataframe(self, table_name):
        """
        Fetches data from the specified table and returns it as a DataFrame.

        Args:
            table_name (str): Name of the table.

        Returns:
            pandas.DataFrame: DataFrame containing the data from the table.
        """
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.conn)
        return df
        
    def insert_dataframe(self, table_name, dataframe):
        columns = dataframe.columns.tolist()
        values = dataframe.to_numpy()
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
        try:
            self.cursor.executemany(query, values)
            last_row_id = self.cursor.lastrowid
            self.conn.commit()
            return last_row_id
        except sqlite3.Error as e:
            self.conn.rollback()
            raise e
        
    def should_fetch_data(self, table_name, max_age_hours=24):
        """
        Determines if data should be fetched from the API based on the presence of data in the specified table
        and the last update date being older than the specified maximum age.

        Args:
            table_name (str): Name of the table to check.
            max_age_hours (int): Maximum age in hours for the data to be considered outdated. Defaults to 24 hours.

        Returns:
            bool: True if data should be fetched, False otherwise.

        Raises:
            sqlite3.Error: If an error occurs while executing the SQL query.
        """
        try:
            # Check if the table is empty
            query = f"SELECT COUNT(*) FROM {table_name}"
            result = self.conn.execute(query).fetchone()
            if result[0] == 0:
                return True

            # Check the last update date
            max_age = datetime.timedelta(hours=max_age_hours)
            last_update_date = self.get_newest_date(table_name)
            if last_update_date is None or (datetime.datetime.now() - last_update_date) > max_age:
                return True

            return False
        except sqlite3.Error as e:
            # Handle the error
            print(f"Error checking data in table: {e}")
            # Optionally, you can raise the exception to propagate it to the caller
            raise e