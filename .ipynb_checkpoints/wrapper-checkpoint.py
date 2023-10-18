"""
    This contains code for MSSQL, AS400 and Oracle sql database sql commands wrappers
"""
import datetime
import decimal
import uuid
import logging
import six

from apache_beam.utils import retry

try:
    from apitools.base.py.exceptions import HttpError
except ImportError:
    pass

import apache_beam as beam
from .exceptions import ExceptionNoColumns, ExceptionBadRow, ExceptionInvalidWrapper

MAX_RETRIES = 5
DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f UTC'
READ_BATCH = 500000

class BaseWrapper(object):
    """
    This eventually need to be replaced with integration with SQLAlchemy
    """

    def __init__(self, connection):
        self.connection = connection
        self._unique_row_id = 0
        # For testing scenarios where we pass in a client we do not want a
        # randomized prefix for row IDs.
        self._row_id_prefix = '' if connection else uuid.uuid4()
        self._temporary_table_suffix = uuid.uuid4().hex

    def escape_name(self, name):
        """Escape name to avoid SQL injection and keyword clashes.
        Doubles embedded backticks, surrounds the whole in backticks.
        Note: not security hardened, caveat emptor.

        """
        return '`{}`'.format(name.replace('`', '``'))

    @property
    def unique_row_id(self):
        """Returns a unique row ID (str) used to avoid multiple insertions.

        If the row ID is provided, we will make a best effort to not insert
        the same row multiple times for fail and retry scenarios in which the insert
        request may be issued several times. This comes into play for sinks executed
        in a local runner.

        Returns:
          a unique row ID string
        """
        self._unique_row_id += 1
        return '%s_%d' % (self._row_id_prefix, self._unique_row_id)

    def _convert_cell_value_to_dict(self, value, field):
        if field.type == 'STRING':
            # Input: "XYZ" --> Output: "XYZ"
            return value
        elif field.type == 'BOOLEAN':
            # Input: "true" --> Output: True
            return value in ['true', 1, '1', 'True']
        elif field.type == 'INTEGER':
            # Input: "123" --> Output: 123
            return int(value)
        elif field.type == 'FLOAT':
            # Input: "1.23" --> Output: 1.23
            return float(value)
        elif field.type == 'TIMESTAMP':
            # The UTC should come from the timezone library but this is a known
            # issue in python 2.7 so we'll just hardcode it as we're reading using
            # utcfromtimestamp.
            # Input: 1478134176.985864 --> Output: "2016-11-03 00:49:36.985864 UTC"
            dt = datetime.datetime.utcfromtimestamp(float(value))
            return dt.strftime(DATETIME_FMT)
        elif field.type == 'BYTES':
            # Input: "YmJi" --> Output: "YmJi"
            return value
        elif field.type == 'DATE':
            # Input: "2016-11-03" --> Output: "2016-11-03"
            return value
        elif field.type == 'DATETIME':
            # Input: "2016-11-03T00:49:36" --> Output: "2016-11-03T00:49:36"
            return value
        elif field.type == 'TIME':
            # Input: "00:49:36" --> Output: "00:49:36"
            return value
        elif field.type == 'RECORD':
            # Note that a schema field object supports also a RECORD type. However
            # when querying, the repeated and/or record fields are flattened
            # unless we pass the flatten_results flag as False to the source
            return self.convert_row_to_dict(value, field)
        elif field.type == 'NUMERIC':
            return decimal.Decimal(value)
        elif field.type == 'GEOGRAPHY':
            return value
        else:
            raise RuntimeError('Unexpected field type: %s' % field.type)

    @staticmethod
    def convert_row_to_dict(row, schema):
        """Converts a TableRow instance using the schema to a Python dict."""
        result = {}
        for index, col in enumerate(schema):
            if isinstance(col, dict):
                result[col['name']] = row[index]
            else:
                result[col] = row[index]
            # result[field.name] = self._convert_cell_value_to_dict(value, field)
        return result

    def _get_cols(self, row, lst_only=False):
        """
        return a sting of columns
        :param row: can be either dict or schema from cursor.description
        :return: string to be placed in insert command of sql
        """
        names = []
        if isinstance(row, dict):
            names = list(row.keys())
        elif isinstance(row, tuple):
            for column in row:
                if isinstance(column, tuple):
                    names.append(column[0])  # columns name is the first attribute in cursor.description
                else:
                    raise ExceptionNoColumns("Not a valid column object")
        if len(names):
            if lst_only:
                return names
            else:
                cols = ', '.join(map(self.escape_name, names))
            return cols
        else:
            raise ExceptionNoColumns("No columns to make")

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def count(self, query):
        with self.connection.cursor() as cursor:
            logging.info("Estimating size for query")
            logging.info(cursor.mogrify(query))
            cursor.execute(query)
            row_count = cursor.rowcount
            return row_count
        
    @staticmethod
    def _convert_to_str(value):
        if isinstance(value, six.string_types):
            return value.replace("'", "''")
        elif isinstance(value, (datetime.date, datetime.datetime)):
            return str(value)
        else:
            return value

    @staticmethod
    def _get_data_row(cols, rows):
        data_rows = []
        row_format = ("'{}',"*(len(cols))).rstrip(',')
        for row in rows:
            data = []
            for col in cols:
                _value = row[col]
                _value = BaseWrapper._convert_to_str(_value)
                data.append(_value)
            data_rows.append(row_format.format(*data))
        return tuple(data_rows)

    @staticmethod
    def format_data_rows_query(data_rows):
        rows_format = ("({}),"*len(data_rows)).rstrip(',')
        formatted_rows = rows_format.format(*data_rows)
        return formatted_rows

class MSSQLWrapper(BaseWrapper):
    """Microsoft SQL Server client wrapper with utilities for querying.
    """

    def read(self, query, batch=READ_BATCH):
        """
            Execute the query and return the result in batch

            or read in batch

            # for i in range((size//batch)+1):
            #     records = cursor.fetchmany(size=batch)
            #     yield records, schema
            TODO://
            1. Add batch read

            :param query: query to execute
            :param batch: size of batch to read
            :return: iterator of records in batch
        """
        with self.connection.cursor() as cursor:
            logging.info(f"Executing Read query: {query}")
            print(f"Executing Read query: {query}")
            cursor.execute(query)
            schema = cursor.description
            size = cursor.rowcount
            records = cursor.fetchall()
            yield records, schema
    
    @staticmethod
    def paginated_query(query, limit, offset, primary_key=1):
        query = query.strip(";")
        pag_query = f"{query} ORDER BY {primary_key} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        return pag_query

    def total_rows(self, query):
        row_count_query = f"SELECT COUNT(1) AS row_count FROM ({query}) AS sub"
        print(f'''row count query: {row_count_query}''')
        logging.info(f'''row count query: {row_count_query}''')
        with self.connection.cursor() as cursor:
            cursor.execute(row_count_query)
            result = cursor.fetchall()
        result = result[0][0]
        return result
    
    @staticmethod
    def row_as_dict(row, schema):
        """
            postgres cursor object contains the description in this format
            (Column(name='id', type_code=23), Column(name='name', type_code=25))

            pymysql cursor description has below format
            ((col1, 123,123,1,23), (col2, 23,123,1,23))
            :param row: database row, tuple/list of objects
            :param schema:
            :return:
        """
        row_dict = {}
        for index, column in enumerate(schema):
            row_dict[column[0]] = row[index]
            # if isinstance(column, Column):
            #     row_dict[column.name] = row[index]
            # else:
            #     row_dict[column[0]] = row[index]
        return row_dict

class AS400Wrapper(BaseWrapper):
    """IBM AS400 client wrapper with utilities for querying.
    """
    
    def read(self, query, batch=READ_BATCH):
        """
            Execute the query and return the result in batch

            or read in batch

            # for i in range((size//batch)+1):
            #     records = cursor.fetchmany(size=batch)
            #     yield records, schema
            TODO://
            1. Add batch read

            :param query: query to execute
            :param batch: size of batch to read
            :return: iterator of records in batch
        """
        with self.connection.cursor() as cursor:
            logging.info(f"Executing Read query: {query}")
            print(f"Executing Read query: {query}")
            cursor.execute(query)
            schema = cursor.description
            size = cursor.rowcount
            records = cursor.fetchall()
            yield records, schema
    
    @staticmethod
    def paginated_query(query, limit, offset, primary_key=1):
        query = query.strip(";")
        pag_query = f"{query} ORDER BY {primary_key} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        return pag_query

    def total_rows(self, query):
        row_count_query = f"SELECT COUNT(1) AS row_count FROM ({query}) AS sub"
        print(f'''row count query: {row_count_query}''')
        logging.info(f'''row count query: {row_count_query}''')
        with self.connection.cursor() as cursor:
            cursor.execute(row_count_query)
            result = cursor.fetchall()
        result = result[0][0]
        return result
    
    @staticmethod
    def row_as_dict(row, schema):
        """
            postgres cursor object contains the description in this format
            (Column(name='id', type_code=23), Column(name='name', type_code=25))

            pymysql cursor description has below format
            ((col1, 123,123,1,23), (col2, 23,123,1,23))
            :param row: database row, tuple/list of objects
            :param schema:
            :return:
        """
        row_dict = {}
        for index, column in enumerate(schema):
            row_dict[column[0]] = row[index]
            # if isinstance(column, Column):
            #     row_dict[column.name] = row[index]
            # else:
            #     row_dict[column[0]] = row[index]
        return row_dict

class OracleWrapper(BaseWrapper):
    """Oracle client wrapper with utilities for querying.
    """
    
    def read(self, query, batch=READ_BATCH):
        """
            Execute the query and return the result in batch

            or read in batch

            # for i in range((size//batch)+1):
            #     records = cursor.fetchmany(size=batch)
            #     yield records, schema
            TODO://
            1. Add batch read

            :param query: query to execute
            :param batch: size of batch to read
            :return: iterator of records in batch
        """
        with self.connection.cursor() as cursor:
            logging.info(f"Executing Read query: {query}")
            print(f"Executing Read query: {query}")
            cursor.execute(query)
            schema = cursor.description
            size = cursor.rowcount
            records = cursor.fetchall()
            yield records, schema
    
    @staticmethod
    def paginated_query(query, limit, offset, primary_key=1):
        query = query.strip(";")
        pag_query = f"{query} ORDER BY {primary_key} OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        return pag_query

    def total_rows(self, query):
        row_count_query = f"SELECT COUNT(1) AS row_count FROM ({query})"
        print(f'''row count query: {row_count_query}''')
        logging.info(f'''row count query: {row_count_query}''')
        with self.connection.cursor() as cursor:
            cursor.execute(row_count_query)
            result = cursor.fetchall()
        result = result[0][0]
        return result
    
    @staticmethod
    def row_as_dict(row, schema):
        """
            postgres cursor object contains the description in this format
            (Column(name='id', type_code=23), Column(name='name', type_code=25))

            pymysql cursor description has below format
            ((col1, 123,123,1,23), (col2, 23,123,1,23))
            :param row: database row, tuple/list of objects
            :param schema:
            :return:
        """
        row_dict = {}
        for index, column in enumerate(schema):
            row_dict[column[0]] = row[index]
            # if isinstance(column, Column):
            #     row_dict[column.name] = row[index]
            # else:
            #     row_dict[column[0]] = row[index]
        return row_dict
