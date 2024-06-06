"""
This module implements the bounded source for Postgres read


TODO://
1. Add transaction level for select, read committed etc

"""
import decimal
import json
from apache_beam.io import iobase
from apache_beam.metrics import Metrics
from apache_beam.io.range_trackers import OffsetRangeTracker
from apache_beam.transforms.display import DisplayDataItem
from apache_beam import coders
import os
import sys
import requests
import logging
from urllib.parse import quote_plus,unquote

try:
    from apitools.base.py.exceptions import HttpError
except ImportError:
    pass

import apache_beam as beam
from apache_beam.options import value_provider
from apache_beam.transforms.util import Reshuffle

from .wrapper import BaseWrapper, MSSQLWrapper, AS400Wrapper, OracleWrapper

from .exceptions import ExceptionInvalidWrapper

JSON_COMPLIANCE_ERROR = 'NAN, INF and -INF values are not JSON compliant.'
AUTO_COMMIT = False
READ_BATCH = 500000

def default_encoder(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError(
        "Object of type '%s' is not JSON serializable" % type(obj).__name__)

class RowAsDictJsonCoder(coders.Coder):
    """A coder for a table row (represented as a dict) to/from a JSON string.

    This is the default coder for sources and sinks if the coder argument is not
    specified.
    """

    def encode(self, table_row):
        # The normal error when dumping NAN/INF values is:
        # ValueError: Out of range float values are not JSON compliant
        # This code will catch this error to emit an error that explains
        # to the programmer that they have used NAN/INF values.
        try:
            return json.dumps(
                table_row, allow_nan=False, default=default_encoder).encode('utf-8')
        except ValueError as e:
            raise ValueError('%s. %s' % (e, JSON_COMPLIANCE_ERROR))

    def decode(self, encoded_table_row):
        return json.loads(encoded_table_row.decode('utf-8'))

class SQLSouceInput(object):
    def __init__(self, host=None, port=None, username=None, password=None,
                 database=None, table=None, query=None, primary_key=None, sql_url=None, sql_url_auth_header=None,
                 validate=False, coder=None, batch=READ_BATCH, autocommit=AUTO_COMMIT, wrapper=MSSQLWrapper, schema_only=False, *args, **kwargs):
        """

        :param host: db host ip address or domain
        :param port: db port 
        :param username: db username
        :param password: db password
        :param database: db connecting database
        :param table: table to fetch data, all data will be fetched in cursor
        :param query: query sting to fetch. either query or table can be passed
        :param primary_key: keys for de table or query for order by in pagination
        :param sql_url: url of sql file to download and use the sql url as query
        :param sql_url_auth_header: auth header to download from sql_url, should be json string, which will be decoded at calling time, default to no header
        :param validate: validation ? not used as of now
        :param coder: default coder to use
        :param batch: size of match to read the records default to READ_BATCH, not used
        :param autocommit: connection autocommit
        :param wrapper: which wrapper to use, mysql or postgres
        :param schema_only: return schema or data
        """

        self.database = database
        self.validate = validate
        self.coder = coder or RowAsDictJsonCoder()

        # connection
        self.table = table
        self.query = query
        self.primary_key = primary_key
        self.sql_url = sql_url
        self.sql_url_auth_header = sql_url_auth_header
        self.host = host
        self.port = port
        self.username = username
        self.password = quote_plus(password)
        self.batch = batch
        self.autocommit = autocommit
        if wrapper in [BaseWrapper, MSSQLWrapper, AS400Wrapper, OracleWrapper]:
            self.wrapper = wrapper
        else:
            raise ExceptionInvalidWrapper("Wrapper can be [BaseWrapper, MSSQLWrapper, AS400Wrapper, OracleWrapper]")

        self._connection = None
        self._client = None
        self.schema = None
        self.schema_only = schema_only

        self.runtime_params = ['host', 'port', 'username', 'password', 'database',
                               'table', 'query', 'primary_key','sql_url', 'sql_url_auth_header',
                               'batch', 'schema_only']

    @staticmethod
    def _build_value(source, keys):
        for key in keys:
            setattr(source, key, SQLSource.get_value(getattr(source, key, None)))

class SQLSource(SQLSouceInput, beam.io.iobase.BoundedSource):
    """
        This needs to be called as beam.io.Read(SQLSource(*arguments))

        TODO://
        1. To accept dsn or connection string

    """

    @property
    def client(self):
        """
            Create connection object with mysql or postgre based on the wrapper passed
            TODO://
            1. Make connection based on dsn or connection string
            :return:
        """
        self._build_value(self.runtime_params)
        if self._connection is not None and hasattr(self._connection, 'close'):
            self._client = self.wrapper(connection=self._connection)
            return self._client

        self._connection = self._create_connection()
        self._client = self.wrapper(connection=self._connection)
        return self._client

    def _create_connection(self):
        if self.wrapper == MSSQLWrapper:
            import pymssql
            logging.info('Validation pass:',self.password)
            logging.info('Validation pass:',unquote(self.password))
            # _connection = pymssql.connect(host=self.host,
            #                             user=self.username,
            #                             password=unquote(self.password),
            #                             database=self.database)
            sys.exit("DEBUG")
        elif self.wrapper == AS400Wrapper:
            import pyodbc
            _connection = pyodbc.connect(
                                        driver='{IBM i Access ODBC Driver}',
                                        system=self.host,
                                        port=int(self.port),
                                        uid=self.username,
                                        pwd=self.password,
                                        # database=self.source.database,
                                        translate=1)
        elif self.wrapper == OracleWrapper:
            import oracledb
            oracledb.init_oracle_client()
            dns_str = f"{self.host}:{self.port}/{self.database}"
            _connection = oracledb.connect(
                                        user=self.username,
                                        password=self.password, 
                                        dsn=dns_str)
        else:
            raise ExceptionInvalidWrapper("Invalid wrapper passed")

        return _connection

    def estimate_size(self):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        return self.client.count(self.query)

    def get_range_tracker(self, start_position, stop_position):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = beam.io.range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Use an unsplittable range tracker. This means that a collection can
        # only be read sequentially for now.
        range_tracker = beam.io.range_trackers.OffsetRangeTracker(start_position, stop_position)
        range_tracker = beam.io.range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        self._build_value(self.runtime_params)

        if self.table is not None:
            self.query = """SELECT * FROM {}""".format(self.table)
        elif self.query is not None:
            self.query = self.query
        elif self.sql_url is not None:
            self.query = self.download_sql(self.sql_url, self.sql_url_auth_header)
        else:
            raise ValueError("Source must have either a table or query or sql_url")

        if self.schema_only:
            if not self.table:
                raise Exception("table argument is required for schema")
            self.query = "DESCRIBE {}".format(self.table)

        for records, schema in self.client.read(self.query, batch=self.batch):
            print("SQLSource records: ", records)
            if self.schema is None:
                self.schema = schema

            if self.schema_only:
                yield records
                break

            for row in records:
                yield self.client.row_as_dict(row, schema)

    def _build_value(self, keys):
        for key in keys:
            setattr(self, key, self.get_value(getattr(self, key, None)))

    def _validate(self):
        if self.table is not None and self.query is not None:
            raise ValueError('Both a table and a query were specified.'
                             ' Please specify only one of these.')
        elif self.table is None and self.query is None:
            raise ValueError('A table or a query must be specified')
        elif self.table is not None:
            self.table = self.table
            self.query = None
        else:
            self.query = self.query
            self.table = None

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`
        This function will currently not be called, because the range tracker
        is unsplittable
        """
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = beam.io.range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Because the source is unsplittable (for now), only a single source is
        # returned.
        yield beam.io.iobase.SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)

    @staticmethod
    def get_value(value_obj):
        """
        Extract the value from value_obj, if value object is a runtime value provider
        :param value_obj:
        :return:
        """
        if callable(value_obj):
            return value_obj()
        if hasattr(value_obj, 'get'):
            return value_obj.get()
        elif hasattr(value_obj, 'value'):
            return value_obj.value
        else:
            return value_obj

    @staticmethod
    def download_sql(url, auth_headers):
        try:
            headers = json.loads(auth_headers)
        except Exception as ex:
            logging.debug("Could not json.loads the auth headers, {}, exception {}".format(auth_headers, ex))
            headers = None
        if os.path.exists(url) and os.path.isfile(url):
            with open(url, 'r') as fobj:
                return fobj.read()
        else:
            logging.info("Downloading form {}".format(url))
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                query = res.text
                logging.debug(("Downloaded query ", query))
                return query
            else:
                raise Exception("Could not successfully download data from {}, text, {}, status: {}".format(url, res.text, res.status_code))

class PaginateQueryDoFn(beam.DoFn):
        def __init__(self, *args, **kwargs):
            self.args = args
            logging.info(f"pagination query do fn wrapper:{kwargs['wrapper']}")
            
            self.kwargs = kwargs

        def process(self, query, *args):
            source = SQLSource(*self.args, **self.kwargs)
            SQLSouceInput._build_value(source, source.runtime_params)
            print(f"we're in the process method now; source.client: {source.client}")
            logging.info(f"we're in the process method now; source.client: {source.client}")
            
            query = source.query
            primary_key = source.primary_key
            batch = source.batch
            
            row_count = 0
            queries = []
            try:
                row_count = source.client.total_rows(query)
                print(f"number of rows to process: {row_count}")
                logging.info(f"number of rows to process: {row_count}")
                offsets = list(range(0, row_count, batch))
                for offset in offsets:
                    paginated_query = source.client.paginated_query(query, batch, offset, primary_key)
                    queries.append(paginated_query)
            except Exception as ex:
                logging.info(ex)
                print(ex)
                queries.append(query)
            queries = sorted(list(set(queries)))
            print("paginated queries:")
            print(queries)
            logging.info("paginated queries:")
            logging.info(queries)
            return queries

class SQLSourceDoFn(beam.DoFn):
    """
        This needs to be called as beam.io.Read(SQLSource(*arguments))
        TODO://
        1. To accept dsn or connection string
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def process(self, query, *args, **kwargs):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        source = SQLSource(*self.args, **self.kwargs)
        SQLSouceInput._build_value(source, source.runtime_params)
        self.source = source
        for records, schema in source.client.read(query):
            for row in records:
                yield source.client.row_as_dict(row, schema)

class ReadFromSQL(beam.PTransform):
    def __init__(self, *args, **kwargs):
        self.source = SQLSouceInput(*args, **kwargs)
        # logging.info(f"from inside the readFromSQL class, source: {self.source.wrapper}")
        self.args = args
        self.kwargs = kwargs

    def expand(self, pcoll):
        "now expanding!!!"
        return (pcoll.pipeline
                | 'UserQuery' >> beam.Create([1])
                | 'SplitQuery' >> beam.ParDo(PaginateQueryDoFn(*self.args, **self.kwargs))
                | 'reshuffle' >> Reshuffle()
                | 'Read' >> beam.ParDo(SQLSourceDoFn(*self.args, **self.kwargs))
                )
