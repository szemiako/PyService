import math
import pyodbc
import base_parser as p

def _servers():
    return {
        'PRODUCTION.US.CLOUD': {
            'Environment': 'Production',
            'Region': 'US'
        },
        'PRODUCTION.US.DATA_CENTER': {
            'Environment': 'Production',
            'Region': 'US'
        },
        'DEV.SERVER.LOCAL': {
            'Environment': 'Dev',
            'Region': 'US'
        }
    }

def sqlize(record):
    record = str(record)[1:-1]

    danger_quotes = p.re.findall(r'[A-Za-z]{1}\'[A-Za-z]{1}|[A-Za-z]{1}\'[ ]{1}', record)
    if danger_quotes != []:
        for dq in danger_quotes:
            record = p.re.sub(dq, "{0}''{1}".format(dq[0], dq[-1]), record)
        record = p.re.sub('"', "'", record)
    
    record = p.re.sub(r'None', "''", record)
    return '({0})'.format(record)

def batch_load(kind, records, batch=999):
    c = queries()

    i = 0
    rows = len(records)
    iterations = math.ceil(rows / batch)
    
    while i < iterations:
        r = records[(i * batch) : (i * batch) + (batch - 1)]
        query = c.Server(kind, ','.join(list(map(sqlize, r))))._query
        c._execute_insert(str(query))
        i += 1

def _load_to_server(kind, records):
    if not kind == 'account':
        return batch_load(kind, records.values.tolist())
    else:
        account_records = records[[
            'GUID',
            'Name',
            'Number',
            'Server'
        ]]
        return batch_load(kind, account_records.values.tolist())

class base_connection:
    """Base class for the SQL database connection."""
    def __init__(
        self,
        server,
        database,
    ):
        self._server = server
        self._database = database
        self._driver = '{SQL Server}'
        self._trusted_connection = 'yes'
        self._connection_parameters = 'Driver={driver}; Server={server}; Database={db}; Trusted_Connection={trusted_connection}'.format(
            driver = self._driver,
            server = self._server,
            db = self._database,
            trusted_connection = self._trusted_connection
        )
        self._connection = pyodbc.connect(self._connection_parameters)
        self._cursor = self._open()

    # Make a new cursor.
    def _open(self):
        return self._connection.cursor()

    # Close the existing cursor.
    def _close(self):
        return self._cursor.close()

    # Create a fresh cursor.
    def _new_cursor(self):
        self._close()
        return self._open()

class queries(base_connection):
    """Create a class for queries that inherits the Base Connection.
    In general, this is meant to only "hold" the queries needed for execution,
    and inherits the Base Connection so that we can just quickly call
    the Queries class to interact with the databse with all values
    populated."""
    def __init__(
        self,
        server='QA',     # Default server.
        database='PTCC'  # Default databse.
    ):
        super().__init__(
            server,
            database
        )

    def _execute_select_all(self, query):
        """Execute a query, but specifically SELECT statements.
        fetchall() allows us to return the results of our query
        (through the use of execute) as a list. We want
        to retain the cursor information, as we will need it again
        and also we only expect to use Execute Select All once (for
        each class)."""
        self._cursor.execute(query)
        results = self._cursor.fetchall()
        self._cursor = self._new_cursor()
        return results

    def _execute_insert(self, query):
        self._cursor.execute(query)
        results = self._connection.commit()
        self._cursor = self._new_cursor()
        return results

    def _use_temp(self, query, values, select):
        self._cursor.execute(query)
        self._cursor.execute(values)
        return self._execute_select_all(select)

    class Stocks:
        def __init__(
            self,
            kind=None,
            broker=None
        ):
            self._template = self._get_security_template(kind)
            self._query = (lambda s: self._template if s == 'securities' else self._template.format(broker))(kind)

        def _get_security_template(self, kind):
            t = {
                'broker': self._security_type_template(),
                'securities': [                       # Broken out into distinct segments to make it easier to make changes later.
                    self._sod_price_template_create(),
                    self._sod_price_template_insert(),
                    self._sod_price_template_select()
                ]
            }
            return t[kind]

        def _security_type_template(self):
            return """
                SELECT
                    cm.VendorCode,
                    t.Type
                FROM Brokers b WITH (NOLOCK)
                JOIN CodeMap cm WITH (NOLOCK) ON b.GUID = cm.GUID
                JOIN Types t WITH (NOLOCK) ON c.VendorCode = t.Type
                WHERE b.Name = '{0}'
            """
        
        def _sod_price_template_create(self):
            return """
                CREATE TABLE ##securities (
                    symbol VARCHAR(50)
                )
            """
        
        def _sod_price_template_insert(self):
            return """
                INSERT INTO ##securities VALUES {0}
            """

        def _sod_price_template_select(self):
            return """
                SELECT 
                    stocks.Symbol,
                    stocks.[End of Day Price]
                FROM ##securities ts
                CROSS APPLY (
                    SELECT TOP (1)
                        s.Symbol,
                        ISNULL(s.Price, 0) [End of Day Price]
                    FROM dbo.Stocks s WITH (NOLOCK)
                    WHERE (
                        s.Symbol = ts.symbol
                    )
                    ORDER BY s.LastUpdated DESC
                ) stocks
            """

    class Server:
        def __init__(
            self,
            kind,
            records
        ):
            self._template = self._get_template(kind)
            self._query = self._template.format(records)

        def _get_template(self, kind):
            q = {
                'locations': self.Locs()._template,
                'account': self.Accounts()._template,
                'holding': self.Holding()._template,
                'transaction': self.Transaction()._template
            }
            return q[kind]

        class Locs:
            def __init__(
                self
            ):
                self._template = """
                    SELECT
                        @@SERVERNAME [ServerName],
                        Number,
                        Name
                    FROM Locs WITH (NOLOCK)
                    WHERE Name = {0}
                    GROUP BY
                        Number,
                        Name
                """

        class Accounts:
            def __init__(
                self
            ):
                self._template = """
                    INSERT INTO dbo.Accounts (
                        GUID,
                        Name,
                        Number,
                        Server
                    )
                    VALUES {0}
                """
        
        class Holding:
            def __init__(
                self
            ):
                self._template = """
                    INSERT INTO dbo.Holdings (
                        GUID,
                        Name,
                        Number,
                        Server,
                        Symbol,
                        Shares
                    )
                    VALUES {0}"""

        class Transaction:
            def __init__(
                self
            ):
                self._template = """
                    INSERT INTO dbo.Transaction (
                        GUID,
                        Name,
                        Number,
                        Server,
                        Symbol,
                        Shares,
                        Price,
                        Direction,
                        Date
                    )
                    VALUES {0}"""