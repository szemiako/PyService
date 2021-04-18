import connection
import interface
import json
import pandas as pd
from pandas import DataFrame
import parsers_factory as pf
import sys

def _base_dir():
    """Retrieves base working directory."""
    return pf.p.os.path.dirname(pf.p.os.path.realpath(__file__))

class helper:
    """Helper class used to interact with the different broker
    feed parsers. These are intended to be analogous to a
    base class with methods applicable to almost all broker
    feeds. The specific methods required are defined in
    config.json. They include:
        * Files
        ** "config_id":= The respective configuration ID.
                        Helps to associate many files
                        to one feed.
        ** "filemask":= The regex filename mask.
        ** "kind":= The kind of file. In order of frequency
                    of occurance:
                    Positions, Transactions, Accounts,
                    Securities
        ** "delimiter":= Blank ("") if fixed width, else use
                        file delimiter.
        ** "headers":= Number of header records in the file.
        ** "trailers":= Number of trailer records in the file.
        ** "aggregation":= Blank ("") if aggregation of values
                        on a key not required; else use key
                        to be aggregated (e.g., "shares").
        ** "has_price":= If the source broker file requires
                        pricing information.
        * Configurations (Configs)
        ** "Broker":= Name of the broker dealer.
        ** "Customer":= Blank ("") if the specific customer
                        doesn't matter."""
    def __init__(
        self,
        id,
        filepath
    ):
        self._setting = self._fetch_setting(id, filepath)
        self._queries = connection.queries()

    def _fetch_setting(self, id, filepath, setting={}):
        """Given the input file ID, retrieve a specific file's
        associated information, as well as any other configurations."""
        with open('{0}/configs.json'.format(_base_dir()), 'r') as configs:
            settings = json.loads(configs.read())
            setting = settings['files'][id]
            setting.update(settings['configs'][setting['config_id']])
            setting['filepath'] = filepath
        return setting

    def _convert_slice_index(self, string, trailer=False):
        """Method used to convert strings to integers.
        Used to read headers and trailers from the configurations
        file, and using Python's slicing syntax (e.g., str[1:-1]),
        we extract specific records."""
        if string == '':
            return None
        else:
            return -1 * int(string) if trailer else int(string)

    def _fetch_content(self, c={}):
        """Method used to split the source file's content into a
        header, details, and trailer, by utilizing the properties
        of Python's slicing syntax."""
        with open(self._setting['filepath'], 'r') as f:
            content = list(f.read().splitlines())
            c['header'] = (lambda h: content[:self._convert_slice_index(h)] if h != '' else None)(self._setting['headers'])
            c['trailers'] = (lambda h: content[self._convert_slice_index(h, True):] if h != '' else None)(self._setting['trailers'])
            c['details'] = content[self._convert_slice_index(self._setting['headers']):self._convert_slice_index(self._setting['trailers'], True)]
            return self._setting.update({'content': c})

    def _parse_content(self):
        """This is where we apply generic parsing and processing rules
        in bulk to each record. We use the map function to apply
        parsing. We also retrieve the data model's attributes from
        the first element of the resulting map to apply the correct
        header later."""
        self._parsed = list(map(lambda record: pf.parser_factory(self._setting, record), self._setting['content']['details'])) # Candidate for improviement. At every pass of each record, we're basically loading all the parsers each time causing un-needed load.
        records = list(map(lambda record: record._parsed.__dict__ if record._parsed is not None else None, self._parsed))
        self._records = list(filter(None, records))
        if len(self._records) > 0:
            self._keys = self._records[0].keys()
        return self._records
 
    def _build_aggregate(self, agg, agg_on, rn, agg_val, df):
        """Storing results of aggregation done in separate
        method. We build a dictionary of aggregated values
        as a result of aggregation of values across an
        index. We just need the 0th row as the records are
        "duplicates"."""
        row = df[df['_record_number'] == rn]
        agg[rn] = row.to_dict('records')[0]
        agg[rn][agg_on] = agg_val
        return agg

    def _aggregate(self, df, agg={}):
        """Check to see if aggregation is required, as per
        configuration file by checking to see if aggregation
        over key is provided. For example, if
            'aggregation' == 'shares'
        then we sum the shares values across all records
        with the same key."""
        if self._setting['aggregation']:
            key = '_{0}'.format(self._setting['aggregation'])
            grouped = df.groupby('_record_number')[key].sum()                                               # Aggregation happens here.
            list(map(lambda g: self._build_aggregate(agg, key, g[0], g[1], df), grouped.to_dict().items())) # Building the aggregated values dictionary.
            df = DataFrame(list(map(lambda r: r.values(), agg.values())), columns=self._keys)
        return df
  
    def _map_security_types(self, security_types, st={}):
        """The intention here is to hit the PTCC database once to get
        security type mappings from the CodeMap
        table. We get a template string, and then query against
        the database. We then convert it to a dictionary
        and map the security types from the broker feed our internal
        types."""
        query = self._queries.Stocks(kind='broker', broker=self._setting['broker'])._query # Build template.
        all_raw_types = self._queries._execute_select_all(query)                           # Get values.
        if all_raw_types != []:
            list(map(lambda s: st.update({s[0]: s[1]}), all_raw_types))                    # Lambda cannot contain assignment of key: value pair, so update is used.
            security_types = list(map(lambda k: st.get(k, -1), security_types))            # Map function.
        return security_types

    def _sod_prices(self, securities):
        """This method is meant to take a list of securities
        identifiers from the respective source files, build
        a temp table, and use that temp table to get the
        price for each security. Because the nature of the
        query, unique results are returned."""
        query = self._queries.Stocks(kind='securities')._query
        insert = query[1].format(','.join(list(map(connection.sqlize, securities))))
        return self._queries._use_temp(query[0], insert, query[2])

    def _get_sod_price(self, securities, h={}):
        """We use _sod_prices to get all prices of securities on
        the feed, but here we build a dict of unique securities
        and prices, and then map them to the source files."""
        prices = self._sod_prices(securities)        
        list(map(lambda p: h.update({'{0}{1}'.format(p[0], p[1]): float(p[2])}), prices))
        return list(map(lambda s: h.get('{0}{1}'.format(s[0], s[1]), 0), securities))

    def _get_is_price_needed(self, df):
        """Check to see if extraction of price information is
        required for a file; e.g., Robinhood position files
        don't include price information."""
        if self._setting['has_price'] == str(False):
            df['_price'] = self._get_sod_price(list(df['_symbol'].values.tolist()))
        return df

    def _normalize_security(self, df):
        """Here we perform simple normalizations, just to make
        sure correct data types are applied. Basic data
        wrangling for attributes not necessarily applciable
        to accounts."""
        if not self._setting['kind'] == 'account':
            df['_security_type'] = self._map_security_types(list(df['_security_type']))
            df['_price'] = pd.to_numeric(df['_price'])
            df['_shares'] = pd.to_numeric(df['_shares'])
        return df

    def _get_locations(self):
        """Read file of current Locations."""
        with open('{0}/locations.txt'.format(_base_dir())) as fal:
            locations = list(map(lambda r: r.split(','), fal.read().splitlines()))
            fal.close()
        return locations

    def _build_locations(self, l={}):
        """Here we build a unique dictionary of all account
        numbers and broker names from the 
        Locations table and create an "aggregated" list
        of servers where those accounts are disclosed.
        If we just join to the original records (the 
        unfilitered orginal list of Locations),
        it will select only the first values and won't
        duplicate records."""
        def _agg_list(unique, key, val):
            if not unique.get(key):
                unique[key] = [val]
            else:
                unique[key].append(val) # Aggregation of Servers.
            return unique
        fal = self._get_locations()
        return list(map(lambda r: _agg_list(l, '{0}{1}'.format(r[1], r[2]), r[0]), fal))[0]

    def _get_server(self, key):
        """Map each record to all of it's respective servers."""
        fal = self._build_locations()
        return list(map(lambda k: fal.get(k, 'QA'), key))

    def _copy_record(self, d, v, ds, i=0):
        """This method is used to "flatten" the multiple
        servers into individual records. The records are
        identical except for their destination servers. We
        do this this recursively."""
        k = str(pf.p.uuid.uuid4())
        if type(ds) == list:
            while i < len(ds):
                self._copy_record(d, v, ds[i], i=i) # Call the method and update the existing dictionary. Do this for each value in the list.
                i += 1
        else:
            t = v.to_dict()                         # The original record is a DataFrame, so we convert it to a dictionary.
            t['_server'] = str(ds)
            t['_guid'] = k
            d.update({'{0}{1}'.format(k, i): t})
        return d

    def _map_servers(self, df, dmn={}):
        """Here we perform the "flattening" of servers,
        and return the "duplicated" records, so that the
        records are distributed to the correct servers."""
        dmn_servers = self._get_server(list(map(lambda ind: '{0}{1}'.format(ind['_account_number'], ind['_broker_name']), df.to_dict('records'))))
        list(map(lambda r, ds: self._copy_record(dmn, df.iloc[r], ds), range(len(dmn_servers)), dmn_servers))
        return DataFrame(list(map(lambda r: r.values(), dmn.values())), columns=self._keys)

    def _order_and_rename_columns(self, df):
        """Map, order and re-name column headers of DataFrame."""
        h = pf.p.models._data_model_map()
        df = df.reindex(columns=h[self._setting['kind']].keys())
        df.columns = h[self._setting['kind']].values()
        self._keys = list(df.columns)
        return df

    def _make_df(self):
        """Make the the original DataFrame and preform
        necessary transformations to data."""
        df = DataFrame(self._records, columns=self._keys)        # Original DataFrame.
        df.dropna                                                # Drop any invalid records (records where all values are None).
        df.fillna('NULL')
        df = self._aggregate(df)                                 # Aggregate if needed.
        df = self._get_is_price_needed(df)                       # Get security master price if needed.
        df = self._normalize_security(df)                        # Normalize few parameters related to securities.
        df = self._map_servers(df)                               # Map Servers and duplicate records if needed.
        df = self._order_and_rename_columns(df)                  # Order and rename columns.
        self._df = df
        return self._df                                          # Create DataFrame.

    def _write_content(self):
        """Write the contents to a .csv file. Can create a
        global configuration for the delimiter if needed."""
        if self._df is None:
            return False
        else:
            f = open('{0}_{1}.csv'.format(                # Create the file.
                self._setting['filepath'][:-4],
                str(pf.p.uuid.uuid4())
                ),
                'w'
            )
            f.write('{0}\n'.format(','.join(self._keys))) # Write the header row.
            self._df.to_csv(                              # Write the DataFrame to the file.
                f,
                quoting=2,
                header=False,
                index=False,
                line_terminator='\n',
                sep=',',
                date_format='%Y%m%d %H:%M:%S'
            )
            return f.close()

def process(parameters):
    """We interact with the parser here, via configuration
    file (config.json) and arg parse command line
    interface. The try/catch statements are meant to
    capture possible errors at each step of the
    parsing proces."""
    instance = helper(
       parameters._args.file_id,
       parameters._args.filepath
    )

    try:
        instance._fetch_content()
    except:
        sys.exit('{0} | Error accessing file:\n{1}'.format(
            pf.p.models.datetime.strftime(pf.p.models.datetime.now(), '%Y-%m-%d %H:%M:%S'),
            parameters._args.filepath
        ))

    try:
        instance._parse_content()
    except:
        sys.exit('{0} | Error parsing file:\n{1}'.format(
            pf.p.models.datetime.strftime(pf.p.models.datetime.now(), '%Y-%m-%d %H:%M:%S'),
            parameters._args.filepath
        ))
        
    try:
        instance._make_df()
    except:
        sys.exit('{0} | Error building DataFrame:\n{1}'.format(
            pf.p.models.datetime.strftime(pf.p.models.datetime.now(), '%Y-%m-%d %H:%M:%S'),
            parameters._args.filepath
        ))
    
    try:
        instance._write_content()
    except:
        sys.exit('{0} | Error writing content to file:\n{1}'.format(
            pf.p.models.datetime.strftime(pf.p.models.datetime.now(), '%Y-%m-%d %H:%M:%S'),
            parameters._args.filepath
        ))

    try:
        connection._load_to_server(instance._setting['kind'], instance._df)
    except:
        sys.exit('{0} | Error loading data to target server:\n{1}'.format(
            pf.p.models.datetime.strftime(pf.p.models.datetime.now(), '%Y-%m-%d %H:%M:%S'),
            parameters._args.filepath
        ))

process(interface.sf_args())