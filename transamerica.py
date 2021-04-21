import base_parser as p

class holding(p.base_holding):
    def __init__(
        self,
        setting=None,
        record=None
    ):
        self._record = self.__call__(setting, record)

    def _preprocess(self, delimiter, record):
        pattern = p.re.compile('''((?:[^{0}"']|"[^"]*"|'[^']*')+)'''.format(delimiter))
        record = pattern.split(record)[1::2]
        record = list(map(lambda x: p.re.sub(r'N/A|NULL', '', x), record))
        return record

    def _get_identifiers(self, record):
        symbol = record[4].strip()
        if symbol == 'XXX':
            security = {
                'symbol': 'XXX'
            }
        else:
            security = {
                'symbol': symbol
            }
        return security

    def __call__(self, setting, record):
        self._raw = record
        self._new = p.models.holding.__call__(self)
        if setting is None:
            return self._new
        else:
            record = self._preprocess(setting['delimiter'], record)
            security = self._get_identifiers(record)
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = record[0]
            self._new._account_number = record[1]
            self._new._symbol = security['symbol']
            self._new._record_number = '{0}{1}'.format(self._new._account_number, self._new._symbol)
            self._new._shares = float(record[5])
            return self._validate_record()
    
    def _validate_record(self):
        security = {
            'symbol': self._new._symbol
        }
        
        validators = [
            False if float(self._new._shares) == 0 else True,
            any(list(map(lambda security: False if not security else True, security.values())))
        ]

        if all(validators): return self._new

class transaction(p.base_transaction):
    def __init__(
        self,
        setting=None,
        record=None
    ):
        self._record = self.__call__(setting, record)

    def _preprocess(self, delimiter, record):
        pattern = p.re.compile('''((?:[^{0}"']|"[^"]*"|'[^']*')+)'''.format(delimiter))
        record = pattern.split(record)[1::2]
        record = list(map(lambda x: p.re.sub(r'N/A|NULL', '', x), record))
        return record

    def _get_identifiers(self, record):
        symbol = record[4].strip()
        if symbol == 'XXX':
            security = {
                'symbol': 'XXX'
            }
        else:
            security = {
                'symbol': symbol
            }
        return security

    def _get_direction(self, record):      
        directions = {
            'BY': 'Buy',
            'SL': 'Sell'
        }
        return directions.get(record)  

    def __call__(self, setting, record):
        self._raw = record
        self._new = p.models.transaction.__call__(self)
        if setting is None:
            return self._new
        else:
            record = self._preprocess(setting['delimiter'], record)
            security = self._get_identifiers(record)
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = record[0]
            self._new._account_number = record[1]
            self._new._symbol = security['symbol']
            self._new._record_number = '{0}{1}'.format(self._new._account_number, self._new._symbol)
            self._new._shares = float(record[5])
            self._new._price = float(record[6])
            self._new._direction = self._get_direction(record[4])
            return self._validate_record()

    def _validate_record(self):
        security = {
            'symbol': self._new._symbol
        }
        
        validators = [
            False if float(self._new._shares) == 0 else True,
            any(list(map(lambda security: False if not security else True, security.values()))),
            False if self._new._direction == None else True
        ]

        if all(validators): return self._new