import base_parser as p

class account(p.base_account):
    def __init__(
        self,
        setting=None,
        record=None
    ):
        self._record = self.__call__(setting, record)

    def __call__(self, setting, record):
        self._raw = record
        self._new = p.models.account.__call__(self)
        if setting is None:
            return self._new
        else:
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = setting['broker']
            self._new._account_number = record
            return self._validate_record()

    def _validate_record(self):
        validators = [
            False if self._new._account_number is None else True
        ]

        if all(validators): return self._new

class holding(p.base_holding):
    def __init__(
        self,
        setting=None,
        record=None
    ):
        self._record = self.__call__(setting, record)

    def _get_identifiers(self, record):
        security = {
            'security_type': record[6]
        }

        if not security['security_type'] == '35':
            s = {
                'symbol': record[1]
            }
        else:
            s = {
                'symbol': record[7]
            }
        security.update(s)
        return security

    def __call__(self, setting, record):
        self._raw = record
        self._new = p.models.holding.__call__(self)
        if setting is None:
            return self._new
        else:
            record = record.split(setting['delimiter'])
            security = self._get_identifiers(record)
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = setting['broker']
            self._new._account_number = record[0]
            self._new._record_number = '{0}{1}'.format(self._new._account_number, security['symbol'])
            self._new._symbol = security['symbol']
            self._new._shares = record[9]
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

    def _get_direction(self, record):      
        directions = {
            'Buy': 'Buy',
            'Sell': 'Sell',
            'BuyToOpen': 'Buy',
            'BuyToClose': 'Cover Short',
            'SellToOpen': 'Sell Short',
            'SellToClose': 'Sell',
            'CAN': 'Cancelled'
        }
        return directions.get(record)

    def _get_identifiers(self, record):
        security = {
            'security_type': record[6]
        }

        if not security['security_type'] == '35':
            s = {
                'symbol': record[1]
            }
        else:
            s = {
                'symbol': record[7]
            }

        security.update(s)
        return security

    def __call__(self, setting, record):
        self._raw = record
        self._new = p.models.transaction.__call__(self)
        if setting is None:
            return self._new
        else:
            record = record.split(setting['delimiter'])
            security = self._get_identifiers(record)
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = setting['broker']
            self._new._account_number = record[0]
            self._new._record_number = record[13]
            self._new._symbol = security['symbol']
            self._new._shares = record[9]
            self._new._price = record[10]
            self._new._direction = self._get_direction(record[12])
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