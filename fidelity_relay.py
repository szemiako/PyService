import base_parser as p

class holding(p.base_holding):
    def __init__(
        self,
        setting=None,
        record=None
    ):
        self._record = self.__call__(setting, record)

    def _get_identifiers(self, record, security={}):
        security.update({'security_type': record[28]})
        if not security['security_type'] == '8':
            s = {
                'symbol': record[19:27].strip()
            }
        else:
            s = {
                'symbol': self._build_osi(record[109:129].strip())
            }
        security.update(s)
        return security

    def _build_osi(self, unpadded_osi, price_regex=r'(?<=[C|P])[0-9\.]{1,}$'):
        no_price = p.re.sub(price_regex, '', unpadded_osi)
        unpadded_price = p.re.findall(price_regex, unpadded_osi)[0]
        price_components = unpadded_price.split('.')
        price = (lambda p: '{0}{1}'.format('{:>05d}'.format(int(p[0])), '{:<03d}'.format(int(p[1]))) if len(p) > 1 else '{0}{1}'.format('{:>05d}'.format(int(p[0])), '{:<03d}'.format(0)))(price_components)
        return '{0}{1}'.format(no_price, price)

    def __call__(self, setting, record):
        self._raw = record
        self._new = p.models.holding.__call__(self)
        if setting is None:
            return self._new
        else:
            security = self._get_identifiers(record)
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = setting['broker']
            self._new._account_number = record[0:9].strip()
            self._new._record_number = '{0}{1}'.format(self._new._account_number, security['symbol'])
            self._new._symbol = security['symbol']
            self._new._shares = (int(record[149:165]) / 100000) * (lambda account_type: -1 if account_type == '3' else 1)(self._new._account_type)
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
            'B': 'Buy',
            'S': 'Sell',
            'short_types': {
                'SHORT SALE':  'Sell Short',
                'SHORT COVER': 'Cover Short'
            }
        }

        short_type = list(filter(lambda e: e if e in record else False, directions['short_types'].keys()))
        
        if short_type:
            return directions['short_types'][short_type[0]]
        else:
            return directions[record[139].upper()]

    def _get_identifiers(self, record, security={}):
        security.update({'security_type': record[18]})
        if not security['security_type'] == '8':
            s = {
                'symbol': record[:9].strip()
            }
        else:
            s = {
                'symbol': self._build_osi(record[99:119].strip())
            }
        security.update(s)
        return security

    def _build_osi(self, unpadded_osi, price_regex=r'(?<=[C|P])[0-9\.]{1,}$'):
        no_price = p.re.sub(price_regex, '', unpadded_osi)
        unpadded_price = p.re.findall(price_regex, unpadded_osi)[0]
        price_components = unpadded_price.split('.')
        price = (lambda p: '{0}{1}'.format('{:>05d}'.format(int(p[0])), '{:<03d}'.format(int(p[1]))) if len(p) > 1 else '{0}{1}'.format('{:>05d}'.format(int(p[0])), '{:<03d}'.format(0)))(price_components)
        return '{0}{1}'.format(no_price, price)

    def __call__(self, setting, record):
        self._raw = record
        self._new = p.models.transaction.__call__(self)
        if setting is None or record.startswith('THERE WERE NO TRADES PROCESSED'):
            return self._new
        else:
            security = self._get_identifiers(record)
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = setting['broker']
            self._new._account_number = record[141:150].strip()
            self._new._record_number = '{0}{1}'.format(self._new._account_number, security['symbol'])
            self._new._symbol = security['symbol']
            self._new._shares = (int(record[176:192]) / 100000) * (lambda direction: -1 if direction == 'S' else 1)(record[139].upper())
            self._new._price = (int(record[158:175]) / 1000000000) * (lambda assigned: 0 if assigned in record else 1)('ASSIGNED PUTS')
            self._new._direction = self._get_direction(record)
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