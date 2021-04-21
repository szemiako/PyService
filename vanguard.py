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
            record = record.split(setting['delimiter'])
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = setting['broker']
            self._new._account_number = record[1].strip()
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

    def __call__(self, setting, record):
        self._raw = record
        self._new = p.models.holding.__call__(self)
        if setting is None:
            return self._new
        else:
            record = record.split(setting['delimiter'])
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = setting['broker']
            self._new._account_number = record[1].strip()
            self._new._symbol = record[8].strip()
            self._new._record_number = '{0}{1}'.format(self._new._account_number, self._new._symbol)
            self._new._shares = float(p.re.sub(',', '', record[14].strip()))
            return self._validate_record()

    def _validate_record(self):
        security = {
            'symbol': self._new._symbol
        }
        
        validators = [
            False if float(self._new._shares) == 0 else True,
            any(list(map(lambda security: False if not security else True, security.values()))),
            False if self._new._symbol.upper() == 'CASH' else True
        ]

        if all(validators): return self._new

class transaction(p.base_transaction):
    def __init__(
        self,
        setting=None,
        record=None
    ):
        self._record = self.__call__(setting, record)

    def _get_direction(self, record, quantity):
        record = record.strip()

        if record in {'BUY', 'BUYE', 'BUYOP'}:
            direction = 'Buy'
        elif record in {'SELCL', 'SELE', 'SELL'}:
            direction = 'Sell'
        elif record in {'SELSH', 'SELOP'}:
            direction = 'Sell Short'
        elif record in {'BUYCV', 'BUYCL'}:
            direction = 'Cover Short'
        elif record in {
            '0123',
            '4567',
            '8910'
        }:
            if float(quantity) < 0:
                direction = 'Sell'
            else:
                direction = 'Buy'
        else:
            direction = None
        return direction  

    def __call__(self, setting, record):
        self._raw = record
        self._new = p.models.transaction.__call__(self)
        if setting is None:
            return self._new
        else:
            record = record.split(setting['delimiter'])
            self._new._guid = str(p.uuid.uuid4())
            self._new._server = None
            self._new._broker_name = setting['broker']
            self._new._account_number = record[1].strip()
            self._new._record_number = record[17]
            self._new._symbol = record[4].strip()
            self._new._shares = float(p.re.sub(',', '', record[10].strip()))
            self._new._price = float(p.re.sub(',', '', record[11].strip()))
            self._new._direction = self._get_direction(record[6].upper(), self._new._shares)
            return self._validate_record()

    def _validate_record(self):
        security = {
            'symbol': self._new._symbol
        }
        
        validators = [
            False if float(self._new._shares) == 0 else True,
            any(list(map(lambda security: False if not security else True, security.values()))),
            False if self._new._direction == None else True,
            False if self._new._symbol.upper() == 'CASH' else True
        ]

        if all(validators): return self._new