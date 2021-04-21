from datetime import datetime

def _data_model_map():
    return {
        'account': {
            '_guid': 'GUID',
            '_broker_name': 'Name',
            '_account_number': 'Number',
            '_server': 'Server'
        },
        'holding': {
            '_guid': 'GUID',
            '_broker_name': 'Name',
            '_account_number': 'Number',
            '_server': 'Server',
            '_symbol': 'Symbol',
            '_shares': 'Shares'
        },
        'transaction': {
            '_guid': 'GUID',
            '_broker_name': 'Name',
            '_account_number': 'Number',
            '_server': 'Server',
            '_symbol': 'Symbol',
            '_shares': 'Shares',
            '_price': 'Price',
            '_direction': 'Direction',
            '_date': 'Date'
        }
    }

class base_model:
    def __init__(
        self,
        guid,
        server,
        record_number,
        broker_name,
        account_number
    ):
        self._guid = guid
        self._server = server
        self._record_number = record_number
        self._broker_name = broker_name
        self._account_number = account_number

class account(base_model):
    def __init__(
        self,
        guid,
        server,
        record_number,
        broker_name,
        account_number    
    ):
        super().__init__(
            guid,
            server,
            record_number,
            broker_name,
            account_number
        )

    def __call__(self, setting=None, record=None):
        return account(
            guid=None,
            server=None,
            record_number=None,
            broker_name=None,
            account_number=None
        )

class holding(base_model):
    def __init__(
        self,
        guid,
        server,
        record_number,
        broker_name,
        account_number,
        symbol,
        shares
    ):
        super().__init__(
            guid,
            server,
            record_number,
            broker_name,
            account_number
        )
        self._symbol = symbol
        self._shares = shares

    def __call__(self, setting=None, record=None):
        return holding(
            guid=None,
            server=None,
            record_number=None,
            broker_name=None,
            account_number=None,
            symbol=None,
            shares=None
        )

class transaction(base_model):
    def __init__(
        self,
        guid,
        server,
        record_number,
        broker_name,
        account_number,
        symbol,
        shares,
        price,
        direction,
        date
    ):
        super().__init__(
            guid,
            server,
            record_number,
            broker_name,
            account_number
        )
        self._symbol = symbol
        self._shares = shares
        self._price = price
        self._direction = direction
        self._date=datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

    def __call__(self, setting=None, record=None):
        return transaction(
            guid=None,
            server=None,
            record_number=None,
            broker_name=None,
            account_number=None,
            symbol=None,
            shares=None,
            price=None,
            direction=None,
            date=None
        )