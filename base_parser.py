from datetime import datetime
import models
import os
import re
import uuid

class base_account:
    def __init__(
        self,
        setting=None,
        record=None
    ):
        self._record = None

    def __call__(self, setting, record):
        self._new = models.account.__call__(self)
        return self._new
        
    def _validate_record(self):
        return True

class base_holding:
    def __init__(
        self,
        setting=None,
        record=None
    ):
        self._record = self.__call__(setting, record)

    def _get_identifiers(self, record):
        security = {}
        return security

    def __call__(self, setting, record):
        self._new = models.account.__call__(self)
        return self._new
        
    def _validate_record(self):
        return True

class base_transaction:
    def __init__(
        self,
        setting=None,
        record=None
    ):
        self._record = self.__call__(setting, record)

    def _get_transaction_type(self, record):      
        transaction_types = {}
        return transaction_types.get(record)  

    def _get_identifiers(self, record):
        security = {}
        return security

    def __call__(self, setting, record):
        self._new = models.account.__call__(self)
        return self._new
        
    def _validate_record(self):
        return True