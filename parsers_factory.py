import base_parser as p
import fidelity_relay
import robinhood
import suntrust
import transamerica
import vanguard

class parser_factory:
    """Create a parser factory that registers all file parsers
    which are accessed by building keys based on config file."""
    def __init__(
        self,
        setting,
        record
    ):
        self._setting = setting     # The current configuration.
        self._record = record       # Source record.
        self._parsers = {}
        self._parsed = self._parse( # Actually parse.
            '{0}{1}{2}'.format(setting['broker'], setting['customer'], setting['kind']),
            self._setting,
            self._record
        )
        
    def _register_parser(self, key, parser):
        """Add parser to parsers dict."""
        self._parsers[key] = parser

    def _build_parsers(self):
        """Registering all the current parsers. Notice that
        Robinhood can be configured either per-customer
        or as a consolidated feed."""
        self._register_parser('{0}{1}{2}'.format('Fidelity', 'Relay', 'holding'), fidelity_relay.holding())
        self._register_parser('{0}{1}{2}'.format('Fidelity', 'Relay', 'transaction'), fidelity_relay.transaction())
        self._register_parser('{0}{1}{2}'.format('Robinhood', '', 'account'), robinhood.account())
        self._register_parser('{0}{1}{2}'.format('Robinhood', '', 'holding'), robinhood.holding())
        self._register_parser('{0}{1}{2}'.format('Robinhood', '', 'transaction'), robinhood.transaction())
        self._register_parser('{0}{1}{2}'.format('Vanguard', '', 'account'), vanguard.account())
        self._register_parser('{0}{1}{2}'.format('Vanguard', '', 'holding'), vanguard.holding())
        self._register_parser('{0}{1}{2}'.format('Vanguard', '', 'transaction'), vanguard.transaction())
        self._register_parser('{0}{1}{2}'.format('TransAmerica', '', 'holding'), transamerica.holding())
        self._register_parser('{0}{1}{2}'.format('TransAmerica', '', 'transaction'), transamerica.transaction())
        self._register_parser('{0}{1}{2}'.format('SunTrust Investment Services', '', 'holding'), suntrust.holding())
        self._register_parser('{0}{1}{2}'.format('SunTrust Investment Services', '', 'transaction'), suntrust.transaction())

    def _parse(self, key, setting, record):
        """Access the parser based on the configuration key."""
        self._build_parsers()
        parser = self._parsers.get(key)
        if not parser:
            return key
        else:
            return parser(setting, record)