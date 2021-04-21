import argparse

class sf_args:
    def __init__(
        self
    ):
        self._parser = argparse.ArgumentParser(
            description="""
                Building structured data parsers as command line arguments.
                This is an attepmt at the broker feed parser.
                Specifically, this is for for:
                    Fidelity Relay
                    Robinhood
                    Vanguard
                    TransAmerica
                    SunTrust
                """,
            add_help=True
        )
        self._setting_id = self._add_param('-id')
        self._filepath = self._add_param('-fp')
        self._args = self._parser.parse_args()

    def _add_param(self, param):
        params = {
            '-id': sf_args.file_id(),
            '-fp': sf_args.filepath()
        }
        p = params[str(param)]
        return self._parser.add_argument(
            p._option1,
            p._option2,
            help=p._help,
            type=p._type,
            choices=p._choices,
            required=p._required,
            default=p._default
        )
    
    class file_id:
        def __init__(
            self,
            o1='-id',
            o2='--file_id',
            h='The file_id, as taken from configs.json.',
            t=str,
            c=None,
            r=True,
            d=None
        ):
            self._option1 = o1
            self._option2 = o2
            self._help = h
            self._type = t
            self._choices = c
            self._required = r
            self._default = d

    class filepath:
        def __init__(
            self,
            o1='-fp',
            o2='--filepath',
            h='The path of the file to process.',
            t=str,
            c=None,
            r=True,
            d=None
        ):
            self._option1 = o1
            self._option2 = o2
            self._help = h
            self._type = t
            self._choices = c
            self._required = r
            self._default = d