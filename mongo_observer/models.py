from pathdict import PathDict


class Operations:
    INSERT = 'i'
    UPDATE = 'u'
    DELETE = 'd'
    COMMAND = 'c'
    DB_DECLARE = 'db'
    NO_OP = 'n'


class Document(PathDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, create_if_not_exists=True)
