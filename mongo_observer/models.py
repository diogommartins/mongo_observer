from pathdict.collection import PathDict, StringIndexableList


class Operations:
    INSERT = 'i'
    UPDATE = 'u'
    DELETE = 'd'
    COMMAND = 'c'
    DB_DECLARE = 'db'
    NO_OP = 'n'


class NullableList(StringIndexableList):
    def __delitem__(self, key):
        if isinstance(key, str):
            key = int(key)
        self[key] = None


class Document(PathDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs,
                         create_if_not_exists=True,
                         list_class=NullableList)
