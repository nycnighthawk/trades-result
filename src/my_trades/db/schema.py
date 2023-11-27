# module that deals with high level DB representation like table, column

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum, auto
from typing import Any, Callable

__all__ = [
    "Null",
    "null",
    "Blob",
    "TimeStamp",
    "DB_DATA_VALUE",
    "DataType",
    "Action",
    "ForeignKey",
    "Schema",
    "Table",
]


class Null:
    pass


null = Null()


class Blob:
    pass


@dataclass
class TimeStamp:
    value: int | float

    def __post_init__(self):
        assert self.value >= 0


DB_DATA_VALUE = Null | int | float | Blob | datetime | date | TimeStamp


class DataType(Enum):
    """represent the data type available in a typical RDBMS"""

    text = auto()
    integer = auto()
    real = auto()
    blob = auto()
    date = auto()
    datetime = auto()
    timestamp = auto()


class Action(Enum):
    """represents the action available in a typical RDBMS"""

    nothing = auto()
    restrict = auto()
    null = auto()
    default = auto()
    cascade = auto()


class CaseInsensitiveDict(dict):
    """A custom dictionary data structure. If the key is non-string type,
    it behaves like a normal dictionary. If the key is a string type, the
    entry can be retrieved using in a case-insensitive way. In addition,
    if the key is string type that conforms with python identifer, then it
    can be referenced using 'dot' format in addition to the 'bracket' format.

    Usage Example:
    >>> d = CaseInsensitiveDict({'efg':1, 'xyz': 2, 'eFG': 2})
    >>> d
    {'efg': 2, 'xyz': 2}
    >>> d['abc']=1
    >>> d['ABC']
    1
    >>> d.abc
    1
    >>> d.Abc
    1
    >>> d['aBc'] = 2
    >>> d['abc']
    2
    >>> d
    {'efg': 2, 'xyz': 2, 'abc': 2}
    >>>
    """

    def __init__(self, d: dict = None):
        _name_map = {}
        super().__setattr__("_name_map", _name_map)
        if d is not None:
            for k, v in d.items():
                self.__setitem__(k, v)

    def __setitem__(self, k: Any, v: Any):
        if isinstance(k, str):
            if k.lower() in self._name_map:
                super().__setitem__(self._name_map[k.lower()], v)
            else:
                self._name_map[k.lower()] = k
                super().__setitem__(k, v)
        else:
            super().__setitem__(k, v)

    def __getitem__(self, k):
        if isinstance(k, str) and k.lower() in self._name_map:
            return super().__getitem__(self._name_map[k.lower()])
        return super().__getitem__(k)

    def __getattribute__(self, a):
        name_map = super().__getattribute__("_name_map")
        if a.lower() in name_map:
            return self[name_map[a.lower()]]
        return super().__getattribute__(a)

    def __setattr__(self, a: str, v):
        if a.lower() in self._name_map:
            self[self._name_map[a.lower()]] = v
        else:
            self[a] = v

    def __contains__(self, key: object) -> bool:
        if isinstance(key, str):
            return key.lower() in self._name_map
        return super().__contains__(key)


class Table:
    """
    represent a table in a typical RDBMS
    """

    def __init__(self, name: str, *columns: "Column"):
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "c", CaseInsensitiveDict())
        for column in columns:
            self.add_column(column)

    def __getattribute__(self, a: str):
        if a == "c" or a == "C":
            return object.__getattribute__(self, "C")
        return object.__getattribute__(self, a)

    def __setattr__(self, a, v):
        raise AttributeError(
            f"Cannot modify table {self.table_name} after its created "
            f'with attribute "{a}" to value: {v}'
        )

    def add_column(self, column: "Column"):
        if column.name in self.c:
            raise ValueError(
                f"Duplicate column name: {column.name} in table: {self.name}"
            )
        self.c[column.name] = column

    def __repr__(self):
        column_objs_repr: list[str] = []
        for _, column in self.C.items():
            column_objs_repr.append(
                f"{column.name}:{column.data_type.__str__()}"
            )
        return """Table(name={}, C=[{}])""".format(
            self.name, ", ".join(column_objs_repr)
        )


@dataclass
class ForeignKey:
    table: Table
    column: str
    delete_action: Action = Action.cascade
    update_action: Action = Action.cascade


def assert_proper_value(
    value: DB_DATA_VALUE,
    value_type: int | float | date | TimeStamp | datetime | str | Blob,
    nullable: bool,
):
    if nullable:
        assert isinstance(value, value_type) or isinstance(value, Null)
    else:
        assert isinstance(value, value_type)


@dataclass
class Column:
    name: str
    data_type: DataType
    default_value: DB_DATA_VALUE | None | Callable[[], DB_DATA_VALUE] = None
    nullable: bool = False
    primary_key: bool = False
    foreign_key: ForeignKey | None = None

    def __post_init__(self):
        if self.default_value is not None:
            default_value = self.default_value
            if callable(self.default_value):
                default_value = self.default_value()
            if self.data_type == DataType.integer:
                assert_proper_value(default_value, int, self.nullable)
            elif self.data_type == DataType.real:
                assert_proper_value(default_value, float, self.nullable)
            elif self.data_type == DataType.date:
                assert_proper_value(default_value, date, self.nullable)
            elif self.data_type == DataType.datetime:
                assert_proper_value(default_value, datetime, self.nullable)
            elif self.data_type == DataType.blob:
                assert_proper_value(default_value, Blob, self.nullable)
            elif self.data_type == DataType.timestamp:
                assert_proper_value(default_value, TimeStamp, self.nullable)


@dataclass
class Schema:
    name: str
    tables: CaseInsensitiveDict[str, Table]

    def __init__(self, name):
        self.name = name
        self.tables = CaseInsensitiveDict()

    def __post_init__(self):
        self.table_names = set()

    def table(self, name, *columns: Column) -> Table:
        if name in self.tables:
            raise ValueError(
                f"Cannot create the same table name in schema: {self.name}"
            )
        self.tables[name] = Table(name, *columns)
