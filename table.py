from btree import BTree
from collections import OrderedDict


class Column(object):
    def __init__(self, name, constructor):
        self.name = name
        self.ctr = constructor


class Schema(object):
    def __init__(self, key, value):
        self.key = tuple(Column(*c) for c in key)
        self.value = tuple(Column(*c) for c in value)

    def __len__(self):
        return len(self.key) + len(self.value)

    def columns(self):
        return self.key + self.value

    def column_names(self):
        return list(c.name for c in self.columns())

    def key_names(self):
        return list(c.name for c in self._key)


class Selection(object):
    def __init__(self, source):
        self._source = source

    def __iter__(self):
        yield from (row for row in self._source)

    def filter(self, bool_filter):
        return FilterSelection(self, bool_filter)

    def limit(self, limit):
        return LimitedSelection(self, limit)

    def schema(self):
        if isinstance(self._source, Selection):
            return self._source.schema()
        else:
            raise NotImplementedError

    def select(self, columns):
        return ColumnSelection(self, columns)

    def slice(self, bounds):
        return SliceSelection(self, bounds)

    def supports(self, bounds):
        if isinstance(self._source, Selection):
            return self._source.supports(bounds)
        else:
            return False

    def order_by(self, columns, reverse=False):
        raise NotImplementedError


class ColumnSelection(Selection):
    def __init__(self, source, columns):
        super().__init__(source)
        self._columns = columns

    def __iter__(self):
        columns = self.schema().column_names()
        columns = tuple(i for i in range(len(columns)) if columns[i] in self._columns)
        for row in self._source:
            yield tuple(row[i] for i in columns)


class FilterSelection(Selection):
    def __init__(self, source, bool_filter):
        self._source = source
        self._filter = bool_filter

    def __iter__(self):
        for row in self._source:
            if self._filter(dict(zip(self.schema().column_names(), row))):
                yield row


class LimitedSelection(object):
    def __init__(self, source, limit):
        self._source = source
        self._limit = limit

    def __iter__(self):
        i = 0
        for row in self._source:
            yield row

            i += 1
            if i == self._limit:
                break


class SliceSelection(Selection):
    def __init__(self, source, bounds):
        super().__init__(source)
        self._bounds = bounds

    def __iter__(self):
        yield from self._source.slice(self._bounds)


class Index(Selection):
    def __init__(self, schema, keys=[]):
        self._schema = schema
        super().__init__(BTree(10, tuple(c.ctr for c in schema.columns()), keys))

    def __getitem__(self, bounds):
        yield from self._source[bounds]

    def insert(self, row):
        self._source.insert(row)

    def schema(self):
        return self._schema

    def slice(self, bounds):
        if not self.supports(bounds):
            raise NotImplementedError

        bounds = list(bounds.values())
        if isinstance(bounds[-1], slice):
            start = []
            stop = []
            for v in bounds[:-1]:
                start.append(v)
                stop.append(v)
            if bounds[-1].start:
                start.append(bounds[-1].start)
            if bounds[-1].stop:
                stop.append(bounds[-1].stop)

            return self[slice(start, stop)]
        else:
            return self[bounds]

    def supports(self, bounds):
        if any(callable(v) for v in bounds.values()):
            return False

        columns = self.schema().column_names()
        requested = list(bounds.keys())
        if requested != columns[:len(requested)]:
            return False

        for c in list(bounds.values())[:-1]:
            if isinstance(c, slice):
                return False

        return True


class Table(Selection):
    def __init__(self, index):
        super().__init__(index)

    def __getitem__(self, bounds):
        yield from self._source[bounds]

    def insert(self, row):
        assert len(row) == len(self.schema())
        self._source.insert(row)

    def slice(self, bounds):
        if self._source.supports(bounds):
            return SliceSelection(self._source, bounds) 

        raise IndexError


def test_select_all():
    pk = (("one", str), ("two", int))
    cols = (("three", float), ("four", complex))
    t = Table(Index(Schema(pk, cols)))
    row1 = ("test", 1, 2., 3+1j)
    row2 = ("test2", 4, 5., 6-1j)
    t.insert(row1)
    t.insert(row2)
    assert list(t) == [row1, row2]


def test_pk_range():
    pk = (("one", int), ("two", int))
    t = Table(Index(Schema(pk, [])))
    t.insert([1, 1])
    t.insert([1, 2])
    t.insert([2, 2])

    actual = list(t.slice({"one": 1, "two": 2}))
    assert actual == [(1, 2)]

    actual = list(t.slice({"one": slice(1, 2)}))
    assert actual == [(1, 1), (1, 2)]

    actual = list(t.slice({"one": 1, "two": slice(1, 3)}))
    assert actual == [(1, 1), (1, 2)]


def test_select_column():
    pk = (("one", str), ("two", int))
    t = Table(Index(Schema(pk, (("three", str),))))
    t.insert(("test", 2, "value"))

    actual = list(t.slice({"one": "test"}).select(["three"]))
    assert actual == [("value",)]


def test_limit():
    pk = (("key", int),)
    t = Table(Index(Schema(pk, tuple())))
    for i in range(50):
        t.insert((i,))

    actual = list(t.slice({"key": slice(10, 20)}).limit(5))
    assert actual == [(10,), (11,), (12,), (13,), (14,)]


def test_filter():
    pk = (("key", str),)
    cols = (("value", int),)
    t = Table(Index(Schema(pk, cols)))
    t.insert(("one", 1))
    t.insert(("two", 2))
    t.insert(("three", 3))

    actual = list(t.filter(lambda r: r["key"] == "two"))
    assert actual == [("two", 2)]


if __name__ == "__main__":
    test_select_all()
    test_pk_range()
    test_select_column()
    test_limit()
    test_filter()

