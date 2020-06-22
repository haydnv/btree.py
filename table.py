from btree import BTree
from collections import OrderedDict


class Index(object):
    def __init__(self, schema):
        self._schema = schema
        self._btree = BTree(10, tuple(schema.values()))

    def __getitem__(self, selector):
        return self._btree[selector]

    def insert(self, row):
        self._btree.insert(row)

    def select(self, cond):
        if not self.supports(cond):
            raise IndexError

        cond = list(cond.values())
        if isinstance(cond[-1], slice):
            start = []
            stop = []
            for v in cond[:-1]:
                start.append(v)
                stop.append(v)
            if cond[-1].start:
                start.append(cond[-1].start)
            if cond[-1].stop:
                stop.append(cond[-1].stop)

            return self[slice(start, stop)]
        else:
            return self[cond]

    def supports(self, cond):
        if any(callable(v) for v in cond.values()):
            return False

        columns = list(self._schema.keys())
        requested = list(cond.keys())
        if requested != columns[:len(requested)]:
            return False

        for c in list(cond.values())[:-1]:
            if isinstance(c, slice):
                return False

        return True


class Selection(object):
    def __init__(self, rows, source_columns, dest_columns):
        self._rows = rows
        self._source_columns = source_columns
        self._columns = dest_columns

    def __iter__(self):
        for row in self._rows:
            assert len(row) == len(self._source_columns)
            row = dict(zip(self._source_columns, row))
            yield {k: v for k, v in row.items() if k in self._columns}


class Table(object):
    def __init__(self, key, value):
        self._key = key
        self._value = value
        self._primary = Index(OrderedDict(tuple(key) + tuple(value)))

    def insert(self, row):
        assert len(row) == len(self._key) + len(self._value)
        self._primary.insert(row)

    def select(self, columns=None, cond=None):
        if columns is None:
            columns = list(self._primary._schema.keys())

        if cond is None:
            source_columns = list(self._primary._schema.keys())
            return Selection(self._primary[:], source_columns, columns)

        if self._primary.supports(cond):
            source_columns = list(self._primary._schema.keys())
            rows = self._primary.select(cond)
            return Selection(rows, source_columns, columns)

        raise IndexError


def test_select_all():
    pk = (("one", str), ("two", int))
    cols = (("three", float), ("four", complex))
    t = Table(pk, cols)
    row1 = ("test", 1, 2., 3+1j)
    row2 = ("test2", 4, 5., 6-1j)
    t.insert(row1)
    t.insert(row2)
    assert list(t.select()) == [{
        "one": "test",
        "two": 1,
        "three": 2.,
        "four": 3+1j,
    }, {
        "one": "test2",
        "two": 4,
        "three": 5.,
        "four": 6-1j,
    }]


def test_pk_range():
    pk = (("one", int), ("two", int))
    t = Table(pk, [])
    t.insert([1, 1])
    t.insert([1, 2])
    t.insert([2, 2])

    actual = list(t.select(cond={"one": 2, "two": 2}))
    assert actual == [{"one": 2, "two": 2}]

    actual = list(t.select(cond={"one": slice(1, 2)}))
    assert actual == [{"one": 1, "two": 1}, {"one": 1, "two": 2}]

    actual = list(t.select(cond={"one": 1, "two": slice(1, 2)}))
    assert actual == [{"one": 1, "two": 1}]


def test_column_filter():
    pk = (("one", str), ("two", int))
    t = Table(pk, (("three", str),))
    t.insert(("test", 2, "value"))

    actual = list(t.select(["three"], {"one": "test"}))
    assert actual == [{"three": "value"}]


if __name__ == "__main__":
    test_select_all()
    test_pk_range()
    test_column_filter()

