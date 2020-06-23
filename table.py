from btree import BTree
from collections import OrderedDict


class Column(object):
    def __init__(self, name, constructor):
        self.name = name
        self.ctr = constructor

    def __str__(self):
        return "{}({})".format(self.name, self.ctr)


class Schema(object):
    def __init__(self, key, value):
        self.key = tuple(c if isinstance(c, Column) else Column(*c) for c in key)
        self.value = tuple(c if isinstance(c, Column) else Column(*c) for c in value)

    def __len__(self):
        return len(self.key) + len(self.value)

    def columns(self):
        return self.key + self.value

    def column_names(self):
        return list(c.name for c in self.columns())

    def key_names(self):
        return list(c.name for c in self.key)

    def __str__(self):
        return "{}".format(["{}".format(c) for c in self.columns()])


class Selection(object):
    def __init__(self, source):
        self._source = source

    def __iter__(self):
        yield from (row for row in self._source)

    def filter(self, bool_filter):
        return FilterSelection(self, bool_filter)

    def limit(self, limit):
        return LimitSelection(self, limit)

    def schema(self):
        if isinstance(self._source, Selection):
            return self._source.schema()
        else:
            raise NotImplementedError

    def select(self, columns):
        return ColumnSelection(self, columns)

    def slice(self, bounds):
        if self.supports(bounds):
            return SliceSelection(self, bounds)
        else:
            raise IndexError

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
        columns = self._source.schema().column_names()
        columns = dict(zip(columns, range(len(columns))))
        columns = tuple(columns[c] for c in self._columns)
        for row in self._source:
            yield tuple(row[i] for i in columns)

    def schema(self):
        source_schema = self._source.schema()
        key = [c for c in source_schema.key if c.name in self._columns]
        value = [c for c in source_schema.value if c.name in self._columns]
        return Schema(key, value)


class FilterSelection(Selection):
    def __init__(self, source, bool_filter):
        super().__init__(source)
        self._filter = bool_filter

    def __iter__(self):
        for row in self._source:
            if self._filter(dict(zip(self.schema().column_names(), row))):
                yield row

    def slice(self, bounds):
        if self._source.supports(bounds):
            return FilterSelection(self._source.slice(bounds), self._filter)

        raise IndexError


class LimitSelection(Selection):
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

    def slice(self, _bounds):
        raise IndexError


class MergeSelection(Selection):
    def __init__(self, left, right):
        super().__init__(left)
        self._right = right

    def __iter__(self):
        for key in self._right.select(self.schema().key_names()):
            yield from self._source[key]

    def slice(self, bounds):
        raise IndexError


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
        if isinstance(bounds, tuple):
            bounds = list(bounds)

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
            raise IndexError

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
        self._auxiliary_indices = {}

    def __getitem__(self, bounds):
        yield from self._source[bounds]

    def add_index(self, name, key_columns):
        if name in self._auxiliary_indices:
            raise ValueError

        if set(key_columns) > set(self.schema().column_names()):
            raise ValueError

        key = tuple(c for c in self.schema().columns() if c.name in key_columns)
        value = self.schema().key
        columns = [c.name for c in key + value]
        index = Index(Schema(key, value), self.select(columns))
        self._auxiliary_indices[name] = index

    def insert(self, row):
        assert len(row) == len(self.schema())
        self._source.insert(row)

    def slice(self, bounds):
        if self._source.supports(bounds):
            return SliceSelection(self._source, bounds)

        for index in self._auxiliary_indices.values():
            if index.supports(bounds):
                return MergeSelection(self._source, SliceSelection(index, bounds))

        raise IndexError

    def supports(self, bounds):
        if self._source.supports(bounds):
            return True

        for index in self._auxiliary_indices.values():
            if index.supports(bounds):
                return True

        return False

