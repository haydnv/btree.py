from btree import BTree
from collections import deque, OrderedDict


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
        yield from self._source

    def derive(self, name, func, return_type):
        return DeriveSelection(self, name, func, return_type)

    def filter(self, bool_filter):
        return FilterSelection(self, bool_filter)

    def index(self, new_key=None):
        if new_key is None:
            return Index(self.schema(), self)

        new_key = [c for c in self.schema().columns() if c.name in new_key]
        return Index(self.schema(), self)

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
        if set(columns) > set(self.schema().column_names()):
            raise IndexError

        return OrderSelection(self, columns, reverse)


class ColumnSelection(Selection):
    def __init__(self, source, columns):
        if set(columns) > set(source.schema().column_names()):
            raise IndexError

        super().__init__(source)
        self._columns = columns

    def __getitem__(self, key):
        key_names = self.schema().key_names()
        if len(key) > len(key_names):
            raise IndexError

        key = {key_names[i]: key[i] for i in range(len(key))}
        key = tuple(key.get(c, slice(None)) for c in self._source.schema().column_names())

        columns = self._column_indices()
        for row in self._source[key]:
            yield tuple(row[i] for i in columns)

    def __iter__(self):
        columns = self._column_indices()
        for row in self._source:
            yield tuple(row[i] for i in columns)

    def schema(self):
        source_columns = {c.name: c for c in self._source.schema().columns()}
        return Schema([source_columns[c] for c in self._columns], [])

    def _column_indices(self):
        columns = self._source.schema().column_names()
        columns = dict(zip(columns, range(len(columns))))
        return tuple(columns[c] for c in self._columns)


class DeriveSelection(Selection):
    def __init__(self, source, name, func, return_type):
        super().__init__(source)
        self._derive = (name, return_type)
        self._func = func

    def __getitem__(self, key):
        columns = self._source.schema().column_names()
        for row in self._source[key]:
            row_dict = dict((columns[i], row[i]) for i in range(len(columns)))
            yield row + (self._func(row_dict),)

    def __iter__(self):
        columns = self._source.schema().column_names()
        for row in self._source:
            row_dict = dict((columns[i], row[i]) for i in range(len(columns)))
            yield row + (self._func(row_dict),)
 
    def schema(self):
        source = self._source.schema()
        return Schema(source.key, source.value + (self._derive,))

    def slice(self, bounds):
        return DeriveSelection(
            self._source.slice(bounds), self._derive[0], self._func, self._derive[1])


class FilterSelection(Selection):
    def __init__(self, source, bool_filter):
        super().__init__(source)
        self._filter = bool_filter

    def __iter__(self):
        for row in self._source:
            if self._filter(dict(zip(self.schema().column_names(), row))):
                yield row

    def slice(self, bounds):
        return FilterSelection(self._source.slice(bounds), self._filter)


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

    def __getitem__(self, key):
        right = self._right.select(self.schema().key_names())
        for k in right[key]:
            yield from self._source[key]

    def __iter__(self):
        key_names = self.schema().key_names()
        for key in self._right.select(key_names):
            yield from self._source[key]


class OrderSelection(Selection):
    def __init__(self, source, columns, reverse):
        index = source.index(columns)
        super().__init__(index)
        self._reverse = reverse

    def __iter__(self):
        if self._reverse:
            yield from self._source.reversed(slice(None))
        else:
            yield from self._source


class SchemaSelection(Selection):
    def __init__(self, schema, source):
        super().__init__(source)
        self._schema = schema

    def schema(self):
        return self._schema


class SliceSelection(Selection):
    def __init__(self, source, bounds):
        super().__init__(source)
        self._bounds = bounds

    def __getitem__(self, key):
        if self.contains(key):
            return self._source[key]
        else:
            return []

    def __iter__(self):
        yield from self._source.slice(self._bounds)

    def contains(self, key):
        columns = self.schema().column_names()
        if len(key) > len(columns):
            raise IndexError

        key = OrderedDict([(columns[i], key[i]) for i in range(len(key))])
        for col, val in key.items():
            if col in self._bounds:
                bound = self._bounds[col]
                if isinstance(bound, slice):
                    if bound.start and bound.start > val:
                        return False
                    if bound.stop and bound.stop <= val:
                        return False
                elif self._bounds[col] < val or self._bounds[col] > val:
                    return False

        return True

    def supports(self, bounds):
        return self._source.supports(bounds)


class Index(Selection):
    def __init__(self, schema, keys=[]):
        self._schema = schema
        super().__init__(BTree(10, tuple(c.ctr for c in schema.columns()), keys))

    def __getitem__(self, bounds):
        if isinstance(bounds, tuple):
            bounds = list(bounds)

        yield from self._source[bounds]

    def contains(self, key):
        return self._source.contains(list(key))

    def insert(self, row):
        self._source.insert(row)

    def schema(self):
        return self._schema

    def slice(self, bounds, reverse=False):
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

            return SchemaSelection(self._schema, self[slice(start, stop)])
        else:
            return SchemaSelection(self._schema, self[bounds])

    def supports(self, bounds):
        if set(bounds.keys()) > set(self.schema().column_names()):
            return False

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

    def reversed(self, bounds):
        yield from self._source.select(bounds, True)


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
        if self._source.contains(row[:len(self.schema().key)]):
            raise ValueError

        self._source.insert(row)

        if self._auxiliary_indices:
            row = dict(zip(self.schema().column_names(), row))
            for index in self._auxiliary_indices.values():
                index_row = [row[c] for c in index.schema().column_names()]
                index.insert(index_row)

    def slice(self, bounds):
        columns = self.schema().column_names()
        if set(bounds.keys()) > set(columns):
            raise IndexError

        bounds = [(c, bounds[c]) for c in columns if c in bounds]
        while bounds[-1][1] == slice(None):
            bounds = bounds[:-1]

        if self._source.supports(dict(bounds)):
            return SliceSelection(self._source, dict(bounds))

        selection = self._source
        key_columns = self.schema().key_names()
        while bounds:
            supported = False

            for i in reversed(range(1, len(bounds) + 1)):
                subset = dict(bounds[:i])

                if self._source.supports(subset):
                    selection = SliceSelection(self._source, subset)
                    subset = {}
                    bounds = bounds[i:]
                    supported = True
                    break

                for index in self._auxiliary_indices.values():
                    if index.supports(subset):
                        supported = True
                        index_slice = SliceSelection(index, subset)
                        selection = MergeSelection(selection, index_slice)
                        bounds = bounds[i:]
                        break

                if supported:
                    break

            if not supported:
                raise IndexError

        return selection

    def supports(self, bounds):
        if self._source.supports(bounds):
            return True

        for index in self._auxiliary_indices.values():
            if index.supports(bounds):
                return True

        return False

