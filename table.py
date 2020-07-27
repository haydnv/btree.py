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

    def __str__(self):
        return "{}".format(["{}".format(c) for c in self.columns()])

    def columns(self):
        return self.key + self.value

    def column_names(self):
        return list(c.name for c in self.columns())

    def key_names(self):
        return list(c.name for c in self.key)

    def value_names(self):
        return list(c.name for c in self.value)


class Selection(object):
    def __init__(self, source):
        self._source = source

    def __iter__(self):
        yield from self._source

    def count(self):
        count = 0
        for row in self:
            count += 1

        return count

    def delete(self):
        key_len = len(self.schema().key)
        for row in self:
            key = row[:key_len]
            self._delete_row(key)

    def _delete_row(self, key):
        if isinstance(self._source, Selection):
            return self._source._delete_row(key)
        else:
            raise NotImplementedError

    def filter(self, bool_filter):
        return FilterSelection(self, bool_filter)

    def group_by(self, columns):
        return Aggregate(self, columns)

    def index(self, columns=None):
        if columns is None:
            return Index(self.schema(), self)

        key = [c for c in self.schema().columns() if c.name in columns]
        value = [c for c in self.schema().key if c.name not in columns]
        index_schema = Schema(key, value)
        return ReadOnlyIndex(self, index_schema)

    def limit(self, limit):
        return LimitSelection(self, limit)

    def order_by(self, columns, reverse=False):
        return OrderSelection(self, columns, reverse)

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

    def supports_bounds(self, bounds):
        if isinstance(self._source, Selection):
            return self._source.supports_bounds(bounds)
        else:
            return False

    def supports_order(self, columns):
        if isinstance(self._source, Selection):
            return self._source.supports_order(columns)
        else:
            return False

    def update(self, value):
        key_len = len(self.schema().key)
        for row in self:
            key = row[:key_len]
            self._update_row(key, value)

        return self

    def _update_row(self, key, value):
        if isinstance(self._source, Selection):
            self._source._update_row(key, value)
        else:
            raise NotImplementedError


class Aggregate(object):
    def __init__(self, source, columns):
        if set(columns) > set(source.schema().column_names()):
            raise IndexError

        self._source = source.order_by(columns).select(columns)

    def __iter__(self):
        if not self._source:
            return []

        aggregate = iter(self._source)
        group = next(aggregate)
        yield group

        for row in aggregate:
            if row != group:
                group = row
                yield group


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

    def update(self, value):
        if len(value) != len(self._columns):
            raise ValueError

        if set(value.keys()) > set(self._columns):
            raise ValueError

        self._source.update(value)
        return self


class FilterSelection(Selection):
    def __init__(self, source, bool_filter):
        super().__init__(source)
        self._filter = bool_filter

    def __iter__(self):
        allowed = []
        for row in self._source:
            allow = self._filter(dict(zip(self.schema().column_names(), row)))
            if allow:
                allowed.append(row)
            else:
                yield from allowed
                allowed = []

        yield from allowed

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
        if set(columns) > set(source.schema().column_names()):
            raise IndexError

        assert source.supports_order(columns)
        super().__init__(source)
        self._reverse = reverse

    def __iter__(self):
        if self._reverse:
            yield from self._source.reversed()
        else:
            yield from self._source


class SchemaSelection(Selection):
    def __init__(self, source, schema):
        super().__init__(source)
        self._schema = schema

    def schema(self):
        return self._schema


class TableIndexSliceSelection(Selection):
    def __init__(self, table, index, bounds={}):
        super().__init__(index)
        self._table = table
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

    def reversed(self):
        yield from self._source.reversed(self._bounds)

    def _delete_row(self, key):
        self._table._delete_row(key)

    def _update_row(self, key, value):
        self._table._update_row(key, value)


class Index(Selection):
    def __init__(self, schema, keys=[]):
        assert isinstance(schema, Schema)

        self._schema = schema
        super().__init__(BTree(10, tuple(c.ctr for c in schema.columns()), keys))

    def __bool__(self):
        return len(self._source) > 0

    def __getitem__(self, bounds):
        if isinstance(bounds, tuple):
            bounds = list(bounds)

        yield from self._source[bounds]

    def __delitem__(self, key):
        del self._source[key]

    def __len__(self):
        return len(self._source)

    def contains(self, key):
        return self._source.contains(list(key))

    def delete(self):
        del self._source[:]

    def insert(self, row):
        self._source.insert(row)

    def rebalance(self):
        self._source.rebalance()

    def schema(self):
        return self._schema

    def slice(self, bounds, reverse=False):
        if not self.supports_bounds(bounds):
            raise IndexError

        bounds = convert_bounds(bounds)
        return SchemaSelection(self[bounds], self._schema)

    def supports_bounds(self, bounds):
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

        for c in bounds.values():
            if isinstance(c, slice):
                if c.step is not None:
                    if c.step != 1:
                        return False

        return True

    def supports_order(self, columns):
        return self.schema().column_names()[:len(columns)] == columns

    def reversed(self, bounds=None):
        if bounds is None:
            bounds = slice(None)
        else:
            bounds = convert_bounds(bounds)

        yield from self._source.select(bounds, True)


def convert_bounds(bounds):
    bounds = list(bounds.values())
    if bounds and isinstance(bounds[-1], slice):
        start = []
        stop = []
        for v in bounds[:-1]:
            start.append(v)
            stop.append(v)
        if bounds[-1].start:
            start.append(bounds[-1].start)
        if bounds[-1].stop:
            stop.append(bounds[-1].stop)

        return slice(start, stop)
    else:
        return bounds


class ReadOnlyIndex(Index):
    def __init__(self, source, schema):
        Index.__init__(self, schema, source.select(schema.column_names()))

    def __delitem__(self, key):
        raise NotImplementedError

    def insert(self, _row):
        raise NotImplementedError


class Table(Selection):
    def __init__(self, index):
        assert isinstance(index, Index)

        super().__init__(index)
        self._auxiliary_indices = {}

    def __getitem__(self, bounds):
        yield from self._source[bounds]

    def __len__(self):
        return len(self._source)

    def add_index(self, name, key_columns):
        if name in self._auxiliary_indices:
            raise ValueError

        if set(key_columns) > set(self.schema().column_names()):
            raise ValueError

        columns = {c.name: c for c in self.schema().columns()}
        key = tuple(columns[name] for name in key_columns)
        value = tuple(c for c in self.schema().key if c not in key)
        schema = Schema(key, value)
        index = Index(schema, self.select([c.name for c in key + value]))
        self._auxiliary_indices[name] = index

    def delete(self):
        deleted = self._source.delete()
        for index in self._auxiliary_indices.values():
            index.delete()

    def insert(self, row):
        if len(row) != len(self.schema()):
            raise ValueError

        key = row[:len(self.schema().key)]
        value = row[len(self.schema().key):]

        if self._source.contains(key):
            raise ValueError

        self.upsert(key, value)

    def order_by(self, columns, reverse=False):
        if not self.supports_order(columns):
            raise IndexError

        if self._source.supports_order(columns):
            selection = TableIndexSliceSelection(self, self._source)
            return selection.order_by(columns, reverse)

        for index in self._auxiliary_indices.values():
            if index.supports_order(columns):
                index_slice = TableIndexSliceSelection(self, index)
                return MergeSelection(self, index_slice)

        raise IndexError

    def rebalance(self):
        self._source.rebalance()

    def schema(self):
        return self._source.schema()

    def slice(self, bounds):
        columns = self.schema().column_names()
        if set(bounds.keys()) > set(columns):
            raise IndexError

        bounds = [(c, bounds[c]) for c in columns if c in bounds]
        while bounds and bounds[-1][1] == slice(None):
            bounds = bounds[:-1]

        if self._source.supports_bounds(dict(bounds)):
            return TableIndexSliceSelection(self, self._source, dict(bounds))

        selection = TableIndexSliceSelection(self, self._source, {})
        key_columns = self.schema().key_names()
        while bounds:
            supported = False

            for i in reversed(range(1, len(bounds) + 1)):
                subset = dict(bounds[:i])

                if self._source.supports_bounds(subset):
                    selection = TableIndexSliceSelection(self, self._source, subset)
                    subset = {}
                    bounds = bounds[i:]
                    supported = True
                    break

                for index in self._auxiliary_indices.values():
                    if index.supports_bounds(subset):
                        supported = True
                        index_slice = TableIndexSliceSelection(self, index, subset)
                        selection = MergeSelection(selection, index_slice)
                        bounds = bounds[i:]
                        break

                if supported:
                    break

            if not supported:
                raise IndexError

        return selection

    def supports_bounds(self, bounds):
        if self._source.supports_bounds(bounds):
            return True

        for index in self._auxiliary_indices.values():
            if index.supports_bounds(bounds):
                return True

        return False

    def supports_order(self, columns):
        if self._source.supports_order(columns):
            return True

        for index in self._auxiliary_indices.values():
            if index.supports_order(columns):
                return True

        return False

    def update(self, value):
        if set(value.keys()) > set(self.schema().key_names()):
            raise ValueError

        key_len = len(self.schema().key)
        value_names = self.schema().value_names()
        value_labels = dict(zip(value_names, range(len(value_names))))

        for row in self.index():
            row_key = row[:key_len]
            row_value = row[key_len:]
            new_value = tuple(
                value[c] if c in value else row_value[value_labels[c]]
                for c in value_names)
            if row_value != new_value:
                self.upsert(row_key, new_value)

        return self

    def upsert(self, key, value):
        if len(key) != len(self.schema().key):
            raise IndexError

        if len(value) != len(self.schema().value):
            raise ValueError(value)

        row = key + value
        if not len(row) == len(self.schema()):
            print(row, self.schema())
            raise ValueError

        self._delete_row(key)
        self._source.insert(row)

        if self._auxiliary_indices:
            row = dict(zip(self.schema().column_names(), row))
            for index in self._auxiliary_indices.values():
                index_row = [row[c] for c in index.schema().column_names()]
                index.insert(index_row)

    def _delete_row(self, key):
        key_names = self.schema().key_names()
        if not len(key) == len(key_names):
            raise KeyError

        key_dict = dict(zip(key_names, key))
        row = list(self.slice(key_dict))
        if not row:
            return
        elif len(row) > 1:
            raise IndexError("{} has {} keys".format(key, len(row)))
        else:
            row = row[0]

        del self._source[key]
        if self._auxiliary_indices:
            row = dict(zip(self.schema().column_names(), row))
            for index in self._auxiliary_indices.values():
                index_key = [row[c] for c in index.schema().key_names()]
                del index[index_key]

    def _update_row(self, key, value):
        value_names = self.schema().value_names()
        if set(value.keys()) > set(value_names):
            raise ValueError

        row = list(self._source[key])
        if not row:
            raise ValueError
        elif len(row) > 1:
            raise RuntimeError("key {} has {} rows".format(key, len(row)))
        else:
            row = row[0]

        value_index = dict(zip(value_names, range(len(value_names))))
        old_value = row[-len(value_names):]
        new_value = tuple(
            value[c] if c in value else old_value[value_index[c]]
            for c in value_names)
        self.upsert(key, new_value)

