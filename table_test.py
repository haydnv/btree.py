import itertools

from table import Index, Schema, Table


def new_table(key, value=[]):
    return Table(Index(Schema(key, value)))


def test_select_all():
    pk = (("one", str), ("two", int))
    cols = (("three", float), ("four", complex))
    t = new_table(pk, cols)
    row1 = ("test", 1, 2., 3+1j)
    row2 = ("test2", 4, 5., 6-1j)
    t.insert(row1)
    t.insert(row2)
    assert list(t) == [row1, row2]


def test_pk_range():
    pk = (("one", int), ("two", int))
    t = new_table(pk)
    t.insert([1, 1])
    t.insert([1, 2])
    t.insert([2, 2])

    actual = list(t.slice({"one": 1, "two": 2}))
    assert actual == [(1, 2)]

    actual = list(t.slice({"one": slice(1, 2)}))
    assert actual == [(1, 1), (1, 2)]

    actual = list(t.slice({"one": 1, "two": slice(1, 3)}))
    assert actual == [(1, 1), (1, 2)]


def test_tuple_range():
    pk = (("type", str), ("key", tuple))
    values = (("id", int),)
    t = new_table(pk, values)
    t.add_index("id", ("id",))

    t.insert(("node", (1,), 1))

    expected = [("node", (1,), 1)]

    rows = list(t.slice({"type": "node", "key": (1,)}))
    assert rows == expected

    assert list(t.slice({"id": 1})) == expected


def test_limit():
    pk = (("key", int),)
    t = new_table(pk, tuple())
    for i in range(50):
        t.insert((i,))

    actual = list(t.slice({"key": slice(10, 20)}).limit(5))
    assert actual == [(10,), (11,), (12,), (13,), (14,)]


def test_filter():
    pk = (("key1", str), ("key2", int))
    cols = (("value", int),)
    t = new_table(pk, cols)
    t.insert(("one", 1, 1))
    t.insert(("two", 2, 2))
    t.insert(("three", 2, 3))

    actual = list(t.filter(lambda r: r["key1"] == "two").select(["value"]))
    assert actual == [(2,)]


def test_column_select():
    pk = (("one", int),)
    values = (("two", int), ("three", int))
    t = new_table(pk, values)

    for i in range(2):
        t.insert((i, i * 10, i * 100))

    actual = list(t
        .select(["one", "two", "three"])
        .select(["two", "one", "three"])
        .select(["one", "two"]))[-1]
    assert actual == (1, 10)


def test_add_index():
    pk = (("one", str),)
    cols = (("two", int), ("three", str))
    t = new_table(pk, cols)
    t.insert(("One", 2, "Three"))
    t.insert(("Four", 5, "Six"))
    t.add_index("aux", ["two"])
    t.insert(("Seven", 8, "Nine"))

    actual = list(t.slice({"two": slice(2, 8)}))
    assert actual == [("One", 2, "Three"), ("Four", 5, "Six")]


def test_ordering():
    pk = (("one", int), ("two", int), ("three", int))
    t = new_table(pk, [("four", str)])
    for i in range(10):
        for j in range(20, 30):
            t.insert((i, j, i * j, "test"))

    t.add_index("i1", ["one"])
    t.add_index("i2", ["two"])
    t.add_index("i3", ["three"])

    actual = list(t
        .slice({"one": slice(0, 2)})
        .order_by(["one", "two"], reverse=True)
        .select(["one", "two"])
        .limit(3))
    assert actual == [(1, 29), (1, 28), (1, 27)]

    actual = list(t
        .order_by(["two", "one"])
        .select(["two", "one"])
        .limit(3))
    assert actual == [(20, 0), (21, 0), (22, 0)]

    actual = list(t.order_by(["three", "two"]).limit(3).select(["three", "two"]))
    assert actual == [(0, 20), (20, 20), (40, 20)]


def test_slice():
    pk = (("key1", str), ("key2", int))
    cols = (("value", int),)
    t = new_table(pk, cols)
    t.insert(("one", 1, 1))
    t.insert(("two", 2, 2))
    t.insert(("two", 4, 4))
    t.insert(("three", 3, 3))

    actual = list(t.slice({"key1": "two"}).select(["value"]))
    assert actual == [(2,), (4,)]

    actual = list(
        t
            .filter(lambda r: r["key2"] == 4)
            .slice({"key1": "two"})
            .select(["value"]))
    assert actual == [(4,)]


def test_slice_multiple_keys():
    pk = (("a", int), ("b", int), ("c", int), ("d", int))
    t = new_table(pk, [])
    t.add_index("i1", ["a"])
    t.add_index("i2", ["b"])
    t.add_index("i3", ["c"])
    t.add_index("i4", ["d"])

    for i, j, k, l in itertools.product(*[range(5) for _ in range(4)]):
        t.insert((i, j, k, l))

    actual = list(t.slice({"a": slice(2), "b": 1, "d": slice(1, 4)}))
    assert len(set(actual)) == 2 * 1 * 5 * 3


def test_chaining():
    pk = (("one", str), ("two", int))
    cols = (("three", int),)
    t = new_table(pk, cols)
    t.insert(("One", 2, 3))
    t.insert(("Four", 5, 6))
    t.add_index("aux", ["three"])
    t.insert(("Seven", 8, 9))

    actual = list(t
        .filter(lambda row: row["three"] > 3)
        .slice({"three": slice(2, 9)})
        .limit(1)
        .index()
        .slice({"one": "Four"})
        .limit(2)
        .select(["one"])
    )
    assert actual == [("Four",)]


def test_group_by():
    pk = (("one", int), ("two", int))
    cols = (("three", int),)
    t = new_table(pk, cols)
    t.add_index("i1", ["three"])
    t.add_index("i2", ["two", "three"])

    for i in range(10):
        t.insert((i, i * 10, i % 3))

    actual = list(t.group_by(["one", "two"]))
    assert len(actual) == 10

    actual = list(t.group_by(["three"]))
    assert actual == [(0,), (1,), (2,)]

    actual = list(t.group_by(["two", "three"]))
    assert len(actual) == 10


def test_update():
    pk = (("one", int), ("two", str))
    cols = (("three", str), ("four", int))
    t = new_table(pk, cols)
    t.add_index("aux", ["four"])

    t.upsert((1, "upsert"), ("col3", 4))
    assert list(t) == [(1, "upsert", "col3", 4)]

    t.insert((2, "insert", "col3-2", 5))
    t.update({"three": "new"})
    for val in t.select(["three"]):
        assert val == ("new",)

    actual = list(t.slice({"four": 4}).update({"three": "old"}).select(["three"]))
    assert actual == [("old",)]
    assert len(t) == 2

    actual = list(t.slice({"four": 4}).update({"four": 3}).select(["four"]))
    assert actual == []
    assert len(t) == 2

    actual1 = list(t.slice({"four": slice(None)}).select(["four"]))
    actual2 = list(t.select(["four"]))
    expected = [(3,), (5,)]
    assert actual1 == expected
    assert actual2 == expected

    t.rebalance()
    actual1 = list(t.slice({"four": slice(None)}).select(["four"]))
    actual2 = list(t.select(["four"]))
    expected = [(3,), (5,)]
    assert actual1 == expected
    assert actual2 == expected


def test_delete():
    pk = (("a", int),)
    cols = (("b", int), ("c", int))
    t = new_table(pk, cols)
    t.add_index("b", ["b"])

    for i in range(10):
        t.insert((i, i, i))

    assert len(t) == 10

    t.slice({"a": slice(2)}).delete()
    assert len(t) == 8
    assert list(t.select(["a"])) == [(i,) for i in range(2, 10)]

    t.slice({"b": slice(5, 9)}).delete()
    actual = list(t.select(["a"]))
    assert actual == [(2,), (3,), (4,), (9,)]

    t.filter(lambda r: r["a"] % 2 == 0).slice({"a": slice(3)}).delete()
    actual = list(t)
    assert actual == [(3, 3, 3), (4, 4, 4), (9, 9, 9)]

    t.delete()
    assert len(t) == 0
    assert len(list(t)) == 0
    assert len(list(t.slice({"b": slice(0, 10)}))) == 0

    t.rebalance()
    assert len(t) == 0
    assert len(list(t)) == 0
    assert len(list(t.slice({"b": slice(0, 10)}))) == 0


if __name__ == "__main__":
    test_select_all()
    test_pk_range()
    test_tuple_range()
    test_limit()
    test_filter()
    test_column_select()
    test_add_index()
    test_ordering()
    test_slice()
    test_slice_multiple_keys()
    test_chaining()
    test_group_by()
    test_update()
    test_delete()
    print("PASS")

