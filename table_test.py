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


def test_limit():
    pk = (("key", int),)
    t = new_table(pk, tuple())
    for i in range(50):
        t.insert((i,))

    actual = list(t.slice({"key": slice(10, 20)}).limit(5))
    assert actual == [(10,), (11,), (12,), (13,), (14,)]


def test_filter():
    pk = (("key", str),)
    cols = (("value", int),)
    t = new_table(pk, cols)
    t.insert(("one", 1))
    t.insert(("two", 2))
    t.insert(("three", 3))

    actual = list(t.filter(lambda r: r["key"] == "two").select(["value"]))
    assert actual == [(2,)]


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
        .slice({"one": "Four"})
        .limit(2)
        .select(["one"])
    )
    assert actual == [("Four",)]

if __name__ == "__main__":
    test_select_all()
    test_pk_range()
    test_limit()
    test_filter()
    test_add_index()
    test_chaining()
    print("PASS")

