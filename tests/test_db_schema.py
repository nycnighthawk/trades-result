import my_trades.db.schema as S
import pytest


@pytest.fixture
def case_insensitive_test_data():
    return (
        S.CaseInsensitiveDict({"abc": 1, "efg": 2, "ABC": 2}),
        S.CaseInsensitiveDict(),
    )


def test_cls_case_insensitive_dict(case_insensitive_test_data):
    d = case_insensitive_test_data[0]
    assert d.abc == 2
    assert d["efg"] == 2
    d.abc = "this"
    assert d.Abc == d["aBc"] == "this"

    # test other type of keys
    d[123] = "xyz"
    assert d[123] == "xyz"
    assert len(d) == 3

    # test with an empty dict and attribute set/get
    d = case_insensitive_test_data[1]
    d.aBc = "abc"
    assert d.Abc == d["abc"]
    assert "aBc" not in d.__dict__
    assert "aBc" == d._name_map["abc"]
    assert len(d) == 1
    d["abc"] = "xyz"
    assert d.aBc == "xyz"
    assert "abc" in d
    assert "AbC" in d


def test_table():
    t1 = S.Table(
        "t1",
        S.Column("c1", S.DataType.integer, True),
        S.Column("c2", S.DataType.text),
    )
    t2 = S.Table(
        "t2",
        S.Column("c1", S.DataType.text),
        S.Column("c2", S.DataType.integer, foreign_key=S.ForeignKey(t1, "c1")),
    )
