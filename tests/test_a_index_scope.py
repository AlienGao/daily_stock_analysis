from types import SimpleNamespace
from datetime import date

from src.services.a_index_new_high_service import AIndexNewHighService, is_allowed_a_index


def test_a_index_scope_accepts_broad_and_industry_categories():
    assert is_allowed_a_index("规模指数", "000300.SH")
    assert is_allowed_a_index("综合指数", "000001.SH")
    assert is_allowed_a_index("行业指数", "801010.SI")


def test_a_index_scope_rejects_theme_style_and_strategy_categories():
    assert not is_allowed_a_index("主题指数", "931001.CSI")
    assert not is_allowed_a_index("风格指数", "931002.CSI")
    assert not is_allowed_a_index("策略指数", "931003.CSI")
    assert not is_allowed_a_index("行业指数", "000932.SH")
    assert not is_allowed_a_index("行业指数", "801011.SI")


def test_a_index_scope_keeps_core_broad_indices_for_legacy_metadata():
    assert is_allowed_a_index(None, "000300.SH")
    assert is_allowed_a_index("", "399006.SZ")
    assert not is_allowed_a_index(None, "000015.SH")


class _Query:
    def __init__(self, rows):
        self.rows = rows

    def order_by(self, *_args):
        return self

    def all(self):
        return self.rows


class _Session:
    def __init__(self, rows):
        self.rows = rows

    def query(self, *_args):
        return _Query(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class _Db:
    def __init__(self, rows):
        self.rows = rows

    def get_session(self):
        return _Session(self.rows)


def test_list_indices_filters_existing_legacy_rows():
    rows = [
        SimpleNamespace(
            ts_code="000300.SH", category=None,
            to_dict=lambda: {"ts_code": "000300.SH", "category": None},
        ),
        SimpleNamespace(
            ts_code="801010.SI", category="行业指数",
            to_dict=lambda: {"ts_code": "801010.SI", "category": "行业指数"},
        ),
        SimpleNamespace(
            ts_code="000015.SH", category=None,
            to_dict=lambda: {"ts_code": "000015.SH", "category": None},
        ),
        SimpleNamespace(
            ts_code="931001.CSI", category="主题指数",
            to_dict=lambda: {"ts_code": "931001.CSI", "category": "主题指数"},
        ),
    ]
    service = object.__new__(AIndexNewHighService)
    service.db = _Db(rows)

    assert [item["ts_code"] for item in service.list_indices()] == [
        "000300.SH", "801010.SI",
    ]


def test_clear_non_allowed_data_deletes_only_out_of_scope_rows(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from src.storage import Base, IndexBasic, IndexConstituent, IndexDaily, IndexWeekly

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            IndexBasic.__table__, IndexDaily.__table__, IndexWeekly.__table__,
            IndexConstituent.__table__,
        ],
    )
    session_factory = sessionmaker(bind=engine)

    class _SqlDb:
        def get_session(self):
            session = session_factory()

            class _Context:
                def __enter__(self):
                    return session

                def __exit__(self, exc_type, *_args):
                    if exc_type:
                        session.rollback()
                    session.close()

            return _Context()

    with _SqlDb().get_session() as session:
        session.add_all([
            IndexBasic(ts_code="000300.SH", name="沪深300", category="规模指数"),
            IndexBasic(ts_code="801010.SI", name="申万农林牧渔", category="行业指数"),
            IndexBasic(ts_code="931001.CSI", name="主题样例", category="主题指数"),
        ])
        for code in ("000300.SH", "801010.SI", "931001.CSI"):
            session.add(IndexDaily(ts_code=code, trade_date="20260808", close=1))
            session.add(IndexWeekly(ts_code=code, trade_date="20260808", close=1))
            session.add(IndexConstituent(
                index_code=code, con_code="000001", trade_date="20260808",
            ))
        session.commit()

    service = object.__new__(AIndexNewHighService)
    service.db = _SqlDb()
    assert service.clear_non_allowed_data() == {
        "index_basic": 1,
        "index_daily": 1,
        "index_weekly": 1,
        "index_constituent": 1,
    }

    with service.db.get_session() as session:
        assert {row.ts_code for row in session.query(IndexBasic).all()} == {
            "000300.SH", "801010.SI",
        }


def test_scan_new_highs_includes_index_category(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from src.storage import Base, IndexBasic, IndexDaily

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[IndexBasic.__table__, IndexDaily.__table__],
    )
    session_factory = sessionmaker(bind=engine)

    class _SqlDb:
        def get_session(self):
            session = session_factory()

            class _Context:
                def __enter__(self):
                    return session

                def __exit__(self, exc_type, *_args):
                    if exc_type:
                        session.rollback()
                    session.close()

            return _Context()

    with _SqlDb().get_session() as session:
        session.add(IndexBasic(
            ts_code="801010.SI", name="申万农林牧渔", category="行业指数",
        ))
        session.add_all([
            IndexDaily(ts_code="801010.SI", trade_date="20251231", close=90),
            IndexDaily(ts_code="801010.SI", trade_date="20260102", close=100),
            IndexDaily(ts_code="801010.SI", trade_date="20260105", close=101),
        ])
        session.commit()

    service = object.__new__(AIndexNewHighService)
    service.db = _SqlDb()
    monkeypatch.setattr(service, "_maybe_persist_disk", lambda *_args: None)
    payload = service._scan_new_highs_uncached(
        "20260101", date(2026, 1, 5), "20260105", ("test",), "daily",
    )

    assert payload["items"][0]["category"] == "行业指数"
