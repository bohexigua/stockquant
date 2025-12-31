import os
import sys
import pymysql
import pytest

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
sys.path.append(project_root)

from config import config
from tradeDataClean.positions.buy_strategy import BuyStrategy
from tradeDataClean.positions.criteria.sell_conditions.criteria_morning_drop_low_preopen import check
from tradeDataClean.positions.tests.test_utils import print_unbuffered


def _get_db():
    return pymysql.connect(
        host=config.database.host,
        port=config.database.port,
        user=config.database.user,
        password=config.database.password,
        database=config.database.database,
        charset=config.database.charset,
        autocommit=True,
    )

def _pick_codes(conn):
    with conn.cursor() as c:
        c.execute("SELECT stock_code, stock_name FROM ptm_user_watchlist WHERE is_active=1")
        return [(r[0], r[1]) for r in c.fetchall() if r and r[0]]


def test_sell_morning_drop_low_preopen_live(capsys):
    conn = _get_db()
    try:
        pairs = _pick_codes(conn)
        assert pairs
        strategy = BuyStrategy(conn)
        for code, name in pairs:
            ok, reason, data = check(strategy, code, name or code)
            print_unbuffered(capsys, f"[sell_morning_drop] code={code} ok={ok} reason={reason}")
            assert isinstance(ok, bool)
    finally:
        conn.close()


if __name__ == '__main__':
    pytest.main([__file__])

