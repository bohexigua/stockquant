import os
import sys
import pymysql
import pytest

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
sys.path.append(project_root)

from config import config
from tradeDataClean.positions.buy_strategy import BuyStrategy
from tradeDataClean.positions.tests.test_utils import print_unbuffered
from tradeDataClean.positions.criteria.buy_conditions.criteria_has_position import check


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

def _pick_code_name(conn):
    with conn.cursor() as c:
        c.execute("SELECT stock_code, stock_name FROM ptm_user_watchlist WHERE is_active=1 LIMIT 1")
        r = c.fetchone()
        if r and r[0]:
            return r[0], r[1]
        c.execute("SELECT code, name FROM trade_market_stock_daily LIMIT 1")
        r = c.fetchone()
        return (r[0], r[1]) if r else (None, None)


def test_has_position_live(capsys):
    conn = _get_db()
    try:
        code, name = _pick_code_name(conn)
        assert code is not None
        strategy = BuyStrategy(conn)
        ok, reason, data = check(strategy, code, name or code)
        print_unbuffered(capsys, f"[has_position] code={code} name={name or code} ok={ok} reason={reason} position_qty_after={data.get('position_qty_after')}")
        assert isinstance(ok, bool)
    finally:
        conn.close()


if __name__ == '__main__':
    pytest.main([__file__])
