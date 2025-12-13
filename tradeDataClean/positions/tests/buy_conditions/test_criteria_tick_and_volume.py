import os
import sys
import pymysql
import pytest

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
sys.path.append(project_root)

from config import config
from tradeDataClean.positions.buy_strategy import BuyStrategy
from tradeDataClean.positions.criteria.buy_conditions.criteria_tick_available import check as c_tick
from tradeDataClean.positions.criteria.buy_conditions.criteria_preopen_volume import check as c_prevol
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

def _pick_code_name(conn):
    with conn.cursor() as c:
        c.execute("SELECT stock_code, stock_name FROM ptm_user_watchlist WHERE is_active=1 LIMIT 1")
        r = c.fetchone()
        if r and r[0]:
            return r[0], r[1]
        c.execute("SELECT code, name FROM trade_market_stock_daily LIMIT 1")
        r = c.fetchone()
        return (r[0], r[1]) if r else (None, None)


def test_tick_and_preopen_volume_live(capsys):
    conn = _get_db()
    try:
        code, name = _pick_code_name(conn)
        assert code is not None
        strategy = BuyStrategy(conn)
        ok, reason, data = c_tick(strategy, code, name or code)
        print_unbuffered(capsys, f"[tick_available] code={code} ok={ok} reason={reason}")
        if not ok:
            pytest.skip('竞价无数据')
        ok2, reason2, data2 = c_prevol(strategy, code, name or code)
        print_unbuffered(capsys, f"[preopen_volume] code={code} ok={ok2} pre_ratio={data2.get('pre_ratio')} reason={reason2}")
        assert isinstance(ok2, bool)
    finally:
        conn.close()


if __name__ == '__main__':
    pytest.main([__file__])
