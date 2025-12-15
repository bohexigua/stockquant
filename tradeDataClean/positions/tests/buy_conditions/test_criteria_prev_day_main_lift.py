import os
import sys
import pymysql
import pytest

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
sys.path.append(project_root)

from config import config
from tradeDataClean.positions.buy_strategy import BuyStrategy
from tradeDataClean.positions.criteria.buy_conditions.criteria_prev_day_main_lift import check
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

def _pick_codes_names(conn):
    with conn.cursor() as c:
        c.execute("SELECT stock_code, stock_name FROM ptm_user_watchlist WHERE is_active=1")
        rows = c.fetchall()
        pairs = []
        for r in rows:
            if not r or not r[0]:
                continue
            code = r[0]
            name = r[1]
            c.execute(
                "SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<=CURDATE()",
                (code,),
            )
            drow = c.fetchone()
            prev_date = drow[0] if drow and drow[0] else None
            pairs.append((code, name, prev_date))
        return pairs


def test_prev_day_main_lift_live(capsys):
    conn = _get_db()
    try:
        pairs = _pick_codes_names(conn)
        assert pairs, 'no codes found'
        strategy = BuyStrategy(conn)
        for code, name, prev_date in pairs:
            ok, reason, data = check(strategy, code, name or code, prev_date)
            print_unbuffered(capsys, f"[prev_day_main_lift] code={code} date={prev_date} ok={ok} lift_count={data.get('lift_count')} dump_count={data.get('dump_count')} reason={reason}")
            assert isinstance(ok, bool)
    finally:
        conn.close()


if __name__ == '__main__':
    pytest.main([__file__])
