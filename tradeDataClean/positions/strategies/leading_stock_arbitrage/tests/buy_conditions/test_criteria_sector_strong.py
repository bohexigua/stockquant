import os
import sys
import pymysql
import pytest

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
sys.path.append(project_root)

from config import config
from tradeDataClean.positions.buy_strategy import BuyStrategy
from tradeDataClean.positions.criteria.buy_conditions.criteria_sector_strong import check
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

def _pick_codes_names_and_dates(conn):
    with conn.cursor() as c:
        c.execute("SELECT stock_code, stock_name FROM ptm_user_watchlist WHERE is_active=1")
        rows = c.fetchall()
        seen = set()
        pairs = []
        for r in rows:
            code, name = r[0], r[1]
            if not code or code in seen:
                continue
            seen.add(code)
            c.execute("SELECT MAX(trade_date) FROM trade_market_stock_daily WHERE code=%s AND trade_date<=CURDATE()", (code,))
            drow = c.fetchone()
            prev_date = drow[0] if drow and drow[0] else None
            pairs.append((code, name, prev_date))
        return pairs


def test_sector_strong_live(capsys):
    conn = _get_db()
    try:
        pairs = _pick_codes_names_and_dates(conn)
        assert pairs
        for code, name, prev_date in pairs:
            if not prev_date:
                continue
            prev = prev_date.strftime('%Y-%m-%d')
            strategy = BuyStrategy(conn)
            ok, reason, data = check(strategy, code, name or code)
            peers1 = data.get('peers1') or []
            peers2 = data.get('peers2') or []
            def _fmt(items):
                try:
                    return ','.join([f"{it['name']}:{it['rise']:.2%}" for it in items[:5]]) if items else '无'
                except Exception:
                    return '无'
            print_unbuffered(capsys, f"[sector_strong] code={code} date={prev} ok={ok} strong_count={data.get('strong_count')} theme1={data.get('theme1')} theme2={data.get('theme2')} peers1={_fmt(peers1)} peers2={_fmt(peers2)} reason={reason}")
            assert isinstance(ok, bool)
            if ok:
                assert isinstance(peers1, list)
                assert isinstance(peers2, list)
    finally:
        conn.close()


if __name__ == '__main__':
    pytest.main([__file__])
