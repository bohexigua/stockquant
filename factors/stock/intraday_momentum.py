#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import pandas as pd
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config import config


class IntradayMomentumCalculator:
    def __init__(self):
        self.db_config = config.database
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def _get_db_connection(self) -> pymysql.Connection:
        try:
            conn = pymysql.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                charset=self.db_config.charset,
                autocommit=False,
            )
            return conn
        except Exception as e:
            self.logger.error(f"数据库连接失败: {e}")
            raise

    def get_trade_dates_in_range(self, start_date: str, end_date: str) -> List[str]:
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = (
                    "SELECT DISTINCT trade_date FROM trade_market_stock_5min "
                    "WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date"
                )
                cursor.execute(sql, (start_date, end_date))
                rows = cursor.fetchall()
                dates = [row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0]) for row in rows]
                return dates
        except Exception as e:
            self.logger.error(f"获取分时交易日期失败: {e}")
            return []
        finally:
            if connection:
                connection.close()

    def fetch_5min_data_for_date(self, trade_date: str) -> pd.DataFrame:
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = (
                    "SELECT trade_date, code, name, trade_time, open, close, high, low, vol, amount "
                    "FROM trade_market_stock_5min WHERE trade_date=%s"
                )
                cursor.execute(sql, (trade_date,))
                rows = cursor.fetchall()
                if not rows:
                    return pd.DataFrame()
                df = pd.DataFrame(rows, columns=['trade_date','code','name','trade_time','open','close','high','low','vol','amount'])
                return df
        except Exception as e:
            self.logger.error(f"获取{trade_date}分时数据失败: {e}")
            return pd.DataFrame()
        finally:
            if connection:
                connection.close()

    @staticmethod
    def _classify_main_action(row: pd.Series, prev_close: float, vol_avg: float) -> (str, str):
        try:
            close = float(row['close']) if row['close'] is not None else None
            vol = float(row['vol']) if row['vol'] is not None else 0.0
            if close is None or prev_close is None or prev_close <= 0:
                return '', ''
            ret = (close - prev_close) / prev_close
            vol_ratio = (vol / vol_avg) if vol_avg > 0 else 0.0

            if ret >= 0.01 and vol_ratio >= 1.5:
                return '主力拉升', f"5min涨幅:{ret:.2%}, 量能倍数:{vol_ratio:.2f}"
            if ret <= -0.007 and vol_ratio >= 1:
                return '主力出货', f"5min跌幅:{ret:.2%}, 量能倍数:{vol_ratio:.2f}"
            return '无明显动作', f"涨幅:{ret:.2%}, 量能倍数:{vol_ratio:.2f}"
        except Exception:
            return '', ''

    def build_intraday_actions(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        try:
            # 类型转换
            df['open'] = pd.to_numeric(df['open'], errors='coerce')
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            df['vol'] = pd.to_numeric(df['vol'], errors='coerce').fillna(0)
            # 分组
            out_rows = []
            for code, g in df.groupby('code'):
                g2 = g.sort_values('trade_time')
                vol_avg = max(g2['vol'].mean(), 1.0)
                prev_close = None
                for _, r in g2.iterrows():
                    action, semantic = self._classify_main_action(r, prev_close if prev_close is not None else r['open'], vol_avg)
                    out_rows.append({
                        'trade_date': r['trade_date'],
                        'trade_time': r['trade_time'],
                        'code': r['code'],
                        'name': r['name'],
                        'main_action': action,
                        'main_action_semantic': semantic,
                    })
                    prev_close = r['close'] if not pd.isna(r['close']) else prev_close
            return pd.DataFrame(out_rows)
        except Exception as e:
            self.logger.error(f"构建分时主力动作失败: {e}")
            return pd.DataFrame()

    def clear_existing_date(self, trade_date: str) -> bool:
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                # 清理当前日期的数据
                cursor.execute("DELETE FROM trade_factor_stock_intraday_momentum WHERE trade_date=%s", (trade_date,))
                # 清理30天前的数据
                try:
                    target_dt = datetime.strptime(str(trade_date), '%Y-%m-%d')
                except ValueError:
                    target_dt = datetime.strptime(str(trade_date), '%Y%m%d')
                
                expire_date = (target_dt - timedelta(days=30)).strftime('%Y-%m-%d')
                cursor.execute("DELETE FROM trade_factor_stock_intraday_momentum WHERE trade_date < %s", (expire_date,))
                
                connection.commit()
                return True
        except Exception as e:
            self.logger.error(f"清理{trade_date}已有分时因子失败: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                connection.close()

    def insert_intraday_actions(self, df: pd.DataFrame) -> bool:
        if df.empty:
            self.logger.info("无分时因子可写入")
            return True
        connection = None
        try:
            connection = self._get_db_connection()
            with connection.cursor() as cursor:
                sql = (
                    "INSERT INTO trade_factor_stock_intraday_momentum "
                    "(trade_date, trade_time, code, name, main_action, main_action_semantic, created_time, updated_time) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE main_action=VALUES(main_action), main_action_semantic=VALUES(main_action_semantic), updated_time=CURRENT_TIMESTAMP"
                )
                now = datetime.now()
                def _norm_time(t):
                    try:
                        from datetime import datetime as _dt, timedelta as _td
                        import pandas as _pd
                        if hasattr(t, 'time'):
                            return t.time()
                        if isinstance(t, _td):
                            return (_dt.min + t).time()
                        if isinstance(t, _pd.Timedelta):
                            return (_dt.min + t.to_pytimedelta()).time()
                        if isinstance(t, str):
                            try:
                                return _dt.strptime(t, '%H:%M:%S').time()
                            except Exception:
                                return _dt.strptime(t, '%H:%M').time()
                        return t
                    except Exception:
                        return t
                data_list = [(
                    r['trade_date'], _norm_time(r['trade_time']), r['code'], r['name'], r['main_action'], r['main_action_semantic'], now, now
                ) for _, r in df.iterrows()]
                cursor.executemany(sql, data_list)
                connection.commit()
                self.logger.info(f"写入分时因子 {len(data_list)} 条")
                return True
        except Exception as e:
            self.logger.error(f"写入分时因子失败: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                connection.close()

    def run_for_date(self, trade_date: str) -> bool:
        try:
            self.logger.info(f"开始计算分时因子: {trade_date}")
            df = self.fetch_5min_data_for_date(trade_date)
            if df.empty:
                self.logger.warning(f"{trade_date} 无5分钟数据")
                return False
            actions_df = self.build_intraday_actions(df)
            if actions_df.empty:
                self.logger.warning(f"{trade_date} 无分时因子")
                return False
            if not self.clear_existing_date(trade_date):
                return False
            return self.insert_intraday_actions(actions_df)
        except Exception as e:
            self.logger.error(f"运行分时因子失败: {e}")
            return False

    def run_range(self, start_date: str, end_date: str) -> bool:
        dates = self.get_trade_dates_in_range(start_date, end_date)
        if not dates:
            self.logger.warning("指定区间无交易日")
            return False
        ok_count = 0
        for d in dates:
            if self.run_for_date(d):
                ok_count += 1
        self.logger.info(f"区间处理完成: 成功 {ok_count}/{len(dates)} 天")
        return ok_count > 0


def main():
    import argparse
    parser = argparse.ArgumentParser(description='分时动量因子计算')
    parser.add_argument('--date', help='交易日期 YYYY-MM-DD')
    parser.add_argument('--start-date', help='开始日期 YYYY-MM-DD')
    parser.add_argument('--end-date', help='结束日期 YYYY-MM-DD')
    args = parser.parse_args()

    calc = IntradayMomentumCalculator()
    if args.date:
        ok = calc.run_for_date(args.date)
        sys.exit(0 if ok else 1)
    if args.start_date and args.end_date:
        ok = calc.run_range(args.start_date, args.end_date)
        sys.exit(0 if ok else 1)
    # 默认跑最近交易日
    today = datetime.now().strftime('%Y-%m-%d')
    ok = calc.run_for_date(today)
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
