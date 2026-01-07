def check(
    strategy,
    code: str,
    stock_name: str,
    theme1: str = None,
    theme2: str = None,
    now_dt=None,
):
    """
    检查同板块持仓限制
    :param strategy: 策略实例，包含db连接
    :param code: 股票代码
    :param stock_name: 股票名称
    :param theme1: 题材1
    :param theme2: 题材2
    :param now_dt: 当前时间
    :return: (ok, reason, data)
    """
    if not theme1 and not theme2:
        return True, "", {}

    from datetime import datetime

    if now_dt is None:
        now_dt = datetime.now()

    from tradeDataClean.positions.strategies.leading_stock_arbitrage import sql_utils
    subquery_positions = sql_utils.get_subquery_positions()

    view_theme = sql_utils.get_subquery_related_theme(now_dt)

    try:
        with strategy.db.cursor() as c:
            c.execute(
                f"""
                SELECT t1.stock_code
                FROM {subquery_positions} as t1
                WHERE t1.qty > 0
            """
            )

            rows = c.fetchall()
            held_codes = [r[0] for r in rows if r and r[0]]

            if not held_codes:
                return True, "", {}

            count = 0
            for h_code in held_codes:
                # 获取题材时，限制 trade_date < now_dt.date()
                c.execute(
                    f"SELECT all_themes_name FROM {view_theme} as t WHERE stock_code=%s ORDER BY trade_date DESC LIMIT 1",
                    (h_code,),
                )
                tr = c.fetchone()
                if not tr or not tr[0]:
                    continue

                raw = [x.strip() for x in str(tr[0]).split(",") if x and x.strip()]
                h_theme1 = raw[0] if len(raw) > 0 else ""
                h_theme2 = raw[1] if len(raw) > 1 else ""

                match = False
                if theme1 and (theme1 == h_theme1 or theme1 == h_theme2):
                    match = True
                elif theme2 and (theme2 == h_theme1 or theme2 == h_theme2):
                    match = True

                if match:
                    count += 1

            if count >= 2:
                return (
                    False,
                    f"同板块({theme1}/{theme2})持仓已达上限(2只)",
                    {"count": count},
                )
            return True, "", {"count": count}
    except Exception as e:
        # 出错时默认通过，避免阻断交易
        return True, "", {}
