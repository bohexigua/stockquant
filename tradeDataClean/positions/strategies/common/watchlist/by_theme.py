from typing import Dict, Any
from datetime import datetime
import json

def get_watchlist_by_theme(cursor: Any, now_dt: datetime) -> Dict[str, str]:
    """
    根据复杂逻辑获取每日自选股列表
    返回: {stock_code: stock_name}
    """
    if now_dt is None:
        now_dt = datetime.now()

    d = now_dt.strftime('%Y-%m-%d')
    
    # 替换用户SQL中的HTML实体 &quot; 为 '
    # theme_pattern = "['人工智能', 'AI智能体', '端侧AI', '文化传媒', '游戏']  ['商业航天', '大飞机', '通信', '卫星导航'] ['阿里巴巴概念', 'AI智能体', '数字经济'] AI医疗 ['电力', '变压器', '电源', '特高压', '智能电网']"
    
    # 动态获取 theme_pattern
    try:
        cursor.execute(f"SELECT analysis_json FROM trade_market_ai_theme_analysis WHERE trade_date = '{d}' ORDER BY trade_date DESC LIMIT 1")
        row = cursor.fetchone()
        if row and row[0]:
            analysis_data = json.loads(row[0])
            related_list = []
            for item in analysis_data:
                try:
                    strength = float(item.get('strength', 0))
                    if strength >= 0.8:
                        if 'related' in item and isinstance(item['related'], list):
                            related_list.extend(item['related'])
                        if 'name' in item:
                            related_list.append(item['name'])
                except (ValueError, TypeError):
                    continue
            
            # 去重并构建 theme_pattern
            if related_list:
                unique_related = list(set(related_list))
                # 构建类似于 SQL LOCATE 能够匹配的格式，虽然原始逻辑是字符串包含，这里我们构建一个包含所有关键词的字符串
                # 注意：原始 SQL 使用 LOCATE(tft.most_related_theme_name, theme_pattern) > 0
                # 这意味着只要 most_related_theme_name 出现在 theme_pattern 中即可
                # 所以我们将所有关键词拼接成一个长字符串
                theme_pattern = str(unique_related)
            else:
                 # Fallback if no strong themes found, keep empty or default? 
                 # If empty, LOCATE might fail or return 0 always. 
                 # Let's keep a minimal fallback or log warning.
                 # For now, fallback to empty list string which will likely match nothing if theme names are not empty brackets.
                 theme_pattern = "[]"
        else:
             # No analysis data found
             theme_pattern = "[]"
    except Exception as e:
        print(f"Error fetching theme analysis: {e}")
        theme_pattern = "[]"

    # print(f"theme_pattern: {theme_pattern}")

    sql = f"""
    SELECT DISTINCT 
      tft.stock_code 
    FROM 
      trade_factor_most_related_theme tft 
    WHERE 
      tft.trade_date < '{d}'
      AND (
        (
          -- 条件1：板块匹配 
          LOCATE ( 
            tft.most_related_theme_name, 
            "{theme_pattern}" 
          ) > 0 
          -- 条件2：满足 (涨停 OR 资金流入) 
          AND ( 
            -- 近 20 个交易日有涨停 
            EXISTS ( 
              SELECT 
                1 
              FROM 
                trade_factor_most_related_theme t_lu 
              WHERE 
                t_lu.stock_code = tft.stock_code 
                AND t_lu.trade_date < '{d}'
                AND t_lu.trade_date >= ( 
                  SELECT 
                    MIN(trade_date) 
                  FROM 
                    ( 
                      SELECT DISTINCT 
                        trade_date 
                      FROM 
                        trade_factor_most_related_theme 
                      WHERE trade_date < '{d}'
                      ORDER BY 
                        trade_date DESC 
                      LIMIT 
                        20 
                    ) r_x 
                ) 
            ) 
            OR 
            -- 近 7 个交易日有 2 次 TOP-500 资金流入 
            tft.stock_code IN ( 
              SELECT 
                code 
              FROM 
                ( 
                  SELECT 
                    code, 
                    COUNT(*) AS cnt 
                  FROM 
                    ( 
                      SELECT 
                        tmf.code, 
                        ROW_NUMBER() OVER ( 
                          PARTITION BY 
                            tmf.trade_date 
                          ORDER BY 
                            tmf.net_amount DESC 
                        ) AS rn 
                      FROM 
                        trade_market_stock_fund_flow tmf 
                      WHERE 
                        tmf.trade_date < '{d}'
                        AND tmf.trade_date >= ( 
                          SELECT 
                            MIN(trade_date) 
                          FROM 
                            ( 
                              SELECT DISTINCT 
                                trade_date 
                              FROM 
                                trade_market_stock_fund_flow 
                              WHERE trade_date < '{d}'
                              ORDER BY 
                                trade_date DESC 
                              LIMIT 
                                7 
                            ) r_y 
                        ) 
                    ) ranked 
                  WHERE 
                    rn <= 500 
                  GROUP BY 
                    code 
                  HAVING 
                    COUNT(*) >= 2 
                ) fund_top 
            ) 
          ) 
          -- 条件3：近 120 个交易日东财 TOP-100 出现 >= 4 次 
          AND tft.stock_code IN ( 
            SELECT 
              code 
            FROM 
              ( 
                SELECT 
                  h.code, 
                  SUM( 
                    CASE 
                      WHEN h.hot_rank <= 20 THEN 1 
                      ELSE 0 
                    END 
                  ) AS top100_cnt 
                FROM 
                  trade_market_dc_stock_hot h 
                WHERE 
                  h.trade_date < '{d}'
                  AND h.trade_date >= ( 
                    SELECT 
                      MIN(trade_date) 
                    FROM 
                      ( 
                        SELECT DISTINCT 
                          trade_date 
                        FROM 
                          trade_market_dc_stock_hot 
                        WHERE trade_date < '{d}'
                        ORDER BY 
                          trade_date DESC 
                        LIMIT 
                          120 
                      ) r_z 
                  ) 
                GROUP BY 
                  h.code 
                HAVING 
                  top100_cnt >= 4 
              ) dc_top 
          )
        )
        -- 条件4：取当日东财 TOP20 出来 
        OR tft.stock_code IN ( 
          SELECT 
            code 
          FROM 
            trade_market_dc_stock_hot h 
          WHERE 
            h.trade_date >= ( 
              SELECT 
                MAX(trade_date) 
              FROM 
                ( 
                  SELECT DISTINCT 
                    trade_date 
                  FROM 
                    trade_market_dc_stock_hot 
                  WHERE trade_date < '{d}'
                  ORDER BY 
                    trade_date DESC 
                  LIMIT 
                    1 
                ) r_z 
            ) 
            AND h.hot_rank <= 10
        ) 
      )
    """
    
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        codes = [r[0] for r in rows if r and r[0]]
        
        if not codes:
            return {}
            
        # 获取股票名称
        # 优先从 trade_market_stock_basic 获取，如果没有则尝试从 trade_factor_most_related_theme 获取
        watchlist = {}
        
        # 尝试 1: trade_market_stock_basic (最标准)
        try:
            format_strings = ','.join(['%s'] * len(codes))
            cursor.execute(f"""
                SELECT code, name 
                FROM (
                    SELECT code, name, ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) as rn
                    FROM trade_market_stock_basic_daily 
                    WHERE code IN ({format_strings}) AND trade_date < '{d}'
                ) t
                WHERE t.rn = 1
            """, tuple(codes))
            name_rows = cursor.fetchall()
            for r in name_rows:
                if r and r[0]:
                    watchlist[r[0]] = r[1]
        except Exception as e:
            print(f"Error in trade_market_stock_basic_daily: {e}")
            pass
        
        # 如果还是没有名字，就用 code 作为名字
        for code in codes:
            if code not in watchlist:
                watchlist[code] = code
                
        return watchlist
    except Exception as e:
        print(f"Error in get_watchlist_by_theme: {e}")
        return {}
