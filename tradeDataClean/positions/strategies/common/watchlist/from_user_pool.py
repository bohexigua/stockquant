from typing import Dict, Any, Optional
from datetime import datetime

def get_watchlist_from_user_pool(cursor: Any, now_dt: Optional[datetime] = None) -> Dict[str, str]:
    """
    从 ptm_user_watchlist 获取自选股
    简单的查询，不依赖 now_dt (或者未来可以依赖)
    """
    try:
        cursor.execute("SELECT stock_code, stock_name FROM ptm_user_watchlist WHERE is_active=1")
        rows = cursor.fetchall()
        return {r[0]: r[1] for r in rows if r and r[0]}
    except Exception as e:
        print(f"Error in get_watchlist_from_user_pool: {e}")
        return {}
