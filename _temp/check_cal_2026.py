import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
c = duckdb.connect(DB, read_only=True)

# Trade calendar around 2026-04
cal = c.execute("""
    SELECT cal_date, pretrade_date, is_open
    FROM trade_calendar
    WHERE cal_date >= '2026-03-25' AND cal_date <= '2026-04-15'
    ORDER BY cal_date
""").fetchdf()
print("Trade calendar 2026-03 to 2026-04:")
print(cal.to_string(index=False))

# Latest daily_bar date
for tbl in ['daily_bar_raw', 'daily_bar_adjusted']:
    mx = c.execute(f"SELECT MAX(trade_date) FROM {tbl}").fetchone()[0]
    mn = c.execute(f"SELECT MIN(trade_date) FROM {tbl}").fetchone()[0]
    print(f"\n{tbl}: {mn} to {mx}")

c.close()
