import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
c = duckdb.connect(DB, read_only=True)

# Check constraints / primary key
pk = c.execute("""
    SELECT constraint_name, column_names
    FROM information_schema.table_constraints
    WHERE table_name = 'daily_bar_raw' AND constraint_type = 'PRIMARY KEY'
""").fetchdf()
print("PRIMARY KEY on daily_bar_raw:", pk.to_string(index=False))

# Check if there's a unique constraint
uniq = c.execute("""
    SELECT constraint_name, column_names
    FROM information_schema.table_constraints
    WHERE table_name = 'daily_bar_raw' AND constraint_type = 'UNIQUE'
""").fetchdf()
print("UNIQUE constraints:", uniq.to_string(index=False))

# Try to insert a duplicate - see what error we get
try:
    c.execute("""
        INSERT INTO daily_bar_raw
        (ts_code,trade_date,symbol,open,high,low,close,volume,amount,
         pct_chg,turnover,data_source,created_at,adj_factor,pre_close,
         is_suspend,limit_up,limit_down)
        VALUES ('600000.SH','2026-04-03','600000',10.0,10.1,9.9,10.0,1000,10000,
                0.0,0.0,'test','2026-04-03',1.0,NULL,FALSE,FALSE,FALSE)
    """)
    print("INSERT succeeded (no unique constraint)")
except Exception as e:
    print(f"INSERT failed (has constraint): {e}")

c.close()
