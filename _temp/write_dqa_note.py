import sys
sys.path.insert(0, r'C:\Users\zengj\.qclaw\workspace\stock_data_project')
import duckdb
DB = r'C:\Users\zengj\.qclaw\workspace\stock_data_project\data\stock_data.duckdb'
conn = duckdb.connect(DB)
# Mark 2 remaining sentinel delisted stocks
conn.execute("""
    INSERT INTO data_quality_alert (alert_type, ts_code, detail, created_at)
    VALUES ('DELIST_DATE_SENTINEL', '000999.SH',
            'Baostock outDate empty, likely pre-1999 delist, sentinel kept', CURRENT_TIMESTAMP)
""")
conn.execute("""
    INSERT INTO data_quality_alert (alert_type, ts_code, detail, created_at)
    VALUES ('DELIST_DATE_SENTINEL', '399923.SZ',
            'Baostock outDate empty, likely pre-1999 delist, sentinel kept', CURRENT_TIMESTAMP)
""")
conn.commit()
conn.close()
print("data_quality_alert updated with 2 remaining sentinel notes")
