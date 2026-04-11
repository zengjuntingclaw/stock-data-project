import baostock as bs
import sys
sys.stdout.reconfigure(encoding='utf-8')

lg = bs.login()
print("Login:", lg.error_code, lg.error_msg)

# Test query_stock_basic for delisted stock
rs = bs.query_stock_basic(code='sz.000005')
print("Fields:", rs.fields)
rows = []
while rs.next():
    rows.append(rs.get_row_data())
print("Rows:", rows)

rs2 = bs.query_stock_basic(code='sh.600001')
rows2 = []
while rs2.next():
    rows2.append(rs2.get_row_data())
print("sh.600001:", rows2)

bs.logout()
