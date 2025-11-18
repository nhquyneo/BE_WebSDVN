import mysql.connector

def get_connection():
    return mysql.connector.connect(
        host="127.0.0.1",      # hoặc localhost
        port=3306,             # 👉 cổng mặc định MySQL, sửa nếu khác
        user="root",           # tài khoản MySQL của bạn
        password="2804",           # mật khẩu MySQL của bạn
        database="sdvn"   # tên database
    )