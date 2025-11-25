import pandas as pd
import mysql.connector
from db import get_connection
# ==== 1. ĐỌC FILE CSV ====
csv_path = r"dc4.csv"
df = pd.read_csv(csv_path)

# Xoá khoảng trắng thừa ở tên cột nếu có
df.columns = df.columns.str.strip()

print("Đọc CSV thành công, số dòng:", len(df))
print("Cột CSV:", list(df.columns))

# ==== 1.1 CHUYỂN NaN -> None ====
df = df.astype(object).where(pd.notnull(df), None)

# ==== 1.2 CHUYỂN 0.0 -> "0" (trừ MachineID, Days) ====
def convert_number(x):
    if x is None:
        return None
    # giữ nguyên kiểu cho MachineID & Days (xử lý riêng khi insert)
    return x

df = df.map(convert_number)

# ==== 2. KẾT NỐI MYSQL ====
conn = get_connection()

cursor = conn.cursor()

# ==== 3. LẤY DANH SÁCH MACHINEID HỢP LỆ TỪ BẢNG machine ====
cursor.execute("SELECT MachineID FROM machine")
valid_machine_ids = {row[0] for row in cursor.fetchall()}
print("MachineID hợp lệ trong bảng machine:", valid_machine_ids)

csv_machine_ids = set(df["MachineID"])
print("MachineID có trong CSV:", csv_machine_ids)

missing_ids = csv_machine_ids - valid_machine_ids
if missing_ids:
    print("⚠ MachineID có trong CSV nhưng KHÔNG tồn tại trong bảng machine:", missing_ids)
else:
    print("✅ Tất cả MachineID trong CSV đều tồn tại trong bảng machine.")

# ==== 4. CÂU LỆNH INSERT (KHÔNG CÓ idDayValues, KHÔNG CÓ MachineryEdit) ====
sql = """
INSERT INTO dayvalues (
    MachineID,
    Days,
    PowerRun,
    Operation,
    SmallStop,
    Fault,
    Break,
    Maintenance,
    Eat,
    Waiting,
    CheckMachinery,
    ChangeProductCode,
    Glue_CleaningPaper,
    Others,
    TargetDayHours,
    OEERatio,
    OKProductRatio,
    OutputRatio,
    ActivityRatio,
    Note
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
"""

# ==== 5. INSERT CHỈ NHỮNG DÒNG CÓ MachineID HỢP LỆ ====
count_ok = 0
count_skip = 0

for _, row in df.iterrows():
    mid = row["MachineID"]
    # ép MachineID về int nếu có thể
    if mid is None or str(mid).strip() == "":
        count_skip += 1
        continue

    try:
        mid_int = int(float(mid))
    except ValueError:
        print("⚠ MachineID không convert được sang int, bỏ qua dòng:", mid)
        count_skip += 1
        continue

    if mid_int not in valid_machine_ids:
        # bỏ qua dòng vì MachineID không tồn tại trong bảng machine
        count_skip += 1
        continue

    # xử lý các giá trị số khác: 0.0 -> "0"
    def norm(v):
        if v is None:
            return None
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return v

    values = (
        mid_int,                 # MachineID là int
        row["Days"],             # format yyyy-mm-dd trong CSV là ok
        norm(row["PowerRun"]),
        norm(row["Operation"]),
        norm(row["SmallStop"]),
        norm(row["Fault"]),
        norm(row["Break"]),
        norm(row["Maintenance"]),
        norm(row["Eat"]),
        norm(row["Waiting"]),
        norm(row["CheckMachinery"]),
        norm(row["ChangeProductCode"]),
        norm(row["Glue_CleaningPaper"]),
        norm(row["Others"]),
        norm(row["TargetDayHours"]),
        norm(row["OEERatio"]),
        norm(row["OKProductRatio"]),
        norm(row["OutputRatio"]),
        norm(row["ActivityRatio"]),
        row["Note"],
    )

    cursor.execute(sql, values)
    count_ok += 1

conn.commit()
cursor.close()
conn.close()

print(f"✅ Insert thành công {count_ok} dòng vào dayvalues.")
print(f"⚠ Bỏ qua {count_skip} dòng do MachineID không hợp lệ hoặc thiếu.")