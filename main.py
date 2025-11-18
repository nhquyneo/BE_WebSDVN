from flask import Flask, request, jsonify,send_file
from flask_cors import CORS
from db import get_connection
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Border, Side
app = Flask(__name__)
CORS(app)

@app.route("/api/login", methods=["POST"])
def login():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return jsonify({"ok": False, "message": "Thiếu tài khoản hoặc mật khẩu"}), 400

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE username=%s AND password=%s", (username, password))
    user = cur.fetchone()
    cur.close()
    conn.close()

    if user:
        return jsonify({"ok": True, "user": {"username": user["username"], "full_name": user["full_name"]}})
    else:
        return jsonify({"ok": False, "message": "Sai tài khoản hoặc mật khẩu"}), 401
@app.route("/api/register", methods=["POST"])
def register():
    data = request.get_json(force=True) or {}
    print("DEBUG /api/register payload:", data)  # xem payload thực tế

    def as_text(v):
        if isinstance(v, str):
            return v
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, dict):
            # cố gắng lấy các key hay gặp khi gửi nhầm object
            for k in ("value", "username", "name"):
                if isinstance(v.get(k), str):
                    return v[k]
        return ""

    username = as_text(data.get("username")).strip()
    password = as_text(data.get("password")).strip()
    full_name = as_text(data.get("full_name")).strip()

    if not username or not password or not full_name:
        return jsonify({"ok": False, "message": "Thiếu username/password/full_name"}), 400

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT id FROM users WHERE username=%s LIMIT 1", (username,))
    if cur.fetchone():
        cur.close();
        conn.close()
        return jsonify({"ok": False, "message": "Tài khoản đã tồn tại"}), 409

    cur.execute(
        "INSERT INTO users (username, password, full_name) VALUES (%s, %s, %s)",
        (username, password, full_name)
    )
    conn.commit()
    cur.close();
    conn.close()
    return jsonify({"ok": True, "message": "Đăng ký thành công"})
@app.route("/api/lines")
def get_lines():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT 
            LineID   AS idline,
            LineName AS ten_line
        FROM productionline
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return jsonify(rows)
@app.route("/api/lines/<int:idline>/machines")
def get_machines_by_line(idline):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT 
            MachineID   AS id,
            MachineName AS name
        FROM machine
        WHERE LineID = %s
    """, (idline,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(rows)
@app.route("/api/machines/<int:machine_id>/day")
def get_machine_day(machine_id):
    day = request.args.get("day")
    if not day:
        return jsonify({"error": "Missing day param"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # --------- LẤY DỮ LIỆU THỜI GIAN (dayvalues) ----------
    cursor.execute("""
        SELECT 
            Days,
            PowerRun,
            Operation,
            SmallStop,
            Fault,
            Break,
            Maintenance,
            Eat,
            Waiting,
            MachineryEdit,
            ChangeProductCode,
            Glue_CleaningPaper,
            Others
        FROM dayvalues
        WHERE MachineID = %s AND Days = %s
        LIMIT 1
    """, (machine_id, day))

    row = cursor.fetchone()

    # (tí nữa còn dùng connection, đừng đóng vội)
    if not row:
        cursor.close()
        conn.close()
        return jsonify({
            "machine_id": machine_id,
            "day": day,
            "data": None
        })

    # ---- POWER RUN: 2 chữ sau dấu chấm ----
    raw_power = row.get("PowerRun")
    try:
        power_val = float(raw_power) if raw_power else 0.0
    except:
        power_val = 0.0
    power_run_str = f"{power_val:.2f}"

    # ---- CÁC CATEGORY (cho pie + bảng) ----
    categories_raw = {
        "Operation":          float(row["Operation"]          or 0.0),
        "SmallStop":          float(row["SmallStop"]          or 0.0),
        "Fault":              float(row["Fault"]              or 0.0),
        "Break":              float(row["Break"]              or 0.0),
        "Maintenance":        float(row["Maintenance"]        or 0.0),
        "Eat":                float(row["Eat"]                or 0.0),
        "Waiting":            float(row["Waiting"]            or 0.0),
        "MachineryEdit":      float(row["MachineryEdit"]      or 0.0),
        "ChangeProductCode":  float(row["ChangeProductCode"]  or 0.0),
        "Glue_CleaningPaper": float(row["Glue_CleaningPaper"] or 0.0),
        "Others":             float(row["Others"]             or 0.0),
    }

    total_hours = sum(categories_raw.values())
    if total_hours <= 0:
        total_hours = 1.0

    color_map = {
        "Operation":          "#00a03e",
        "SmallStop":          "#f97316",
        "Fault":              "#ef4444",
        "Break":              "#eab308",
        "Maintenance":        "#6b21a8",
        "Eat":                "#22c55e",
        "Waiting":            "#0ea5e9",
        "MachineryEdit":      "#1d4ed8",
        "ChangeProductCode":  "#a855f7",
        "Glue_CleaningPaper": "#fb7185",
        "Others":             "#6b7280",
    }

    detail_rows = []
    pie_data = []

    for label, value in categories_raw.items():
        hours = float(value)
        h = int(hours)
        m = int(round((hours - h) * 60))
        time_str = f"{h}h {m}m"

        ratio = round((hours / total_hours) * 100.0, 2)
        ratio_text = f"{ratio:.2f}%"

        detail_rows.append({
            "label": label,
            "value": hours,
            "time": time_str,
            "ratio": ratio,
            "ratio_text": ratio_text,
            "color": color_map[label],
        })

        pie_data.append({
            "name": label,
            "value": ratio,
            "color": color_map[label],
        })

    # --------- THÊM PHẦN PRODUCT: TOTAL / OK / NG / RATIO ----------
    # TODO: sửa lại tên bảng + cột cho đúng DB thực tế của bạn
    #
    # Ví dụ: bảng dayproduct có cột:
    #   MachineID, Days, Total, OK, NG
    #
    cursor.execute("""
        SELECT 
            totalproduct_actual AS Total,
            totalproduct_ok as OK,
            totalproduct_ng as NG
        FROM production_output
        WHERE machineid = %s AND days = %s
        LIMIT 1
    """, (machine_id, day))

    prod = cursor.fetchone()
    cursor.close()
    conn.close()

    if prod:
        total = float(prod["Total"] or 0)
        ok = float(prod["OK"] or 0)
        ng = float(prod["NG"] or 0)
    else:
        total, ok, ng = 0.0, 0.0, 0.0

    ratio = (ok * 100.0 / total) if total > 0 else 0.0

    product = {
        "total": int(total),
        "ok": int(ok),
        "ng": int(ng),
        "ratio": round(ratio, 2),
        "ratio_text": f"{ratio:.2f}%"
    }

    # --------- KẾT QUẢ TRẢ VỀ ----------
    result = {
        "machine_id": machine_id,
        "day": row["Days"],
        "power_run": power_run_str,
        "total_hours": round(total_hours, 2),
        "pie": pie_data,
        "details": detail_rows,
        "product": product,          # 👈 FE dùng cho bảng PRODUCT
    }

    return jsonify(result)
from flask import request, jsonify

@app.route("/api/machines/<int:machine_id>/month-ratio")
def get_machine_month_ratio(machine_id):
    try:
        month = int(request.args.get("month"))
    except (TypeError, ValueError):
        return jsonify({"error": "Missing or invalid month param"}), 400

    data_type = request.args.get("data", "")  # VD "OEE RATIO" (để echo lại cho FE)

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # ⚠️ Nếu schema thực tế khác, sửa tên schema/bảng/cột cho đúng
    cursor.execute(
        """
        SELECT
            Days,
            OEERatio,
            OKProductRatio,
            OutputRatio,
            ActivityRatio
        FROM sdvn.dayvalues
        WHERE MachineID = %s
          AND MONTH(Days) = %s
        ORDER BY Days
        """,
        (machine_id, month),
    )

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    days = []
    for row in rows:
        day_raw = row["Days"]
        day_str = (
            day_raw.strftime("%Y-%m-%d") if hasattr(day_raw, "strftime") else str(day_raw)
        )

        days.append(
            {
                "day": day_str,
                "oee": float(row["OEERatio"] or 0.0),
                "ok_ratio": float(row["OKProductRatio"] or 0.0),
                "output_ratio": float(row["OutputRatio"] or 0.0),
                "activity_ratio": float(row["ActivityRatio"] or 0.0),
            }
        )

    return jsonify(
        {
            "machine_id": machine_id,
            "month": month,
            "data_type": data_type or None,
            "days": days,  # dùng cho biểu đồ/bảng đầu
        }
    )
@app.route("/api/machines/<int:machine_id>/month")
def get_machine_month_time(machine_id):
    try:
        month = int(request.args.get("month"))
    except (TypeError, ValueError):
        return jsonify({"error": "Missing or invalid month param"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # Nếu DB mặc định đã là sdvn thì có thể bỏ "sdvn." ở trước dayvalues
    cursor.execute(
        """
        SELECT
            Days,
            Operation,
            SmallStop,
            Fault,
            Break,
            Maintenance,
            Eat,
            Waiting,
            MachineryEdit,
            ChangeProductCode,
            Glue_CleaningPaper,
            Others
        FROM sdvn.dayvalues
        WHERE MachineID = %s
          AND MONTH(Days) = %s
        ORDER BY Days
        """,
        (machine_id, month),
    )

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    days = []
    monthly_totals = {
        "Operation": 0.0,
        "SmallStop": 0.0,
        "Fault": 0.0,
        "Break": 0.0,
        "Maintenance": 0.0,
        "Eat": 0.0,
        "Waiting": 0.0,
        "MachineryEdit": 0.0,
        "ChangeProductCode": 0.0,
        "Glue_CleaningPaper": 0.0,
        "Others": 0.0,
    }

    for row in rows:
        day_raw = row["Days"]
        day_str = (
            day_raw.strftime("%Y-%m-%d") if hasattr(day_raw, "strftime") else str(day_raw)
        )

        categories = {
            "Operation": float(row["Operation"] or 0.0),
            "SmallStop": float(row["SmallStop"] or 0.0),
            "Fault": float(row["Fault"] or 0.0),
            "Break": float(row["Break"] or 0.0),
            "Maintenance": float(row["Maintenance"] or 0.0),
            "Eat": float(row["Eat"] or 0.0),
            "Waiting": float(row["Waiting"] or 0.0),
            "MachineryEdit": float(row["MachineryEdit"] or 0.0),
            "ChangeProductCode": float(row["ChangeProductCode"] or 0.0),
            "Glue_CleaningPaper": float(row["Glue_CleaningPaper"] or 0.0),
            "Others": float(row["Others"] or 0.0),
        }

        # cộng dồn totals
        for k in monthly_totals:
            monthly_totals[k] += categories[k]

        days.append({
            "day": day_str,
            "categories": categories,
        })

    result = {
        "machine_id": machine_id,
        "month": month,
        "days": days,
        "monthly_totals": {k: round(v, 2) for k, v in monthly_totals.items()},
    }

    return jsonify(result)
@app.route("/api/machines/<int:machine_id>/month-export", methods=["GET"])
def export_machine_month_excel(machine_id):
    try:
        month = int(request.args.get("month"))
    except (TypeError, ValueError):
        return jsonify({"error": "Missing or invalid month param"}), 400

    data_type = request.args.get("data", "ALL")

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # --- 1. LẤY TÊN MÁY THEO ID ---
    # Nếu bạn có bảng machines(MachineID, MachineName) thì sửa query này lại cho đúng
    cursor.execute(
        "SELECT MachineName FROM machine WHERE MachineID = %s",
        (machine_id,),
    )
    mrow = cursor.fetchone()
    machine_name = (
        mrow["MachineName"] if mrow and mrow.get("MachineName") else f"Machine_{machine_id}"
    )

    # --- 2. LẤY DỮ LIỆU THÁNG TỪ dayvalues ---
    cursor.execute(
        """
        SELECT
            Days,
            OEERatio,
            OKProductRatio,
            OutputRatio,
            ActivityRatio,
            Operation,
            SmallStop,
            Fault,
            Break,
            Maintenance,
            Eat,
            Waiting,
            MachineryEdit,
            ChangeProductCode,
            Glue_CleaningPaper,
            Others
        FROM sdvn.dayvalues
        WHERE MachineID = %s
          AND MONTH(Days) = %s
        ORDER BY Days
        """,
        (machine_id, month),
    )
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # --- 3. TẠO EXCEL ---
    wb = Workbook()
    ws = wb.active
    ws.title = machine_name

    # Dòng thông tin chung
    ws.append(
        [
            f"Machine: {machine_name}",
            f"Month: {month}",
            f"Data filter: {data_type}",
        ]
    )
    ws.append([])

    # Header cột
    headers = [
        "Date",
        # Ratio
        "OEERatio",
        "OKProductRatio",
        "OutputRatio",
        "ActivityRatio",
        # Time (giờ)
        "Operation",
        "SmallStop",
        "Fault",
        "Break",
        "Maintenance",
        "Eat",
        "Waiting",
        "MachineryEdit",
        "ChangeProductCode",
        "Glue_CleaningPaper",
        "Others",
        # Time (%)
        "OperationPct",
        "SmallStopPct",
        "FaultPct",
        "BreakPct",
        "MaintenancePct",
        "EatPct",
        "WaitingPct",
        "MachineryEditPct",
        "ChangeProductCodePct",
        "Glue_CleaningPaperPct",
        "OthersPct",
    ]
    ws.append(headers)

    # Ghi từng ngày
    for row in rows:
        # Date
        day_raw = row["Days"]
        day_str = (
            day_raw.strftime("%Y-%m-%d")
            if hasattr(day_raw, "strftime")
            else str(day_raw)
        )

        # Ratio
        oee = float(row.get("OEERatio") or 0.0)
        okr = float(row.get("OKProductRatio") or 0.0)
        out = float(row.get("OutputRatio") or 0.0)
        act = float(row.get("ActivityRatio") or 0.0)

        # Time (giờ)
        op = float(row.get("Operation") or 0.0)
        ss = float(row.get("SmallStop") or 0.0)
        flt = float(row.get("Fault") or 0.0)
        brk = float(row.get("Break") or 0.0)
        mt = float(row.get("Maintenance") or 0.0)
        eat = float(row.get("Eat") or 0.0)
        wait = float(row.get("Waiting") or 0.0)
        me = float(row.get("MachineryEdit") or 0.0)
        cpc = float(row.get("ChangeProductCode") or 0.0)
        gcp = float(row.get("Glue_CleaningPaper") or 0.0)
        oth = float(row.get("Others") or 0.0)

        total_time = (
            op
            + ss
            + flt
            + brk
            + mt
            + eat
            + wait
            + me
            + cpc
            + gcp
            + oth
        )

        def pct(val: float) -> float:
            if total_time <= 0:
                return 0.0
            return round((val * 100.0) / total_time, 2)

        ws.append(
            [
                day_str,
                # Ratio
                oee,
                okr,
                out,
                act,
                # Time (giờ)
                op,
                ss,
                flt,
                brk,
                mt,
                eat,
                wait,
                me,
                cpc,
                gcp,
                oth,
                # Time (%)
                pct(op),
                pct(ss),
                pct(flt),
                pct(brk),
                pct(mt),
                pct(eat),
                pct(wait),
                pct(me),
                pct(cpc),
                pct(gcp),
                pct(oth),
            ]
        )

    # --- 4. KẺ BẢNG (BORDER) CHO TẤT CẢ Ô ---
    thin = Side(style="thin")
    thin_border = Border(left=thin, right=thin, top=thin, bottom=thin)

    max_row = ws.max_row
    max_col = ws.max_column

    for row in ws.iter_rows(
        min_row=1, max_row=max_row, min_col=1, max_col=max_col
    ):
        for cell in row:
            cell.border = thin_border

    # --- 5. LƯU RA BUFFER ---
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    # --- 6. TÊN FILE: tenmay_thang.xlsx (VD: LINE_01_09.xlsx) ---
    #safe_name = "".join(
      #  ch if ch.isalnum() or ch == " " else "_" for ch in machine_name
   # )
   # safe_name = safe_name.replace(" ", "_")
    filename = f"{machine_name}_{month:02d}.xlsx"

    try:
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except TypeError:
        # fallback nếu Flask cũ
        return send_file(
            output,
            as_attachment=True,
            attachment_filename=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
from flask import request, jsonify

@app.route("/api/machines/<int:machine_id>/year-ratio", methods=["GET"])
def get_machine_year_ratio(machine_id):
    """
    Ratio theo NĂM, luôn trả đủ 12 tháng.
    Tháng không có dữ liệu => ratio = 0
    """
    try:
        year = int(request.args.get("year"))
    except (TypeError, ValueError):
        return jsonify({"error": "Missing or invalid year param"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT
            MONTH(Days) AS m,
            AVG(OEERatio)       AS avg_oee,
            AVG(OKProductRatio) AS avg_ok,
            AVG(OutputRatio)    AS avg_output,
            AVG(ActivityRatio)  AS avg_activity
        FROM sdvn.dayvalues
        WHERE MachineID = %s
          AND YEAR(Days) = %s
        GROUP BY MONTH(Days)
        ORDER BY m
        """,
        (machine_id, year),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Chuyển thành dict theo tháng
    month_map = {int(r["m"]): r for r in rows}

    months = []
    for m in range(1, 13):  # luôn trả đủ 12 tháng
        if m in month_map:
            r = month_map[m]
            months.append(
                {
                    "month": m,
                    "oee": float(r.get("avg_oee") or 0.0),
                    "ok_ratio": float(r.get("avg_ok") or 0.0),
                    "output_ratio": float(r.get("avg_output") or 0.0),
                    "activity_ratio": float(r.get("avg_activity") or 0.0),
                }
            )
        else:
            months.append(
                {
                    "month": m,
                    "oee": 0,
                    "ok_ratio": 0,
                    "output_ratio": 0,
                    "activity_ratio": 0,
                }
            )

    return jsonify({"months": months})
@app.route("/api/machines/<int:machine_id>/year", methods=["GET"])
def get_machine_year_time(machine_id):
    """
    Thời gian theo NĂM, luôn trả đủ 12 tháng.
    Tháng không có dữ liệu => tất cả các field = 0
    """
    try:
        year = int(request.args.get("year"))
    except (TypeError, ValueError):
        return jsonify({"error": "Missing or invalid year param"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT
            MONTH(Days) AS m,
            SUM(Operation)          AS op,
            SUM(SmallStop)          AS ss,
            SUM(Fault)              AS flt,
            SUM(`Break`)            AS brk,
            SUM(Maintenance)        AS mt,
            SUM(Eat)                AS eat,
            SUM(Waiting)            AS w,
            SUM(MachineryEdit)      AS me,
            SUM(ChangeProductCode)  AS cpc,
            SUM(Glue_CleaningPaper) AS gcp,
            SUM(Others)             AS oth
        FROM sdvn.dayvalues
        WHERE MachineID = %s
          AND YEAR(Days) = %s
        GROUP BY MONTH(Days)
        ORDER BY m
        """,
        (machine_id, year),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # Dict theo tháng
    month_map = {int(r["m"]): r for r in rows}

    months = []
    for m in range(1, 13):  # luôn trả 1..12
        if m in month_map:
            r = month_map[m]
            months.append(
                {
                    "month": m,
                    "categories": {
                        "Operation": float(r.get("op") or 0.0),
                        "SmallStop": float(r.get("ss") or 0.0),
                        "Fault": float(r.get("flt") or 0.0),
                        "Break": float(r.get("brk") or 0.0),
                        "Maintenance": float(r.get("mt") or 0.0),
                        "Eat": float(r.get("eat") or 0.0),
                        "Waiting": float(r.get("w") or 0.0),
                        "MachineryEdit": float(r.get("me") or 0.0),
                        "ChangeProductCode": float(r.get("cpc") or 0.0),
                        "Glue_CleaningPaper": float(r.get("gcp") or 0.0),
                        "Others": float(r.get("oth") or 0.0),
                    },
                }
            )
        else:
            months.append(
                {
                    "month": m,
                    "categories": {
                        "Operation": 0,
                        "SmallStop": 0,
                        "Fault": 0,
                        "Break": 0,
                        "Maintenance": 0,
                        "Eat": 0,
                        "Waiting": 0,
                        "MachineryEdit": 0,
                        "ChangeProductCode": 0,
                        "Glue_CleaningPaper": 0,
                        "Others": 0,
                    },
                }
            )

    return jsonify({"months": months})
@app.route("/api/machines/<int:machine_id>/year-export", methods=["GET"])
def export_machine_year_excel(machine_id):
    """
    Xuất Excel (.xlsx) dữ liệu NĂM cho 1 máy – 1 sheet, 12 dòng (tháng 1..12)

    Mỗi dòng:
    - Month
    - OEERatio, OKProductRatio, OutputRatio, ActivityRatio (TB theo tháng)
    - Operation..Others (tổng giờ theo tháng)
    - OperationPct..OthersPct (tỷ lệ %, làm tròn 2 số)
    """
    # --- Lấy YEAR ---
    try:
        year = int(request.args.get("year"))
    except (TypeError, ValueError):
        return jsonify({"error": "Missing or invalid year param"}), 400

    data_type = request.args.get("data", "ALL")  # để ghi chú trong header

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # --- 1. LẤY TÊN MÁY THEO ID (productionline) ---
    cursor.execute(
        "SELECT MachineName FROM machine WHERE MachineID = %s",
        (machine_id,),
    )
    mrow = cursor.fetchone()
    machine_name = (
        mrow["MachineName"] if mrow and mrow.get("MachineName") else f"Machine_{machine_id}"
    )

    # --- 2. LẤY DỮ LIỆU NĂM (GỘP THEO THÁNG) ---
    cursor.execute(
        """
        SELECT
            MONTH(Days)           AS m,
            AVG(OEERatio)         AS avg_oee,
            AVG(OKProductRatio)   AS avg_ok,
            AVG(OutputRatio)      AS avg_output,
            AVG(ActivityRatio)    AS avg_activity,
            SUM(Operation)        AS sum_op,
            SUM(SmallStop)        AS sum_small,
            SUM(Fault)            AS sum_fault,
            SUM(`Break`)          AS sum_break,
            SUM(Maintenance)      AS sum_maint,
            SUM(Eat)              AS sum_eat,
            SUM(Waiting)          AS sum_wait,
            SUM(MachineryEdit)    AS sum_me,
            SUM(ChangeProductCode)  AS sum_cpc,
            SUM(Glue_CleaningPaper) AS sum_gcp,
            SUM(Others)             AS sum_oth
        FROM sdvn.dayvalues
        WHERE MachineID = %s
          AND YEAR(Days) = %s
        GROUP BY MONTH(Days)
        ORDER BY m
        """,
        (machine_id, year),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # map theo tháng
    month_map = {int(r["m"]): r for r in rows}

    # --- 3. TẠO EXCEL ---
    wb = Workbook()
    ws = wb.active
    ws.title = machine_name

    # Header thông tin chung
    ws.append(
        [
            f"MachineName: {machine_name}",
            f"Year: {year}",
            f"Data filter: {data_type}",
        ]
    )
    ws.append([])

    # Header cột
    headers = [
        "Month",
        # Ratio
        "OEERatio",
        "OKProductRatio",
        "OutputRatio",
        "ActivityRatio",
        # Time (giờ)
        "Operation",
        "SmallStop",
        "Fault",
        "Break",
        "Maintenance",
        "Eat",
        "Waiting",
        "MachineryEdit",
        "ChangeProductCode",
        "Glue_CleaningPaper",
        "Others",
        # Time (%)
        "OperationPct",
        "SmallStopPct",
        "FaultPct",
        "BreakPct",
        "MaintenancePct",
        "EatPct",
        "WaitingPct",
        "MachineryEditPct",
        "ChangeProductCodePct",
        "Glue_CleaningPaperPct",
        "OthersPct",
    ]
    ws.append(headers)

    # helper tính %
    def pct_part(val, total):
        if not total or total <= 0:
            return 0.0
        return round((val * 100.0) / total, 2)

    # Ghi đủ 12 tháng
    for m in range(1, 13):
        r = month_map.get(m)

        if r:
            oee = float(r.get("avg_oee") or 0.0)
            okr = float(r.get("avg_ok") or 0.0)
            out = float(r.get("avg_output") or 0.0)
            act = float(r.get("avg_activity") or 0.0)

            op   = float(r.get("sum_op") or 0.0)
            ss   = float(r.get("sum_small") or 0.0)
            flt  = float(r.get("sum_fault") or 0.0)
            brk  = float(r.get("sum_break") or 0.0)
            mt   = float(r.get("sum_maint") or 0.0)
            eat  = float(r.get("sum_eat") or 0.0)
            wait = float(r.get("sum_wait") or 0.0)
            me   = float(r.get("sum_me") or 0.0)
            cpc  = float(r.get("sum_cpc") or 0.0)
            gcp  = float(r.get("sum_gcp") or 0.0)
            oth  = float(r.get("sum_oth") or 0.0)
        else:
            oee = okr = out = act = 0.0
            op = ss = flt = brk = mt = eat = wait = me = cpc = gcp = oth = 0.0

        total_time = (
            op
            + ss
            + flt
            + brk
            + mt
            + eat
            + wait
            + me
            + cpc
            + gcp
            + oth
        )

        ws.append(
            [
                m,          # Month
                # Ratio
                oee,
                okr,
                out,
                act,
                # Time
                op,
                ss,
                flt,
                brk,
                mt,
                eat,
                wait,
                me,
                cpc,
                gcp,
                oth,
                # Time %
                pct_part(op, total_time),
                pct_part(ss, total_time),
                pct_part(flt, total_time),
                pct_part(brk, total_time),
                pct_part(mt, total_time),
                pct_part(eat, total_time),
                pct_part(wait, total_time),
                pct_part(me, total_time),
                pct_part(cpc, total_time),
                pct_part(gcp, total_time),
                pct_part(oth, total_time),
            ]
        )

    # --- 4. KẺ BẢNG (BORDER) ---
    thin = Side(style="thin")
    thin_border = Border(left=thin, right=thin, top=thin, bottom=thin)

    max_row = ws.max_row
    max_col = ws.max_column
    for row in ws.iter_rows(min_row=1, max_row=max_row, min_col=1, max_col=max_col):
        for cell in row:
            cell.border = thin_border

    # --- 5. GHI RA BUFFER & TÊN FILE ---
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    safe_name = "".join(
        ch if ch.isalnum() or ch == " " else "_" for ch in machine_name
    )
    safe_name = safe_name.replace(" ", "_")
    filename = f"{safe_name}_nam_{year}.xlsx"

    try:
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except TypeError:
        # nếu Flask cũ
        return send_file(
            output,
            as_attachment=True,
            attachment_filename=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)