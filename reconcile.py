"""
核心对账引擎
三步比对逻辑：
  1. 行数校验：收货单行数 vs 对账单行数
  2. 明细校验：数量 + 单价逐行比对
  3. 总额校验：SUM(行金额) vs 对账单.单据总额
输出：带标红批注的 Excel 结果文件
"""
import os, json, datetime, traceback
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.styles import fills
from openpyxl.comments import Comment
from sqlalchemy.orm import Session

from database import SessionLocal
import models

RED_FILL   = PatternFill("solid", fgColor="FFE0E0")
RED_FONT   = Font(color="CC0000", bold=True)
GREEN_FILL = PatternFill("solid", fgColor="E0FFE8")
GREEN_FONT = Font(color="006400")
HEAD_FILL  = PatternFill("solid", fgColor="1F4E79")
HEAD_FONT  = Font(color="FFFFFF", bold=True)
WARN_FILL  = PatternFill("solid", fgColor="FFF3CD")
THIN_BORDER = Border(
    left=Side(style="thin", color="CCCCCC"),
    right=Side(style="thin", color="CCCCCC"),
    top=Side(style="thin", color="CCCCCC"),
    bottom=Side(style="thin", color="CCCCCC"),
)


def run(task_id: str, file_paths: dict, result_dir: str):
    """后台任务入口，被 FastAPI background_tasks 调用"""
    db: Session = SessionLocal()
    try:
        task = db.query(models.Task).filter(models.Task.id == task_id).first()
        if not task:
            return

        task.status = "processing"
        db.commit()

        summary, result_path = _reconcile(task_id, file_paths, result_dir, task.name)

        task.status = "done"
        task.result_path = result_path
        task.result_summary = json.dumps(summary, ensure_ascii=False)
        task.finished_at = datetime.datetime.utcnow()
        db.commit()

    except Exception as e:
        tb = traceback.format_exc()
        task = db.query(models.Task).filter(models.Task.id == task_id).first()
        if task:
            task.status = "failed"
            task.error_msg = f"{str(e)}\n{tb}"
            task.finished_at = datetime.datetime.utcnow()
            db.commit()
    finally:
        db.close()


def _read_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xlsm"):
        return pd.read_excel(path, engine="openpyxl")
    elif ext == ".xls":
        return pd.read_excel(path, engine="xlrd")
    elif ext == ".csv":
        for enc in ("utf-8-sig", "gbk", "utf-8"):
            try:
                return pd.read_csv(path, encoding=enc)
            except UnicodeDecodeError:
                continue
    raise ValueError(f"不支持的文件格式：{ext}")


def _normalize_cols(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """列名模糊匹配归一化"""
    rename = {}
    for col in df.columns:
        col_s = str(col).strip()
        for target, keywords in mapping.items():
            if any(kw in col_s for kw in keywords):
                rename[col] = target
                break
    return df.rename(columns=rename)


def _reconcile(task_id: str, file_paths: dict, result_dir: str, task_name: str):
    # ── 读取三个文件 ──────────────────────────────────
    po_df   = _read_file(file_paths["po"])
    recv_df = _read_file(file_paths["recv"])
    stmt_df = _read_file(file_paths["stmt"])

    # ── 列名归一化 ────────────────────────────────────
    po_df = _normalize_cols(po_df, {
        "采购单号": ["采购单号","订单号","PO号","PO_NO","po_no","PONO"],
        "品名":    ["品名","品名/规格","物料名称","物料描述","名称","商品名"],
        "单价":    ["单价","含税单价","采购单价","price","Price","unit_price"],
    })
    recv_df = _normalize_cols(recv_df, {
        "采购单号": ["采购单号","订单号","PO号","PO_NO","po_no","PONO"],
        "品名":    ["品名","品名/规格","物料名称","物料描述","名称","商品名"],
        "数量":    ["数量","收货数量","实收数量","qty","Qty","quantity"],
    })
    stmt_df = _normalize_cols(stmt_df, {
        "采购单号": ["采购单号","订单号","PO号","PO_NO","po_no","PONO"],
        "品名":    ["品名","品名/规格","物料名称","物料描述","名称","商品名"],
        "数量":    ["数量","对账数量","结算数量","qty","Qty","quantity"],
        "单价":    ["单价","含税单价","结算单价","price","Price","unit_price"],
        "行金额":  ["行金额","金额","小计","line_amount","amount"],
        "单据总额":["单据总额","总额","合计","发票金额","total","Total"],
    })

    # ── 必需字段检查 ──────────────────────────────────
    _check_cols(po_df,   ["采购单号","品名","单价"],   "采购订单表")
    _check_cols(recv_df, ["采购单号","品名","数量"],   "收货单表")
    _check_cols(stmt_df, ["采购单号","品名","数量","单价","行金额","单据总额"], "电子对账单")

    # 统一 key
    for df in (po_df, recv_df, stmt_df):
        df["_key"] = df["采购单号"].astype(str).str.strip() + "||" + df["品名"].astype(str).str.strip()

    details  = []
    anomalies = 0
    anomaly_amt = 0.0
    total_rows  = 0

    po_numbers = stmt_df["采购单号"].astype(str).str.strip().unique()

    for po_no in po_numbers:
        stmt_rows = stmt_df[stmt_df["采购单号"].astype(str).str.strip() == po_no]
        recv_rows = recv_df[recv_df["采购单号"].astype(str).str.strip() == po_no]

        # 校验1 - 行数
        if len(recv_rows) != len(stmt_rows):
            for _, srow in stmt_rows.iterrows():
                total_rows += 1
                anomalies  += 1
                amt = float(srow.get("行金额", 0) or 0)
                anomaly_amt += amt
                details.append({
                    "po_no":      po_no,
                    "item":       str(srow.get("品名","")).strip(),
                    "check_type": "行数异常",
                    "recv_qty":   None,
                    "stmt_qty":   float(srow.get("数量",0) or 0),
                    "po_price":   None,
                    "stmt_price": float(srow.get("单价",0) or 0),
                    "line_amt":   amt,
                    "anomaly":    True,
                    "note":       f"收货单{len(recv_rows)}行 vs 对账单{len(stmt_rows)}行，行数不一致",
                })
            continue

        # 校验2 - 明细逐行比对（按品名匹配）
        po_all_ok = True
        for _, srow in stmt_rows.iterrows():
            total_rows += 1
            key  = str(srow.get("品名","")).strip()
            amt  = float(srow.get("行金额", 0) or 0)
            s_qty   = float(srow.get("数量",0) or 0)
            s_price = float(srow.get("单价",0) or 0)

            rrow = recv_rows[recv_rows["品名"].astype(str).str.strip() == key]
            prow = po_df[(po_df["采购单号"].astype(str).str.strip()==po_no) &
                         (po_df["品名"].astype(str).str.strip()==key)]

            r_qty   = float(rrow["数量"].values[0]) if len(rrow) else None
            p_price = float(prow["单价"].values[0]) if len(prow) else None

            errors = []
            is_anomaly = False

            if r_qty is None:
                errors.append(f"收货单中未找到品名[{key}]")
                is_anomaly = True
            elif abs(r_qty - s_qty) > 0.001:
                errors.append(f"数量差异：收货单{r_qty} vs 对账单{s_qty}")
                is_anomaly = True

            if p_price is None:
                errors.append(f"采购单中未找到品名[{key}]")
                is_anomaly = True
            elif abs(p_price - s_price) > 0.001:
                errors.append(f"单价差异：采购单¥{p_price} vs 对账单¥{s_price}")
                is_anomaly = True

            if is_anomaly:
                anomalies  += 1
                anomaly_amt += amt
                po_all_ok   = False

            details.append({
                "po_no":      po_no,
                "item":       key,
                "check_type": "、".join(errors) if errors else "通过",
                "recv_qty":   r_qty,
                "stmt_qty":   s_qty,
                "po_price":   p_price,
                "stmt_price": s_price,
                "line_amt":   amt,
                "anomaly":    is_anomaly,
                "note":       "；".join(errors) if errors else "—",
            })

        # 校验3 - 总额（仅当该PO下所有明细通过）
        if po_all_ok:
            calc_total = stmt_rows["行金额"].astype(float).sum()
            stmt_total_vals = stmt_rows["单据总额"].astype(float)
            stmt_total = float(stmt_total_vals.values[0]) if len(stmt_total_vals) else None
            if stmt_total is not None and abs(calc_total - stmt_total) > 0.01:
                # 在最后一行附加总额异常说明
                details.append({
                    "po_no":      po_no,
                    "item":       "【总额校验】",
                    "check_type": "总额异常",
                    "recv_qty":   None, "stmt_qty": None,
                    "po_price":   None, "stmt_price": None,
                    "line_amt":   stmt_total,
                    "anomaly":    True,
                    "note":       f"明细合计¥{calc_total:.2f} vs 单据总额¥{stmt_total:.2f}，差额¥{calc_total-stmt_total:.2f}",
                })
                anomalies  += 1
                anomaly_amt += abs(calc_total - stmt_total)

    # ── 生成 Excel 结果文件 ───────────────────────────
    os.makedirs(result_dir, exist_ok=True)
    result_path = os.path.join(result_dir, f"{task_id}_result.xlsx")
    _write_excel(result_path, task_name, details, po_df, recv_df, stmt_df)

    summary = {
        "total":       total_rows,
        "passed":      total_rows - anomalies,
        "anomalies":   anomalies,
        "anomaly_amt": round(anomaly_amt, 2),
        "details":     details,
    }
    return summary, result_path


def _check_cols(df, required, name):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"【{name}】缺少必需字段：{missing}。实际字段：{list(df.columns)}")


def _write_excel(path: str, task_name: str, details: list, po_df, recv_df, stmt_df):
    wb = Workbook()

    # ── Sheet1: 对账明细 ──────────────────────────────
    ws = wb.active
    ws.title = "对账明细（含批注）"

    headers = ["采购单号","品名","校验结果","收货单数量","对账单数量",
               "采购单单价","对账单单价","行金额","异常说明","状态"]
    col_widths = [14, 18, 14, 12, 12, 12, 12, 12, 36, 10]

    for ci, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=1, column=ci, value=h)
        cell.fill      = HEAD_FILL
        cell.font      = HEAD_FONT
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border    = THIN_BORDER
        ws.column_dimensions[cell.column_letter].width = w
    ws.row_dimensions[1].height = 22

    for ri, d in enumerate(details, 2):
        row_data = [
            d["po_no"], d["item"], d["check_type"],
            d["recv_qty"], d["stmt_qty"],
            d["po_price"], d["stmt_price"],
            d["line_amt"], d["note"],
            "⚠ 异常" if d["anomaly"] else "✅ 通过"
        ]
        is_anom = d["anomaly"]
        for ci, val in enumerate(row_data, 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.border = THIN_BORDER
            cell.alignment = Alignment(vertical="center")
            if is_anom:
                cell.fill = RED_FILL
                cell.font = RED_FONT
            else:
                cell.fill = GREEN_FILL
                cell.font = GREEN_FONT

        # 为异常单元格添加批注
        if is_anom and d["note"] and d["note"] != "—":
            note_cell = ws.cell(row=ri, column=9)
            comment = Comment(d["note"], "ReconCore系统")
            comment.width  = 280
            comment.height = 80
            note_cell.comment = comment

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:J{len(details)+1}"

    # ── Sheet2: 汇总报告 ──────────────────────────────
    ws2 = wb.create_sheet("汇总报告")
    total   = len(details)
    anom    = sum(1 for d in details if d["anomaly"])
    passed  = total - anom
    anom_amt= sum(d["line_amt"] for d in details if d["anomaly"] and d["line_amt"])

    summary_rows = [
        ("对账任务",    task_name),
        ("生成时间",    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        ("",""),
        ("对账总行数",  total),
        ("通过行数",    passed),
        ("异常行数",    anom),
        ("异常总金额",  f"¥{anom_amt:,.2f}"),
        ("",""),
        ("对账结论",    "✅ 全部通过" if anom == 0 else f"⚠️ 存在 {anom} 项异常，请复查"),
    ]
    ws2.column_dimensions["A"].width = 16
    ws2.column_dimensions["B"].width = 36
    for ri, (k, v) in enumerate(summary_rows, 1):
        ka = ws2.cell(row=ri, column=1, value=k)
        va = ws2.cell(row=ri, column=2, value=v)
        ka.font = Font(bold=True, color="1F4E79")
        if k == "对账结论":
            va.font = Font(bold=True, color="CC0000" if anom > 0 else "006400")
            va.fill = RED_FILL if anom > 0 else GREEN_FILL

    # ── Sheet3: 原始文件预览 ──────────────────────────
    for sheet_name, df in [("采购订单_原始", po_df), ("收货单_原始", recv_df), ("对账单_原始", stmt_df)]:
        ws_raw = wb.create_sheet(sheet_name)
        for ci, col in enumerate(df.columns, 1):
            cell = ws_raw.cell(row=1, column=ci, value=str(col))
            cell.fill = HEAD_FILL
            cell.font = HEAD_FONT
            ws_raw.column_dimensions[cell.column_letter].width = 16
        for ri, row in enumerate(df.itertuples(index=False), 2):
            for ci, val in enumerate(row, 1):
                ws_raw.cell(row=ri, column=ci, value=val)

    wb.save(path)
