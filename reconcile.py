"""
核心对账引擎 v4
修复：
  1. 采购单合并单元格 → 自动向下填充采购单号
  2. 收货单「关联单据号」→ 识别为采购单号
  3. 含税/不含税单价智能优先
关联键：采购单号 + 品名
"""
import os, json, datetime, traceback
import pandas as pd
import numpy as np
from openpyxl import load_workbook, Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.comments import Comment
from sqlalchemy.orm import Session

from database import SessionLocal
import models

RED_FILL    = PatternFill("solid", fgColor="FFE0E0")
RED_FONT    = Font(color="CC0000", bold=True)
GREEN_FILL  = PatternFill("solid", fgColor="E0FFE8")
GREEN_FONT  = Font(color="006400")
HEAD_FILL   = PatternFill("solid", fgColor="1F4E79")
HEAD_FONT   = Font(color="FFFFFF", bold=True)
THIN_BORDER = Border(
    left=Side(style="thin", color="CCCCCC"),
    right=Side(style="thin", color="CCCCCC"),
    top=Side(style="thin", color="CCCCCC"),
    bottom=Side(style="thin", color="CCCCCC"),
)

COL_KEYWORDS = {
    # 采购单号：收货单里叫「关联单据号」也要识别
    "采购单号":  ["采购单号","采购单","关联单据号","订单号","采购编号","PO号","PO_NO","po_no","PONO","采购订单"],
    "品名":      ["品名","品名/规格","物料名称","物料描述","商品名称","货品名","名称","产品名","品种","货描"],
    "含税单价":  ["含税单价","含税价","税含单价","含增值税单价","价税合一单价"],
    "单价":      ["单价","采购单价","结算单价","price","Price","unit_price","不含税单价","未税单价"],
    "数量":      ["到货量","数量","收货数量","实收数量","结算数量","对账数量","qty","Qty","quantity","通知收货量","实际数量"],
    "行金额":    ["行金额","金额","小计","价税合计","含税金额","line_amount","amount","总价","合价","价格小计"],
    "单据总额":  ["单据总额","总额","合计","发票金额","total","Total","月结金额","对账总额","合计金额","总金额"],
}

SUMMARY_KW = ["合计","汇总","小计","总计","合计（大写）","grand total","subtotal","合计:","合计："]


def run(task_id: str, file_paths: dict, result_dir: str):
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


def _find_header_row(all_rows: list, needed_keys: list) -> int:
    best_idx, best_score = 0, 0
    for ri, row in enumerate(all_rows[:40]):
        row_text = " ".join([str(v) for v in row])
        score = sum(
            1 for t in needed_keys
            if any(kw in row_text for kw in COL_KEYWORDS.get(t, [t]))
        )
        if score > best_score:
            best_score, best_idx = score, ri
    return best_idx


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all")
    def is_summary(row):
        return any(any(kw in str(v) for kw in SUMMARY_KW) for v in row)
    df = df[~df.apply(is_summary, axis=1)].reset_index(drop=True)
    unnamed = [c for c in df.columns if str(c).startswith("Unnamed") and df[c].isna().all()]
    return df.drop(columns=unnamed)


def _smart_read(path: str, needed_keys: list) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        for enc in ("utf-8-sig", "gbk", "utf-8"):
            try:
                raw = pd.read_csv(path, encoding=enc, header=None, dtype=str)
                hi = _find_header_row(raw.fillna("").values.tolist(), needed_keys)
                df = pd.read_csv(path, encoding=enc, header=hi, dtype=str)
                return _clean_df(df)
            except UnicodeDecodeError:
                continue
        raise ValueError(f"CSV编码识别失败：{os.path.basename(path)}")

    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    all_rows = [[str(v).strip() if v is not None else "" for v in row]
                for row in ws.iter_rows(values_only=True)]
    wb.close()
    if not all_rows:
        raise ValueError(f"文件为空：{os.path.basename(path)}")
    hi = _find_header_row(all_rows, needed_keys)
    engine = "openpyxl" if ext in (".xlsx", ".xlsm") else "xlrd"
    df = pd.read_excel(path, engine=engine, header=hi)
    return _clean_df(df)


def _normalize_cols(df: pd.DataFrame, targets: list) -> pd.DataFrame:
    rename, used = {}, set()
    for col in df.columns:
        col_s = str(col).strip()
        for target in targets:
            if target in used:
                continue
            if any(kw in col_s for kw in COL_KEYWORDS.get(target, [target])):
                rename[col] = target
                used.add(target)
                break
    return df.rename(columns=rename)


def _normalize_po_price_cols(df: pd.DataFrame) -> pd.DataFrame:
    """采购单专用：含税单价和单价分别识别"""
    rename = {}
    tax_col = plain_col = None
    for col in df.columns:
        col_s = str(col).strip()
        if tax_col is None and any(kw in col_s for kw in COL_KEYWORDS["含税单价"]):
            tax_col = col
            rename[col] = "含税单价"
        elif plain_col is None and col != tax_col and any(kw in col_s for kw in COL_KEYWORDS["单价"]):
            plain_col = col
            rename[col] = "单价"
    return df.rename(columns=rename)


def _check_cols(df, required, name):
    missing = [c for c in required if c not in df.columns]
    if missing:
        avail = [c for c in df.columns if not str(c).startswith("Unnamed")]
        raise ValueError(
            f"【{name}】未能识别字段：{missing}\n"
            f"文件实际列名：{avail}"
        )


def _to_float(v):
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        s = str(v).replace(",", "").replace("¥", "").replace("￥", "").strip()
        if s == "" or s.lower() in ("nan", "none", "-"):
            return None
        return float(s)
    except Exception:
        return None


def _to_float_d(v, default=0.0):
    r = _to_float(v)
    return r if r is not None else default


def _to_str(v):
    if v is None:
        return ""
    try:
        if isinstance(v, float) and np.isnan(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _get_po_price(prow) -> tuple:
    """含税单价有值优先，否则用单价"""
    tax   = _to_float(prow.get("含税单价")) if "含税单价" in prow.index else None
    plain = _to_float(prow.get("单价"))     if "单价"    in prow.index else None
    if tax is not None and tax > 0:
        return tax, "含税单价"
    if plain is not None and plain > 0:
        return plain, "单价"
    return None, "未找到"


def _ffill_po_no(df: pd.DataFrame) -> pd.DataFrame:
    """
    关键修复：采购单合并单元格导致采购单号出现NaN
    用 forward fill 向下填充，让每一行都有采购单号
    """
    if "采购单号" in df.columns:
        df["采购单号"] = df["采购单号"].replace("", np.nan)
        df["采购单号"] = df["采购单号"].ffill()
    return df


def _reconcile(task_id, file_paths, result_dir, task_name):
    # 读取
    po_df   = _smart_read(file_paths["po"],   ["采购单号", "品名", "含税单价", "单价"])
    recv_df = _smart_read(file_paths["recv"], ["采购单号", "品名", "数量"])
    stmt_df = _smart_read(file_paths["stmt"], ["采购单号", "品名", "数量", "单价", "行金额"])

    # 列名归一化
    po_df   = _normalize_po_price_cols(po_df)
    po_df   = _normalize_cols(po_df,   ["采购单号", "品名"])
    recv_df = _normalize_cols(recv_df, ["采购单号", "品名", "数量"])
    stmt_df = _normalize_cols(stmt_df, ["采购单号", "品名", "数量", "单价", "行金额", "单据总额"])

    # ── 核心修复：采购单合并单元格向下填充 ────────────
    po_df = _ffill_po_no(po_df)

    # 检查必需字段
    if "含税单价" not in po_df.columns and "单价" not in po_df.columns:
        raise ValueError("【采购订单表】未找到单价，需包含「含税单价」或「单价」列")
    _check_cols(po_df,   ["采购单号", "品名"],                           "采购订单表")
    _check_cols(recv_df, ["采购单号", "品名", "数量"],                   "收货单表")
    _check_cols(stmt_df, ["采购单号", "品名", "数量", "单价", "行金额"], "电子对账单")

    # 清洗字符串
    for df in (po_df, recv_df, stmt_df):
        df["采购单号"] = df["采购单号"].apply(_to_str)
        df["品名"]     = df["品名"].apply(_to_str)
    po_df   = po_df[po_df["采购单号"] != ""].copy()
    recv_df = recv_df[recv_df["采购单号"] != ""].copy()
    stmt_df = stmt_df[stmt_df["采购单号"] != ""].copy()

    has_total   = "单据总额" in stmt_df.columns
    details     = []
    anomalies   = 0
    anomaly_amt = 0.0
    total_rows  = 0

    for po_no in stmt_df["采购单号"].unique():
        if not po_no:
            continue
        stmt_rows = stmt_df[stmt_df["采购单号"] == po_no]
        recv_rows = recv_df[recv_df["采购单号"] == po_no]

        # 校验1 — 行数
        if len(recv_rows) != len(stmt_rows):
            for _, srow in stmt_rows.iterrows():
                total_rows += 1; anomalies += 1
                amt = _to_float_d(srow.get("行金额", 0)); anomaly_amt += amt
                details.append({
                    "po_no": po_no, "item": _to_str(srow.get("品名","")),
                    "check_type": "行数异常", "recv_qty": None,
                    "stmt_qty": _to_float_d(srow.get("数量",0)),
                    "po_price": None, "po_price_src": "—",
                    "stmt_price": _to_float_d(srow.get("单价",0)),
                    "line_amt": amt, "anomaly": True,
                    "note": f"收货单{len(recv_rows)}行 vs 对账单{len(stmt_rows)}行，行数不一致"
                })
            continue

        # 校验2 — 逐行比对
        po_all_ok = True
        for _, srow in stmt_rows.iterrows():
            total_rows += 1
            item    = _to_str(srow.get("品名", ""))
            amt     = _to_float_d(srow.get("行金额", 0))
            s_qty   = _to_float_d(srow.get("数量", 0))
            s_price = _to_float_d(srow.get("单价", 0))

            rrow = recv_rows[recv_rows["品名"].apply(_to_str) == item]
            prow = po_df[(po_df["采购单号"].apply(_to_str) == po_no) &
                         (po_df["品名"].apply(_to_str) == item)]

            r_qty = _to_float(rrow["数量"].values[0]) if len(rrow) else None
            p_price, price_src = (_get_po_price(prow.iloc[0]) if len(prow) > 0 else (None, "未找到"))

            errors = []; is_anomaly = False

            if r_qty is None:
                errors.append("收货单未找到品名[" + item + "]"); is_anomaly = True
            elif abs(r_qty - s_qty) > 0.001:
                errors.append(f"数量差异：收货单{r_qty} vs 对账单{s_qty}"); is_anomaly = True

            if p_price is None:
                errors.append("采购单未找到品名[" + item + "]"); is_anomaly = True
            elif abs(p_price - s_price) > 0.001:
                errors.append(f"单价差异（{price_src}）：采购¥{p_price} vs 对账¥{s_price}"); is_anomaly = True

            if is_anomaly:
                anomalies += 1; anomaly_amt += amt; po_all_ok = False

            details.append({
                "po_no": po_no, "item": item,
                "check_type": "、".join(errors) if errors else "通过",
                "recv_qty": r_qty, "stmt_qty": s_qty,
                "po_price": p_price, "po_price_src": price_src,
                "stmt_price": s_price, "line_amt": amt,
                "anomaly": is_anomaly,
                "note": "；".join(errors) if errors else "—"
            })

        # 校验3 — 总额
        if po_all_ok and has_total:
            calc_total = sum(_to_float_d(r.get("行金额",0)) for _, r in stmt_rows.iterrows())
            stmt_total = _to_float(_to_str(stmt_rows["单据总额"].values[0])) if len(stmt_rows) else None
            if stmt_total and abs(calc_total - stmt_total) > 0.01:
                anomalies += 1; anomaly_amt += abs(calc_total - stmt_total)
                details.append({
                    "po_no": po_no, "item": "【总额校验】",
                    "check_type": "总额异常", "recv_qty": None, "stmt_qty": None,
                    "po_price": None, "po_price_src": "—", "stmt_price": None,
                    "line_amt": stmt_total, "anomaly": True,
                    "note": f"明细合计¥{calc_total:.2f} vs 单据总额¥{stmt_total:.2f}，差额¥{calc_total-stmt_total:.2f}"
                })

    os.makedirs(result_dir, exist_ok=True)
    result_path = os.path.join(result_dir, f"{task_id}_result.xlsx")
    _write_excel(result_path, task_name, details, po_df, recv_df, stmt_df)

    return {
        "total": total_rows, "passed": total_rows - anomalies,
        "anomalies": anomalies, "anomaly_amt": round(anomaly_amt, 2),
        "details": details
    }, result_path


def _write_excel(path, task_name, details, po_df, recv_df, stmt_df):
    wb = Workbook()
    ws = wb.active
    ws.title = "对账明细（含批注）"
    headers    = ["采购单号","品名","校验结果","收货单数量","对账单数量",
                  "采购单单价","单价来源","对账单单价","行金额","异常说明","状态"]
    col_widths = [16, 22, 16, 12, 12, 12, 10, 12, 12, 42, 10]
    for ci, (h, w) in enumerate(zip(headers, col_widths), 1):
        c = ws.cell(row=1, column=ci, value=h)
        c.fill = HEAD_FILL; c.font = HEAD_FONT
        c.alignment = Alignment(horizontal="center", vertical="center")
        c.border = THIN_BORDER
        ws.column_dimensions[c.column_letter].width = w
    ws.row_dimensions[1].height = 22

    for ri, d in enumerate(details, 2):
        row_data = [
            d["po_no"], d["item"], d["check_type"],
            d["recv_qty"], d["stmt_qty"],
            d["po_price"], d.get("po_price_src","—"), d["stmt_price"],
            d["line_amt"], d["note"], "⚠ 异常" if d["anomaly"] else "✅ 通过"
        ]
        for ci, val in enumerate(row_data, 1):
            c = ws.cell(row=ri, column=ci, value=val)
            c.border = THIN_BORDER; c.alignment = Alignment(vertical="center")
            c.fill = RED_FILL if d["anomaly"] else GREEN_FILL
            c.font = RED_FONT if d["anomaly"] else GREEN_FONT
        if d["anomaly"] and d["note"] and d["note"] != "—":
            nc = ws.cell(row=ri, column=10)
            cmt = Comment(d["note"], "ReconCore"); cmt.width = 320; cmt.height = 80
            nc.comment = cmt

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:K{len(details)+1}"

    ws2 = wb.create_sheet("汇总报告")
    total = len(details); anom = sum(1 for d in details if d["anomaly"])
    anom_amt = sum(d["line_amt"] or 0 for d in details if d["anomaly"])
    ws2.column_dimensions["A"].width = 16; ws2.column_dimensions["B"].width = 36
    for ri, (k, v) in enumerate([
        ("对账任务", task_name),
        ("生成时间", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        ("单价匹配规则", "含税单价有值优先；否则用单价列"),
        ("关联标识", "采购单号 + 品名"),
        ("",""), ("对账总行数", total), ("通过行数", total-anom),
        ("异常行数", anom), ("异常总金额", f"¥{anom_amt:,.2f}"), ("",""),
        ("对账结论", "✅ 全部通过" if anom==0 else f"⚠️ 存在{anom}项异常，请复查")
    ], 1):
        ka = ws2.cell(row=ri, column=1, value=k); ka.font = Font(bold=True, color="1F4E79")
        va = ws2.cell(row=ri, column=2, value=v)
        if k == "对账结论":
            va.font = Font(bold=True, color="CC0000" if anom>0 else "006400")
            va.fill = RED_FILL if anom>0 else GREEN_FILL

    for sname, df in [("采购订单_原始",po_df),("收货单_原始",recv_df),("对账单_原始",stmt_df)]:
        wsr = wb.create_sheet(sname)
        for ci, col in enumerate(df.columns, 1):
            c = wsr.cell(row=1, column=ci, value=str(col))
            c.fill = HEAD_FILL; c.font = HEAD_FONT
            wsr.column_dimensions[c.column_letter].width = 14
        for ri, row in enumerate(df.itertuples(index=False), 2):
            for ci, val in enumerate(row, 1):
                wsr.cell(row=ri, column=ci, value=val)
    wb.save(path)
