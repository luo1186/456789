from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Form, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional
import os, shutil

from database import get_db, engine
import models, schemas, auth, reconcile

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="ReconCore API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = os.environ.get("UPLOAD_DIR", "uploads")
RESULT_DIR = os.environ.get("RESULT_DIR", "results")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)

# ── 静态文件 ──────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def root():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

# ── 认证 ─────────────────────────────────────────────
@app.post("/api/login", response_model=schemas.LoginResponse)
def login(body: schemas.LoginRequest, db: Session = Depends(get_db)):
    user = auth.authenticate(db, body.username, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="用户名或密码错误")
    token = auth.create_token(user.id, user.role)
    return {"token": token, "user": user}

@app.get("/api/me", response_model=schemas.UserOut)
def me(current=Depends(auth.get_current_user)):
    return current

# ── 任务 ─────────────────────────────────────────────
@app.post("/api/tasks", response_model=schemas.TaskOut)
def create_task(
    background_tasks: BackgroundTasks,
    name: str = Form(...),
    po_file: UploadFile = File(...),
    recv_file: UploadFile = File(...),
    stmt_file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current=Depends(auth.get_current_user)
):
    import uuid, datetime
    task_id = str(uuid.uuid4())[:8]
    task_dir = os.path.join(UPLOAD_DIR, task_id)
    os.makedirs(task_dir, exist_ok=True)

    paths = {}
    for key, f in [("po", po_file), ("recv", recv_file), ("stmt", stmt_file)]:
        dest = os.path.join(task_dir, f"{key}_{f.filename}")
        with open(dest, "wb") as out:
            shutil.copyfileobj(f.file, out)
        paths[key] = dest

    import json
    task = models.Task(
        id=task_id,
        user_id=current.id,
        name=name,
        status="queued",
        file_paths=json.dumps(paths),
        created_at=datetime.datetime.utcnow()
    )
    db.add(task)
    db.commit()
    db.refresh(task)

    background_tasks.add_task(reconcile.run, task_id, paths, RESULT_DIR)
    return _task_out(task, current)

@app.get("/api/tasks", response_model=list[schemas.TaskOut])
def list_tasks(db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    if current.role == "admin":
        tasks = db.query(models.Task).order_by(models.Task.created_at.desc()).all()
    else:
        tasks = db.query(models.Task).filter(
            models.Task.user_id == current.id
        ).order_by(models.Task.created_at.desc()).all()
    users = {u.id: u for u in db.query(models.User).all()}
    return [_task_out(t, users.get(t.user_id)) for t in tasks]

@app.get("/api/tasks/{task_id}", response_model=schemas.TaskOut)
def get_task(task_id: str, db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    task = _get_task_or_404(task_id, db, current)
    user = db.query(models.User).filter(models.User.id == task.user_id).first()
    return _task_out(task, user)

@app.delete("/api/tasks/{task_id}")
def cancel_task(task_id: str, db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    task = _get_task_or_404(task_id, db, current)
    if current.role != "admin" and task.user_id != current.id:
        raise HTTPException(403, "无权操作")
    if task.status not in ("queued",):
        raise HTTPException(400, "只能取消排队中的任务")
    db.delete(task)
    db.commit()
    return {"ok": True}

@app.delete("/api/tasks/{task_id}/admin")
def admin_delete_task(task_id: str, db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    """管理员删除任意任务（含结果文件）"""
    if current.role != "admin":
        raise HTTPException(403, "仅管理员可删除任务")
    task = db.query(models.Task).filter(models.Task.id == task_id).first()
    if not task:
        raise HTTPException(404, "任务不存在")
    # 删除结果文件
    if task.result_path and os.path.exists(task.result_path):
        try:
            os.remove(task.result_path)
        except Exception:
            pass
    # 删除上传文件目录
    if task.file_paths:
        import json as _json
        try:
            paths = _json.loads(task.file_paths)
            for p in paths.values():
                if os.path.exists(p):
                    os.remove(p)
            # 删除任务目录
            task_dir = os.path.dirname(list(paths.values())[0])
            if os.path.isdir(task_dir):
                os.rmdir(task_dir)
        except Exception:
            pass
    db.delete(task)
    db.commit()
    return {"ok": True}

@app.get("/api/tasks/{task_id}/download")
def download_result(task_id: str, db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    task = _get_task_or_404(task_id, db, current)
    if task.status != "done" or not task.result_path:
        raise HTTPException(400, "结果文件尚未生成")
    if not os.path.exists(task.result_path):
        raise HTTPException(404, "结果文件不存在")
    fname = f"{task.name}_对账结果.xlsx"
    return FileResponse(task.result_path, filename=fname,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.get("/api/tasks/{task_id}/report")
def get_report(task_id: str, db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    task = _get_task_or_404(task_id, db, current)
    if task.status != "done":
        raise HTTPException(400, "任务尚未完成")
    import json
    return json.loads(task.result_summary or "{}")

# ── 管理员统计 ─────────────────────────────────────────
@app.get("/api/admin/stats")
def admin_stats(db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    if current.role != "admin":
        raise HTTPException(403, "仅管理员可访问")
    import datetime, json
    today = datetime.date.today()
    all_tasks = db.query(models.Task).all()
    today_tasks = [t for t in all_tasks if t.created_at and t.created_at.date() == today]
    done_tasks = [t for t in all_tasks if t.status == "done"]
    anomaly_tasks = []
    total_anomaly_amt = 0
    for t in done_tasks:
        if t.result_summary:
            r = json.loads(t.result_summary)
            if r.get("anomalies", 0) > 0:
                anomaly_tasks.append(t)
                total_anomaly_amt += r.get("anomaly_amt", 0)
    anomaly_rate = round(len(anomaly_tasks) / len(done_tasks) * 100, 1) if done_tasks else 0
    return {
        "today_count": len(today_tasks),
        "total_count": len(all_tasks),
        "anomaly_rate": anomaly_rate,
        "anomaly_amt": total_anomaly_amt,
    }

# ── 用户管理（管理员）─────────────────────────────────
@app.get("/api/users", response_model=list[schemas.UserOut])
def list_users(db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    if current.role != "admin":
        raise HTTPException(403)
    return db.query(models.User).all()

@app.post("/api/users", response_model=schemas.UserOut)
def create_user(body: schemas.CreateUserRequest, db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    if current.role != "admin":
        raise HTTPException(403)
    if db.query(models.User).filter(models.User.username == body.username).first():
        raise HTTPException(400, "用户名已存在")
    user = models.User(
        username=body.username,
        display_name=body.display_name,
        password_hash=auth.hash_password(body.password),
        role=body.role
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

@app.patch("/api/users/{user_id}/password")
def reset_password(user_id: int, body: schemas.ResetPasswordRequest, db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    if current.role != "admin":
        raise HTTPException(403, "仅管理员可重置密码")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(404, "用户不存在")
    user.password_hash = auth.hash_password(body.password)
    db.commit()
    return {"ok": True}

@app.delete("/api/users/{user_id}")
def delete_user(user_id: int, db: Session = Depends(get_db), current=Depends(auth.get_current_user)):
    if current.role != "admin":
        raise HTTPException(403, "仅管理员可删除用户")
    if user_id == current.id:
        raise HTTPException(400, "不能删除自己的账号")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(404, "用户不存在")
    db.delete(user)
    db.commit()
    return {"ok": True}

# ── helpers ───────────────────────────────────────────
def _get_task_or_404(task_id, db, current):
    task = db.query(models.Task).filter(models.Task.id == task_id).first()
    if not task:
        raise HTTPException(404, "任务不存在")
    if current.role != "admin" and task.user_id != current.id:
        raise HTTPException(403, "无权访问")
    return task

def _task_out(task, user):
    import json
    # 提取用户友好的错误信息（取第一行有意义的内容）
    error_msg = None
    if task.error_msg:
        lines = task.error_msg.strip().split('\n')
        friendly = next((l.strip() for l in lines if l.strip()
                        and not l.strip().startswith('File ')
                        and not l.strip().startswith('Traceback')
                        and not l.strip().startswith('During')), None)
        error_msg = task.error_msg  # 完整错误，前端自己处理展示
    return schemas.TaskOut(
        id=task.id,
        name=task.name,
        status=task.status,
        user_id=task.user_id,
        user_name=user.display_name if user else "—",
        created_at=str(task.created_at)[:16] if task.created_at else "",
        finished_at=str(task.finished_at)[:16] if task.finished_at else None,
        result_summary=json.loads(task.result_summary) if task.result_summary else None,
        has_result=bool(task.result_path and os.path.exists(task.result_path)),
        error_msg=error_msg
    )
