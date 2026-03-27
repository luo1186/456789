"""
初始化数据库并创建默认账号
运行方式：python seed.py
"""
from database import engine, SessionLocal
import models, auth

models.Base.metadata.create_all(bind=engine)

db = SessionLocal()

DEFAULTS = [
    {"username": "admin",    "display_name": "系统管理员", "password": "Admin@2024", "role": "admin"},
    {"username": "zhangsan", "display_name": "张三",       "password": "Staff@2024", "role": "staff"},
    {"username": "lisi",     "display_name": "李四",       "password": "Staff@2024", "role": "staff"},
]

for u in DEFAULTS:
    exists = db.query(models.User).filter(models.User.username == u["username"]).first()
    if not exists:
        user = models.User(
            username=u["username"],
            display_name=u["display_name"],
            password_hash=auth.hash_password(u["password"]),
            role=u["role"]
        )
        db.add(user)
        print(f"✅ 创建用户：{u['username']} ({u['role']})")
    else:
        print(f"⏭  用户已存在：{u['username']}")

db.commit()
db.close()
print("\n数据库初始化完成！")
print("默认账号：")
print("  管理员  admin    / Admin@2024")
print("  专员    zhangsan / Staff@2024")
print("  专员    lisi     / Staff@2024")
