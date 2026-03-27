from sqlalchemy import Column, String, Integer, DateTime, Text, ForeignKey
from database import Base

class User(Base):
    __tablename__ = "users"
    id           = Column(Integer, primary_key=True, index=True)
    username     = Column(String(64), unique=True, index=True, nullable=False)
    display_name = Column(String(64), nullable=False)
    password_hash= Column(String(256), nullable=False)
    role         = Column(String(16), default="staff")   # staff | admin

class Task(Base):
    __tablename__ = "tasks"
    id             = Column(String(16), primary_key=True)
    user_id        = Column(Integer, ForeignKey("users.id"), nullable=False)
    name           = Column(String(128), nullable=False)
    status         = Column(String(16), default="queued")  # queued|processing|done|failed
    file_paths     = Column(Text)
    result_path    = Column(String(512))
    result_summary = Column(Text)
    created_at     = Column(DateTime)
    finished_at    = Column(DateTime)
    error_msg      = Column(Text)
