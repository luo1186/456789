from pydantic import BaseModel
from typing import Optional, Any

class LoginRequest(BaseModel):
    username: str
    password: str

class UserOut(BaseModel):
    id: int
    username: str
    display_name: str
    role: str
    class Config:
        from_attributes = True

class LoginResponse(BaseModel):
    token: str
    user: UserOut

class TaskOut(BaseModel):
    id: str
    name: str
    status: str
    user_id: int
    user_name: str
    created_at: str
    finished_at: Optional[str]
    result_summary: Optional[Any]
    has_result: bool
    error_msg: Optional[str]

class CreateUserRequest(BaseModel):
    username: str
    display_name: str
    password: str
    role: str = "staff"
