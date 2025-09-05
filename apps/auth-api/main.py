import os, re, uuid, hashlib, secrets, datetime as dt
import asyncpg, jwt
from typing import Optional, Literal
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from passlib.hash import bcrypt
from pydantic import BaseModel, EmailStr

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/market")
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
ACCESS_MIN = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))
DEFAULT_RESET_PASSWORD = os.getenv("DEFAULT_RESET_PASSWORD", "Changeme123@")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

ALGO = "HS256"

# Password policy: ≥8 ký tự, có chữ thường, HOA, số, ký tự đặc biệt
PASSWORD_REGEX = re.compile(r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,}$')


app = FastAPI(title="Auth API")

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"http://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

pool: Optional[asyncpg.Pool] = None

def now_utc():
    return dt.datetime.now(dt.timezone.utc)

def create_access_token(sub: str, role: str):
    exp = now_utc() + dt.timedelta(minutes=ACCESS_MIN)
    return jwt.encode({"sub": sub, "role": role, "exp": exp}, JWT_SECRET, algorithm=ALGO)

def hash_refresh_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()

async def get_user_by_email(conn, email: str):
    return await conn.fetchrow("SELECT * FROM users WHERE email=$1", email)

async def get_user_by_id(conn, uid):
    return await conn.fetchrow("SELECT * FROM users WHERE id=$1", uid)

async def ensure_admin_seed():
    if not ADMIN_EMAIL or not ADMIN_PASSWORD:
        return
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM users WHERE role='ADMIN' LIMIT 1")
        if row: return
        pw_hash = bcrypt.hash(ADMIN_PASSWORD)
        await conn.execute("""
            INSERT INTO users (email, password_hash, display_name, role, status)
            VALUES ($1,$2,$3,'ADMIN','ACTIVE')
            ON CONFLICT (email) DO NOTHING
        """, ADMIN_EMAIL.lower(), pw_hash, "Admin")

class RegisterIn(BaseModel):
    email: EmailStr
    password: str
    display_name: Optional[str] = None

class LoginIn(BaseModel):
    email: EmailStr
    password: str

class ChangePwIn(BaseModel):
    old_password: str
    new_password: str

class SetRoleIn(BaseModel):
    role: Literal["USER","ADMIN"]

class SetStatusIn(BaseModel):
    status: Literal["ACTIVE","DISABLED"]

class ResetByAdminOut(BaseModel):
    temp_password: str

def require_auth(roles: Optional[list[str]]=None):
    async def dep(authorization: Optional[str] = Header(None)):
        if not authorization or not authorization.lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="Missing bearer token")
        token = authorization.split(" ",1)[1].strip()
        try:
            data = jwt.decode(token, JWT_SECRET, algorithms=[ALGO])
            if roles and data.get("role") not in roles:
                raise HTTPException(status_code=403, detail="Forbidden")
            return data
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")
    return dep

def validate_password_strength(pw: str):
    """
    Ném 400 nếu mật khẩu không đạt yêu cầu.
    - ít nhất 8 ký tự
    - có chữ thường, chữ HOA, số, ký tự đặc biệt
    """
    if not PASSWORD_REGEX.match(pw or ""):
        raise HTTPException(
            status_code=400,
            detail="Password must be ≥ 8 chars and include lowercase, uppercase, number, and special character."
        )
    
@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await ensure_admin_seed()

@app.on_event("shutdown")
async def shutdown():
    await pool.close()

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/auth/register")
async def register(body: RegisterIn):
    validate_password_strength(body.password)
    async with pool.acquire() as conn:
        if await get_user_by_email(conn, body.email.lower()):
            raise HTTPException(409, "Email already registered")
        pw_hash = bcrypt.hash(body.password)
        row = await conn.fetchrow("""
          INSERT INTO users (email,password_hash,display_name)
          VALUES ($1,$2,$3)
          RETURNING id, email, display_name, role, status, created_at
        """, body.email.lower(), pw_hash, body.display_name)
    return dict(row)

@app.post("/auth/login")
async def login(body: LoginIn, request: Request):
    async with pool.acquire() as conn:
        u = await get_user_by_email(conn, body.email.lower())
        if not u or not bcrypt.verify(body.password, u["password_hash"]):
            raise HTTPException(401, "Invalid credentials")
        if u["status"] != "ACTIVE":
            raise HTTPException(403, "Account is disabled")
        await conn.execute("UPDATE users SET last_login=NOW() WHERE id=$1", u["id"])
        access = create_access_token(str(u["id"]), u["role"])
        refresh_plain = secrets.token_urlsafe(48)
        refresh_hash = hash_refresh_token(refresh_plain)
        expires_at = now_utc() + dt.timedelta(days=REFRESH_DAYS)
        ua = request.headers.get("user-agent")
        ip = request.client.host if request.client else None
        await conn.execute("""
          INSERT INTO refresh_tokens (user_id, token_hash, expires_at, user_agent, ip)
          VALUES ($1,$2,$3,$4,$5)
        """, u["id"], refresh_hash, expires_at, ua, ip)
    return {
        "access_token": access,
        "refresh_token": refresh_plain,
        "token_type": "bearer",
        "user": {"id": str(u["id"]), "email": u["email"], "display_name": u["display_name"], "role": u["role"]}
    }

class RefreshIn(BaseModel):
    refresh_token: str

@app.post("/auth/refresh")
async def refresh_token(body: RefreshIn):
    r_hash = hash_refresh_token(body.refresh_token)
    async with pool.acquire() as conn:
        r = await conn.fetchrow("""
          SELECT rt.*, u.role FROM refresh_tokens rt
          JOIN users u ON u.id=rt.user_id
          WHERE rt.token_hash=$1 AND rt.revoked=FALSE
        """, r_hash)
        if not r:
            raise HTTPException(401, "Invalid refresh token")
        if r["expires_at"] < now_utc():
            raise HTTPException(401, "Refresh token expired")
        access = create_access_token(str(r["user_id"]), r["role"])
    return {"access_token": access, "token_type": "bearer"}

@app.post("/auth/logout")
async def logout(body: RefreshIn, user=Depends(require_auth())):
    r_hash = hash_refresh_token(body.refresh_token)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE refresh_tokens SET revoked=TRUE WHERE token_hash=$1", r_hash)
    return {"ok": True}

@app.get("/auth/me")
async def me(user=Depends(require_auth())):
    uid = uuid.UUID(user["sub"])
    async with pool.acquire() as conn:
        u = await get_user_by_id(conn, uid)
        if not u: raise HTTPException(404, "User not found")
    return {"id": str(u["id"]), "email": u["email"], "display_name": u["display_name"], "role": u["role"], "status": u["status"]}

@app.post("/auth/change-password")
async def change_password(body: ChangePwIn, user=Depends(require_auth())):
    validate_password_strength(body.password)
    uid = uuid.UUID(user["sub"])
    async with pool.acquire() as conn:
        u = await get_user_by_id(conn, uid)
        if not u or not bcrypt.verify(body.old_password, u["password_hash"]):
            raise HTTPException(400, "Old password incorrect")
        new_hash = bcrypt.hash(body.new_password)
        await conn.execute("UPDATE users SET password_hash=$1 WHERE id=$2", new_hash, uid)
    return {"ok": True}

# --------- ADMIN ---------
@app.get("/admin/users")
async def list_users(page: int = 1, page_size: int = 20, _:dict=Depends(require_auth(["ADMIN"]))):
    page = max(1, page); page_size = max(1, min(page_size, 100))
    offset = (page-1)*page_size
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
          SELECT id, email, display_name, role, status, last_login, created_at
          FROM users ORDER BY created_at DESC
          LIMIT $1 OFFSET $2
        """, page_size, offset)
    return [dict(r) for r in rows]

@app.post("/admin/users/{user_id}/role")
async def set_role(user_id: uuid.UUID, body: SetRoleIn, _:dict=Depends(require_auth(["ADMIN"]))):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET role=$1 WHERE id=$2", body.role, user_id)
    return {"ok": True}

@app.post("/admin/users/{user_id}/status")
async def set_status(user_id: uuid.UUID, body: SetStatusIn, _:dict=Depends(require_auth(["ADMIN"]))):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET status=$1 WHERE id=$2", body.status, user_id)
    return {"ok": True}

@app.post("/admin/users/{user_id}/reset-password", response_model=ResetByAdminOut)
async def reset_password_admin(user_id: uuid.UUID, _:dict=Depends(require_auth(["ADMIN"]))):
    # Đặt về DEFAULT_RESET_PASSWORD (hoặc tạo random nếu muốn)
    temp = DEFAULT_RESET_PASSWORD
    h = bcrypt.hash(temp)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET password_hash=$1 WHERE id=$2", h, user_id)
    return {"temp_password": temp}
