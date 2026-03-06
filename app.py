from fastapi import FastAPI, Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import urllib.parse
from async_lru import alru_cache
from dotenv import load_dotenv
import os
from fastapi.middleware.cors import CORSMiddleware

load_dotenv(override=True)

# -----------------------------------------------------------------------------
# SECURITY SETUP
# -----------------------------------------------------------------------------
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY") 
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key != INTERNAL_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail="Invalid or missing API Key"
        )
    return api_key

# Apply the security check globally to ALL endpoints
app = FastAPI(
    title="University Analytics API (Async & Secured)", 
    version="2.0",
    dependencies=[Depends(get_api_key)]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# ASYNC DATABASE CONFIGURATION
# -----------------------------------------------------------------------------
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

encoded_password = urllib.parse.quote_plus(DB_PASS)
# Notice the change here: postgresql+asyncpg://
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# Create an async engine with network-drop protection
engine = create_async_engine(
    DATABASE_URL, 
    echo=False,
    pool_pre_ping=True,       # Pings the DB to ensure the connection is alive before querying
    pool_recycle=300,         # Refreshes the connection every 5 minutes to prevent stale drops
    connect_args={
        "command_timeout": 60, # Gives the query plenty of time to return large datasets
        "server_settings": {
            "tcp_keepalives_idle": "30", # Forces the network to keep the TCP tunnel open
        }
    }
)

# Super-fast async query helper
async def fetch_dataframe(query: str) -> pd.DataFrame:
    """Executes SQL asynchronously and returns a Pandas DataFrame."""
    async with engine.connect() as conn:
        result = await conn.execute(text(query))
        rows = result.fetchall()
        
        if not rows:
            return pd.DataFrame()
            
        # Convert raw rows to DataFrame instantly
        return pd.DataFrame([tuple(row) for row in rows], columns=result.keys())

# -----------------------------------------------------------------------------
# 1. STUDENT DASHBOARD ENDPOINTS
# -----------------------------------------------------------------------------

@app.get("/api/analytics/student/{student_id}/attendance", tags=["Student"])
@alru_cache(maxsize=128) # <-- Cache up to 128 different student IDs!
async def get_student_attendance(student_id: str):
    query = f"""
        SELECT 
            a."subjectId", 
            s.name as subject_name, 
            a."totalClasses", 
            a."attendedClasses"
        FROM academics_v2."Attendance" a
        JOIN academics_v2."Subject" s ON a."subjectId" = s.id
        WHERE a."studentId" = '{student_id}'
    """
    df = await fetch_dataframe(query)
    if df.empty: return []

    df['attendance_percentage'] = ((df['attendedClasses'] / df['totalClasses']) * 100).fillna(0).round(1)
    return df.to_dict(orient="records")

@app.get("/api/analytics/student/{student_id}/grades-trend", tags=["Student"])
async def get_student_grades_trend(student_id: str):
    query = f"""
        SELECT "semesterId", grade
        FROM academics_v2."Grade"
        WHERE "studentId" = '{student_id}'
    """
    df = await fetch_dataframe(query)
    if df.empty: return []

    trend_df = df.groupby('semesterId')['grade'].mean().round(2).reset_index()
    trend_df.rename(columns={'grade': 'sgpa'}, inplace=True)
    return trend_df.to_dict(orient="records")

# -----------------------------------------------------------------------------
# 2. FACULTY DASHBOARD ENDPOINTS
# -----------------------------------------------------------------------------

@app.get("/api/analytics/faculty/{faculty_id}/course-stats", tags=["Faculty"])
async def get_faculty_course_stats(faculty_id: str):
    query = f"""
        SELECT ba.branch, sub.name as subject_name, g.grade
        FROM academicss_v2."BranchAllocation" ba
        JOIN academics_v2."Subject" sub ON ba."subjectId" = sub.id
        JOIN academics_v2."Grade" g ON sub.id = g."subjectId"
        WHERE ba."facultyId" = '{faculty_id}' AND ba."isApproved" = true
    """
    df = await fetch_dataframe(query)
    if df.empty: return []

    stats_df = df.groupby(['branch', 'subject_name']).agg(
        average_grade=('grade', 'mean'),
        total_students=('grade', 'count')
    ).reset_index()
    stats_df['average_grade'] = stats_df['average_grade'].round(2)
    return stats_df.to_dict(orient="records")

# -----------------------------------------------------------------------------
# 3. DEAN DASHBOARD ENDPOINTS
# -----------------------------------------------------------------------------

@app.get("/api/analytics/dean/campus-occupancy", tags=["Dean"])
async def get_campus_occupancy():
    query = """
        SELECT "isPresentInCampus" 
        FROM user_v2."StudentProfile"
        WHERE "isSuspended" = false
    """
    df = await fetch_dataframe(query)
    if df.empty:
        return {"Inside Campus": 0, "Outside Campus": 0}

    counts = df['isPresentInCampus'].value_counts().to_dict()
    return {
        "Inside Campus": int(counts.get(True, 0)),
        "Outside Campus": int(counts.get(False, 0))
    }

@app.get("/api/analytics/dean/academic-heatmap", tags=["Dean"])
@alru_cache(maxsize=32) # <--  Cache the last 32 requests in memory!
async def get_academic_heatmap():
    query = """
        SELECT g.grade, s.branch, sub.name AS subject_name
        FROM academics_v2."Grade" g
        JOIN user_v2."StudentProfile" s ON g."studentId" = s.id
        JOIN academics_v2."Subject" sub ON g."subjectId" = sub.id
    """
    df = await fetch_dataframe(query)
    if df.empty: return []

    heatmap_df = df.groupby(['branch', 'subject_name'])['grade'].mean().round(2).reset_index()
    return heatmap_df.to_dict(orient="records")

@app.get("/api/analytics/dean/grievance-trends", tags=["Dean"])
async def get_grievance_trends():
    query = """SELECT category, status FROM cron_v2."Grievance" """
    df = await fetch_dataframe(query)
    if df.empty: return []

    trend_df = pd.crosstab(df['category'], df['status']).reset_index()
    return trend_df.to_dict(orient="records")

# -----------------------------------------------------------------------------
# 4. WEBMASTER DASHBOARD ENDPOINTS
# -----------------------------------------------------------------------------

@app.get("/api/analytics/webmaster/upload-health", tags=["Webmaster"])
async def get_upload_health():
    query = """SELECT "createdAt", type, "successCount", "failCount" FROM user_v2."UploadHistory" """
    df = await fetch_dataframe(query)
    if df.empty: return []

    df['date'] = pd.to_datetime(df['createdAt']).dt.strftime('%Y-%m-%d')
    health_df = df.groupby(['date', 'type'])[['successCount', 'failCount']].sum().reset_index()
    health_df['total'] = health_df['successCount'] + health_df['failCount']
    health_df['success_rate_percent'] = ((health_df['successCount'] / health_df['total']) * 100).fillna(0).round(1)
    return health_df.to_dict(orient="records")

@app.get("/api/analytics/webmaster/system-users", tags=["Webmaster"])
async def get_system_user_distribution():
    query = """SELECT role, "isDisabled" FROM auth_v2."AuthCredential" """
    df = await fetch_dataframe(query)
    if df.empty: return []

    user_df = pd.crosstab(df['role'], df['isDisabled']).reset_index()
    user_df.rename(columns={False: 'Active', True: 'Disabled'}, inplace=True)
    if 'Disabled' not in user_df.columns: user_df['Disabled'] = 0
    if 'Active' not in user_df.columns: user_df['Active'] = 0
        
    return user_df.to_dict(orient="records")
