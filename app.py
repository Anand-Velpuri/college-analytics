from fastapi import FastAPI, Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import urllib.parse
from async_lru import alru_cache
from dotenv import load_dotenv
import os
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

app = FastAPI(
    title="University Analytics API (Async & Secured)", 
    version="3.0", # Bumped version for the speed upgrade!
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
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_async_engine(
    DATABASE_URL, 
    echo=False,
    pool_pre_ping=True,        
    pool_recycle=300,          
    connect_args={
        "command_timeout": 60, 
        "server_settings": {
            "tcp_keepalives_idle": "30", 
        }
    }
)



async def fetch_records(query: str, params: dict = None) -> list:
    start_time = time.time()
    
    async with engine.connect() as conn:
        # Measure purely the database execution time
        db_start = time.time()
        result = await conn.execute(text(query), params or {})
        db_end = time.time()
        
        # Measure Python's dictionary mapping time
        records = [dict(row._mapping) for row in result.fetchall()]
        
        total_time = time.time() - start_time
        
        logger.info(f"DB Query Time: {(db_end - db_start):.4f}s | Total Time: {total_time:.4f}s")
        return records

# -----------------------------------------------------------------------------
# 1. STUDENT DASHBOARD ENDPOINTS
# -----------------------------------------------------------------------------

@app.get("/api/analytics/student/{student_id}/attendance", tags=["Student"])
@alru_cache(maxsize=128)
async def get_student_attendance(student_id: str):
    # Let Postgres calculate the percentage natively
    query = """
        SELECT 
            a."subjectId", 
            s.name as subject_name, 
            a."totalClasses", 
            a."attendedClasses",
            ROUND((a."attendedClasses"::numeric / NULLIF(a."totalClasses", 0)) * 100, 1) as attendance_percentage
        FROM academics_v2."Attendance" a
        JOIN academics_v2."Subject" s ON a."subjectId" = s.id
        WHERE a."studentId" = :student_id
    """
    return await fetch_records(query, {"student_id": student_id})

@app.get("/api/analytics/student/{student_id}/grades-trend", tags=["Student"])
async def get_student_grades_trend(student_id: str):
    # Let Postgres do the GROUP BY and AVG()
    query = """
        SELECT "semesterId", ROUND(AVG(grade)::numeric, 2) as sgpa
        FROM academics_v2."Grade"
        WHERE "studentId" = :student_id
        GROUP BY "semesterId"
        ORDER BY "semesterId"
    """
    return await fetch_records(query, {"student_id": student_id})

# -----------------------------------------------------------------------------
# 2. FACULTY DASHBOARD ENDPOINTS
# -----------------------------------------------------------------------------

@app.get("/api/analytics/faculty/{faculty_id}/course-stats", tags=["Faculty"])
async def get_faculty_course_stats(faculty_id: str):
    # Postgres handles the grouping, counting, and averaging
    query = """
        SELECT 
            ba.branch, 
            sub.name as subject_name, 
            ROUND(AVG(g.grade)::numeric, 2) as average_grade,
            COUNT(g.grade) as total_students
        FROM academics_v2."BranchAllocation" ba
        JOIN academics_v2."Subject" sub ON ba."subjectId" = sub.id
        JOIN academics_v2."Grade" g ON sub.id = g."subjectId"
        WHERE ba."facultyId" = :faculty_id AND ba."isApproved" = true
        GROUP BY ba.branch, sub.name
    """
    return await fetch_records(query, {"faculty_id": faculty_id})

# -----------------------------------------------------------------------------
# 3. DEAN DASHBOARD ENDPOINTS
# -----------------------------------------------------------------------------

@app.get("/api/analytics/dean/campus-occupancy", tags=["Dean"])
async def get_campus_occupancy():
    query = """
        SELECT 
            COUNT(CASE WHEN "isPresentInCampus" = true THEN 1 END) as "Inside Campus",
            COUNT(CASE WHEN "isPresentInCampus" = false THEN 1 END) as "Outside Campus"
        FROM user_v2."StudentProfile"
        WHERE "isSuspended" = false
    """
    records = await fetch_records(query)
    return records[0] if records else {"Inside Campus": 0, "Outside Campus": 0}

@app.get("/api/analytics/dean/academic-heatmap", tags=["Dean"])
@alru_cache(maxsize=32)
async def get_academic_heatmap():
    query = """
        SELECT 
            s.branch, 
            sub.name AS subject_name,
            ROUND(AVG(g.grade)::numeric, 2) as average_grade
        FROM academics_v2."Grade" g
        JOIN user_v2."StudentProfile" s ON g."studentId" = s.id
        JOIN academics_v2."Subject" sub ON g."subjectId" = sub.id
        GROUP BY s.branch, sub.name
    """
    return await fetch_records(query)

@app.get("/api/analytics/dean/grievance-trends", tags=["Dean"])
async def get_grievance_trends():
    query = """
        SELECT category, status, COUNT(*) as count 
        FROM cron_v2."Grievance" 
        GROUP BY category, status
    """
    # Grouping by category and status in DB. 
    # If the frontend needs a strict crosstab format, it's trivial to format this list of dicts.
    return await fetch_records(query)

# -----------------------------------------------------------------------------
# 4. WEBMASTER DASHBOARD ENDPOINTS
# -----------------------------------------------------------------------------

@app.get("/api/analytics/webmaster/upload-health", tags=["Webmaster"])
async def get_upload_health():
    # Postgres extracts the date and does the math
    query = """
        SELECT 
            DATE("createdAt") as date, 
            type, 
            SUM("successCount") as successCount, 
            SUM("failCount") as failCount,
            ROUND((SUM("successCount")::numeric / NULLIF(SUM("successCount") + SUM("failCount"), 0)) * 100, 1) as success_rate_percent
        FROM user_v2."UploadHistory"
        GROUP BY DATE("createdAt"), type
        ORDER BY date DESC
    """
    # Convert date objects to strings for JSON serialization
    records = await fetch_records(query)
    for r in records:
        r['date'] = str(r['date'])
    return records

@app.get("/api/analytics/webmaster/system-users", tags=["Webmaster"])
async def get_system_user_distribution():
    query = """
        SELECT 
            role,
            COUNT(CASE WHEN "isDisabled" = false THEN 1 END) as "Active",
            COUNT(CASE WHEN "isDisabled" = true THEN 1 END) as "Disabled"
        FROM auth_v2."AuthCredential"
        GROUP BY role
    """
    return await fetch_records(query)
