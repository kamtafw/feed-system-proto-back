from dotenv import load_dotenv
import os

load_dotenv()  # reads .env from the current working directory

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost/fanoutfeed")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
TIMELINE_MAX = int(os.getenv("TIMELINE_MAX", "500"))

# True when connecting to an external Postgres that requires SSL (e.g. Supabase)
# local Postgres typically doesn't need this
DB_SSL = os.getenv("DB_SSL", "false").lower() == "true"

# Auth
JWT_SECRET = os.getenv("JWT_SECRET", "change-this-before-any-real-use")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "30"))
