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

# Redis Streams (Milestone 2)
# How many events to keep per stream before oldest entries are trimmed.
# The ~ (approximate) flag makes trimming cheaper — Redis trims to the nearest
# internal node boundary rather than exact count, which avoids rewriting entries.
# 10 000 events covers a multi-hour outage at typical write rates for 800-1000 users.
STREAM_MAX_LEN  = int(os.getenv("STREAM_MAX_LEN",  "10000"))

# How long (ms) a message can sit unACKed before XAUTOCLAIM retries it.
# 30 s is generous — fanout for 1000 followers at 1 ms/write takes < 1 s.
# Tune down if you want faster retry; tune up if consumers are legitimately slow.
STREAM_RECLAIM_MS = int(os.getenv("STREAM_RECLAIM_MS", "30000"))