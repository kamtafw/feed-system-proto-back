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
STREAM_MAX_LEN = int(os.getenv("STREAM_MAX_LEN", "10000"))
STREAM_RECLAIM_MS = int(os.getenv("STREAM_RECLAIM_MS", "30000"))

# Post cache (Milestone 5)
POST_CACHE_TTL_SECONDS = int(os.getenv("POST_CACHE_TTL_SECONDS", "86400"))

# Hybrid fanout (Milestone 7)
HEAVY_FANOUT_THRESHOLD = int(os.getenv("HEAVY_FANOUT_THRESHOLD", "4"))

# Rate limiting (Milestone 9)
#
# Sliding-window-log limits, one (max, window) pair per action. These are
# development/product-policy defaults, not claims about a researched or
# objectively correct limit — see docs/milestone-9-rate-limiting.md.
#
# post_create: gates POST /posts. This is the most expensive write in the
# system (Postgres write + cache warm + fanout + WS pushes + event bus
# publish) and the literal abuse case named in architecture-review.md.
RATE_LIMIT_POST_CREATE_MAX = int(os.getenv("RATE_LIMIT_POST_CREATE_MAX", "5"))
RATE_LIMIT_POST_CREATE_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_POST_CREATE_WINDOW_SECONDS", "60"))

# follow_action: a SINGLE shared bucket for both POST /me/follow/{id} and
# DELETE /me/follow/{id}. The policy being enforced is "rate of follow-graph
# mutation," not independent follow/unfollow counts — see M9 ADR-4.
RATE_LIMIT_FOLLOW_ACTION_MAX = int(os.getenv("RATE_LIMIT_FOLLOW_ACTION_MAX", "20"))
RATE_LIMIT_FOLLOW_ACTION_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_FOLLOW_ACTION_WINDOW_SECONDS", "60"))
