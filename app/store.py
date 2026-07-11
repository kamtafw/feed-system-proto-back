from dataclasses import dataclass
from typing import Dict, List, Set
import time


@dataclass
class User:
    id: str
    name: str


@dataclass
class Post:
    id: str
    author_id: str
    author_name: str
    content: str
    created_at: float


USERS: Dict[str, User] = {
    "alice": User(id="alice", name="Alice"),
    "bob": User(id="bob", name="Bob"),
    "carol": User(id="carol", name="Carol"),
    "dave": User(id="dave", name="Dave"),
}


# follows[uid]   = set of user IDs that `uid` follows
# followers[uid] = set of user IDs that follow `uid`
FOLLOWS: Dict[str, Set[str]] = {
    "alice": {"bob", "carol"},
    "bob": {"alice"},
    "carol": {"alice", "bob"},
    "dave": set(),
}

FOLLOWERS: Dict[str, Set[str]] = {
    "alice": {"bob", "carol"},
    "bob": {"alice", "carol"},
    "carol": {"alice"},
    "dave": set(),
}

POSTS: Dict[str, Post] = {}

# timelines[uid] = list of post IDs, newest first
TIMELINES: Dict[str, List[str]] = {uid: [] for uid in USERS}
