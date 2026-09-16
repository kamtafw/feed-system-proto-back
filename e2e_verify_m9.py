"""
e2e_verify_m9.py — Manual E2E verification for Milestone 9, executed as
real HTTP requests against the actual running app (uvicorn + live
Postgres + live Redis), replicating exactly the steps described in the
M9 implementation plan's "Manual E2E verification" section:

  1.  Hit POST /posts rapidly past the configured limit; confirm the
      over-limit response is 429 with a Retry-After header; wait out the
      window; confirm success resumes.
  2.  Alternate follow/unfollow against the same target rapidly; confirm
      the SHARED follow_action bucket is exhausted by the combination of
      both actions, not by either action alone.
  3.  Confirm a post_create rejection does not affect the ability to
      follow/unfollow in the same window, and vice versa.

Run with short RATE_LIMIT_* windows set via env (see the shell command
that invokes this) so the reset behavior is observable in seconds.
"""

import httpx
import time
import sys

BASE = "http://127.0.0.1:8000"

FAIL = False


def check(label: str, cond: bool, detail: str = "") -> None:
    global FAIL
    status = "✅" if cond else "❌"
    print(f"  {status}  {label}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAIL = True


def main() -> None:
    client = httpx.Client(base_url=BASE, timeout=10)

    print("=" * 60)
    print(" M9 — Manual E2E verification (real HTTP, real Postgres+Redis)")
    print("=" * 60)

    # login as seed user alice
    r = client.post("/auth/login", json={"username": "alice", "password": "password123"})
    check("login as alice succeeds", r.status_code == 200, r.text)
    token = r.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    r2 = client.post("/auth/login", json={"username": "bob", "password": "password123"})
    check("login as bob succeeds", r2.status_code == 200, r2.text)
    bob_token = r2.json()["access_token"]
    bob_headers = {"Authorization": f"Bearer {bob_token}"}

    # ---- [1] POST /posts rate limiting ----
    print("\n[1] POST /posts — threshold, 429, Retry-After, real reset")
    statuses = []
    last_resp = None
    for i in range(4):  # RATE_LIMIT_POST_CREATE_MAX=3 for this run
        resp = client.post("/posts", json={"content": f"e2e test post {i}"}, headers=headers)
        statuses.append(resp.status_code)
        last_resp = resp
    check("first 3 requests succeed, 4th is 429", statuses == [200, 200, 200, 429], str(statuses))
    check("429 response carries a Retry-After header", "retry-after" in last_resp.headers, dict(last_resp.headers))
    if "retry-after" in last_resp.headers:
        retry_after = int(last_resp.headers["retry-after"])
        print(f"      Retry-After: {retry_after}s — waiting {retry_after + 1}s for the real window to elapse...")
        time.sleep(retry_after + 1)
        resumed = client.post("/posts", json={"content": "e2e test post after wait"}, headers=headers)
        check("posting succeeds again after the real window elapses", resumed.status_code == 200, resumed.text)

    # ---- [2] shared follow_action bucket: alternating follow/unfollow ----
    print("\n[2] Shared follow_action bucket — alternating follow/unfollow (as bob -> alice)")
    # bob starts NOT following alice by default (seed graph: alice->bob, alice->carol,
    # bob->alice already exists!). Use bob -> dave instead, which is not seeded.
    target = "dave"
    actions = []
    for i in range(4):  # RATE_LIMIT_FOLLOW_ACTION_MAX=3 for this run
        if i % 2 == 0:
            resp = client.post(f"/me/follow/{target}", headers=bob_headers)
            actions.append(("follow", resp.status_code))
        else:
            resp = client.delete(f"/me/follow/{target}", headers=bob_headers)
            actions.append(("unfollow", resp.status_code))
    codes = [c for _, c in actions]
    check(
        "3 alternating follow/unfollow calls succeed, 4th (shared bucket) is 429",
        codes == [200, 200, 200, 429],
        str(actions),
    )

    # ---- [3] bucket independence ----
    print("\n[3] Bucket independence — post_create exhausted must not block follow_action, and vice versa")
    # alice's post_create bucket is exhausted from [1]; her follow_action bucket
    # has NOT been touched this run, so it should still work.
    r3 = client.post("/me/follow/dave", headers=headers)
    check(
        "alice can still follow (follow_action) despite post_create being exhausted",
        r3.status_code == 200,
        r3.text,
    )
    # bob's follow_action bucket is exhausted from [2]; his post_create bucket
    # has NOT been touched this run, so it should still work.
    r4 = client.post("/posts", json={"content": "bob e2e post"}, headers=bob_headers)
    check(
        "bob can still post (post_create) despite follow_action being exhausted",
        r4.status_code == 200,
        r4.text,
    )

    print("\n" + "=" * 60)
    if FAIL:
        print(" ❌  ONE OR MORE E2E CHECKS FAILED — see above")
        sys.exit(1)
    else:
        print(" ✅  All M9 manual E2E checks passed against the live running app")
    print("=" * 60)


if __name__ == "__main__":
    main()
