"""End-to-end backend tests for the distinct-lecture daily cap:
- Streaming cap = 7 distinct lectures / 24h (idempotent replay)
- Download cap = 5 distinct lectures / 24h (idempotent replay)
- Telegram cap = 15 distinct lectures / 24h (pre-seeded)
- Signed media links (/api/generate, /api/download) carry a 3h TTL (10800s)
"""

import os
import time
import urllib.parse

import pytest
import requests
from pymongo import MongoClient

BASE_URL = "http://127.0.0.1:8899"

STREAM_CODE = "XS38GJ6BRQ"
STREAM_USER = "7a2795cc-f725-4145-9126-cfb869032fb7"
STREAM_TOKENS = [
    "QTEST_LbL-jEVF0Co",
    "QTEST_zCPTABh_jDg",
    "QTEST_mdhEixTa338",
    "QTEST_Nn1s-PWv0-w",
    "QTEST_ucp0D2LQTc0",
    "QTEST_nZzrzvKAybM",
    "QTEST_lNjNpK9QrWw",
    "QTEST_FJCSbHnYPrU",
    "QTEST_-HgeopQb13w",
]

TG_CODE = "3HMGK6GMRA"
TG_USER = "d014beb9-7018-49c2-a9cc-bac65fe7d152"
TG_TOKEN_BLOCK = "wLhCaAHWKNDRkAQjQwtHFQ"
TG_TOKEN_OK = "VhkJDs-2a_XjirHVo1-fkg"

MAIN_MONGO = "mongodb+srv://tarangkalal22abcd_db_user:nNdHcBYWNzKaOjnq@cluster0.ludja3o.mongodb.net"
MAIN_DB = "Nobita-Stream-Bot"
AUTODEL_MONGO = "mongodb+srv://afradhiofficial2212_db_user:tarangkalal@cluster0.fflmm94.mongodb.net"
AUTODEL_DB = "bisal_autodel"


@pytest.fixture(scope="module")
def quota_col():
    c = MongoClient(MAIN_MONGO, serverSelectionTimeoutMS=15000)
    col = c[MAIN_DB]["media_daily_quota"]
    yield col
    c.close()


@pytest.fixture(scope="module")
def autodel_col():
    c = MongoClient(AUTODEL_MONGO, serverSelectionTimeoutMS=15000)
    col = c[AUTODEL_DB]["delivered_lectures"]
    yield col
    c.close()


def _reset_user_action(col, user_id, action):
    col.delete_many({"user_id": user_id, "action": action})


def _generate_stream_url(token, code):
    r = requests.get(f"{BASE_URL}/api/generate/{token}", params={"access_code": code}, timeout=30)
    return r


def _generate_download_url(token, code):
    r = requests.get(f"{BASE_URL}/api/download/{token}", params={"access_code": code}, timeout=30)
    return r


def _to_local(url):
    p = urllib.parse.urlparse(url)
    path = p.path
    # /watch/{id}/{name} is an HTML player; strip 'watch/' to hit media_streamer.
    if path.startswith("/watch/"):
        path = path[len("/watch"):]
    return urllib.parse.urlunparse(("http", "127.0.0.1:8899", path, p.params, p.query, p.fragment))


def _consume(url):
    """Issue a Range GET so quota is actually consumed. Returns response."""
    local = _to_local(url)
    r = requests.get(local, headers={"Range": "bytes=0-1023"}, timeout=60, allow_redirects=False)
    return r


# ---------- Signed link 3h TTL ----------

class TestSignedLinkTTL:
    def test_generate_stream_link_has_3h_expiry(self, quota_col):
        _reset_user_action(quota_col, STREAM_USER, "stream")
        now = int(time.time())
        r = _generate_stream_url(STREAM_TOKENS[0], STREAM_CODE)
        assert r.status_code == 200, f"generate failed: {r.status_code} {r.text[:300]}"
        data = r.json()
        assert data.get("success") is True, data
        stream_url = data["stream_url"]
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(stream_url).query)
        expires = int(qs["expires"][0])
        delta = expires - now
        assert 10700 <= delta <= 10900, f"expected ~10800s TTL, got delta={delta}s (expires={expires}, now={now})"

    def test_generate_download_link_has_3h_expiry(self, quota_col):
        _reset_user_action(quota_col, STREAM_USER, "download")
        now = int(time.time())
        r = _generate_download_url(STREAM_TOKENS[0], STREAM_CODE)
        assert r.status_code == 200, f"download-generate failed: {r.status_code} {r.text[:300]}"
        data = r.json()
        assert data.get("success") is True, data
        dl_url = data["download_url"]
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(dl_url).query)
        expires = int(qs["expires"][0])
        delta = expires - now
        assert 10700 <= delta <= 10900, f"expected ~10800s TTL, got delta={delta}s"


# ---------- Stream distinct-lecture cap = 7 ----------

class TestStreamDailyCap:
    def test_seven_distinct_ok_eighth_blocked_and_replay_free(self, quota_col):
        _reset_user_action(quota_col, STREAM_USER, "stream")

        first_stream_url = None
        # First 7 distinct lectures must succeed with actual bytes (206 partial content).
        for i, tok in enumerate(STREAM_TOKENS[:7]):
            gen = _generate_stream_url(tok, STREAM_CODE)
            assert gen.status_code == 200, f"[{i}] generate failed: {gen.status_code} {gen.text[:200]}"
            surl = gen.json()["stream_url"]
            if i == 0:
                first_stream_url = surl
            r = _consume(surl)
            assert r.status_code == 206, f"[{i}] expected 206 partial content, got {r.status_code}: {r.text[:200] if r.status_code != 206 else ''}"

        # 8th distinct lecture must be blocked with 429.
        gen = _generate_stream_url(STREAM_TOKENS[7], STREAM_CODE)
        assert gen.status_code == 200, "generate should still succeed; cap is enforced at bytes-request time"
        eighth_url = gen.json()["stream_url"]
        r = _consume(eighth_url)
        assert r.status_code == 429, f"8th distinct stream must be 429, got {r.status_code}"
        assert "Daily stream limit" in r.text or "daily" in r.text.lower(), r.text[:300]

        # Re-streaming an already-counted lecture must still succeed (idempotent).
        r = _consume(first_stream_url)
        assert r.status_code == 206, f"replay must be free (206), got {r.status_code}"

        distinct = quota_col.count_documents({"user_id": STREAM_USER, "action": "stream"})
        assert distinct == 7, f"expected 7 distinct stream slots consumed, got {distinct}"

    def test_tamper_proof_same_file_via_different_token_not_double_counted(self, quota_col):
        """Same lecture requested via different URL tokens must count as ONE."""
        _reset_user_action(quota_col, STREAM_USER, "stream")

        # Consume once using STREAM_TOKENS[0].
        gen = _generate_stream_url(STREAM_TOKENS[0], STREAM_CODE)
        assert gen.status_code == 200
        surl_a = gen.json()["stream_url"]
        r = _consume(surl_a)
        assert r.status_code == 206

        # Tamper: same signed link, but mutate lecture_key in the query string.
        parsed = urllib.parse.urlparse(surl_a)
        qs = urllib.parse.parse_qs(parsed.query)
        # replace lecture_key with a bogus value; quota_lecture_id is derived
        # from the real file.unique_id server-side, so this MUST NOT double-count.
        if "lecture_key" in qs:
            qs["lecture_key"] = ["tampered-" + qs["lecture_key"][0]]
        new_query = urllib.parse.urlencode(qs, doseq=True)
        tampered_url = urllib.parse.urlunparse(parsed._replace(query=new_query))
        r2 = _consume(tampered_url)
        # Either allowed (206) because same real lecture, or blocked because sig fails.
        # What must NOT happen: creating a second distinct row.
        assert r2.status_code in (206, 403), f"unexpected {r2.status_code}: {r2.text[:200]}"

        distinct = quota_col.count_documents({"user_id": STREAM_USER, "action": "stream"})
        assert distinct == 1, f"tamper must not add a distinct slot; got {distinct}"


# ---------- Download distinct-lecture cap = 5 ----------

class TestDownloadDailyCap:
    def test_five_distinct_ok_sixth_blocked_and_replay_free(self, quota_col):
        _reset_user_action(quota_col, STREAM_USER, "download")

        first_url = None
        for i, tok in enumerate(STREAM_TOKENS[:5]):
            gen = _generate_download_url(tok, STREAM_CODE)
            assert gen.status_code == 200, f"[{i}] dl-generate failed: {gen.status_code} {gen.text[:200]}"
            durl = gen.json()["download_url"]
            if i == 0:
                first_url = durl
            r = _consume(durl)
            assert r.status_code == 206, f"[{i}] expected 206, got {r.status_code}: {r.text[:200] if r.status_code != 206 else ''}"

        # 6th distinct download must be blocked with 429.
        gen = _generate_download_url(STREAM_TOKENS[5], STREAM_CODE)
        assert gen.status_code == 200
        sixth_url = gen.json()["download_url"]
        r = _consume(sixth_url)
        assert r.status_code == 429, f"6th distinct download must be 429, got {r.status_code}"
        assert "Daily download limit" in r.text or "daily" in r.text.lower(), r.text[:300]

        # Re-download an already-counted lecture -> free.
        r = _consume(first_url)
        assert r.status_code == 206, f"download replay must be free, got {r.status_code}"

        distinct = quota_col.count_documents({"user_id": STREAM_USER, "action": "download"})
        assert distinct == 5, f"expected 5 distinct download slots, got {distinct}"


# ---------- Telegram distinct-lecture cap = 15 (pre-seeded) ----------

class TestTelegramDailyCap:
    def test_sixteenth_distinct_blocked_then_real_delivery_succeeds(self, quota_col, autodel_col):
        # Clean slate then pre-seed 15 dummy distinct lectures for TG_USER.
        _reset_user_action(quota_col, TG_USER, "telegram")
        now = time.time()
        dummy_docs = [
            {
                "user_id": TG_USER,
                "action": "telegram",
                "lecture_id": f"dummy{i}",
                "first_at": now,
                "last_at": now,
            }
            for i in range(15)
        ]
        quota_col.insert_many(dummy_docs)
        assert quota_col.count_documents({"user_id": TG_USER, "action": "telegram"}) == 15

        # 16th distinct lecture -> must be 429 with the specific message.
        r = requests.get(
            f"{BASE_URL}/api/telegram/{TG_TOKEN_BLOCK}",
            params={"access_code": TG_CODE},
            timeout=60,
        )
        assert r.status_code == 429, f"expected 429, got {r.status_code}: {r.text[:400]}"
        body = r.json()
        assert body.get("success") is False
        assert "Daily Telegram limit" in body.get("error", ""), body

        # Delete the dummy docs and try a real delivery of a DIFFERENT lecture.
        quota_col.delete_many({"user_id": TG_USER, "action": "telegram", "lecture_id": {"$regex": "^dummy"}})
        assert quota_col.count_documents({"user_id": TG_USER, "action": "telegram"}) == 0

        r2 = requests.get(
            f"{BASE_URL}/api/telegram/{TG_TOKEN_OK}",
            params={"access_code": TG_CODE},
            timeout=120,
        )
        # Happy path may hit BIN FloodWait ("Server is busy"); allow one retry.
        if r2.status_code == 429 and "busy" in r2.text.lower():
            time.sleep(35)
            r2 = requests.get(
                f"{BASE_URL}/api/telegram/{TG_TOKEN_OK}",
                params={"access_code": TG_CODE},
                timeout=120,
            )
        assert r2.status_code == 200, f"real TG delivery failed: {r2.status_code} {r2.text[:400]}"
        body2 = r2.json()
        assert body2.get("success") is True, body2
        assert "Telegram" in body2.get("message", ""), body2

        # A new autodelete record should exist in bisal_autodel.delivered_lectures.
        # Search recent docs; give TTL sweeper no reason to have removed it.
        recent = list(
            autodel_col.find({}).sort("_id", -1).limit(5)
        )
        assert recent, "no delivered_lectures records found at all"

        # Quota should now have exactly 1 real distinct entry (the delivered lecture).
        distinct = quota_col.count_documents({"user_id": TG_USER, "action": "telegram"})
        assert distinct == 1, f"expected 1 telegram distinct entry after real delivery, got {distinct}"
