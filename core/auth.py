import base64
import json
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv

from core.fyers_utils import get_fyers_client, to_ws_access_token

logger = logging.getLogger(__name__)


class AuthException(Exception):
    pass


class FyersAuth:
    def __init__(self):
        load_dotenv()

        self.client_id = (
            os.getenv("FYERS_CLIENT_ID")
            or os.getenv("FYERS_APP_ID")
            or ""
        ).strip()
        self.secret_key = (os.getenv("FYERS_SECRET_KEY") or "").strip()
        self.redirect_uri = (
            os.getenv("FYERS_REDIRECT_URI")
            or "https://trade.fyers.in/api-login/redirect-uri/index.html"
        ).strip()
        self.session_file = Path(os.getenv("FYERS_SESSION_FILE", "session.json"))
        self.access_token_file = Path(
            os.getenv("FYERS_ACCESS_TOKEN_FILE", ".access_token")
        )

        if not self.client_id:
            raise AuthException(
                "FYERS client id missing. Set FYERS_CLIENT_ID (or FYERS_APP_ID) in .env."
            )

        self.session_data = self._load_session()
        if not self.session_data:
            self.refresh()

    def _decode_jwt_payload(self, token: str) -> dict | None:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        try:
            payload_part = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = base64.urlsafe_b64decode(payload_part.encode("utf-8")).decode("utf-8")
            parsed = json.loads(payload)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    def _extract_expiry(self, token: str) -> float | None:
        payload = self._decode_jwt_payload(token)
        if not payload:
            return None
        exp = payload.get("exp")
        if isinstance(exp, (int, float)):
            return float(exp)
        return None

    def _is_expired(self, token: str, leeway_seconds: int = 120) -> bool:
        expiry = self._extract_expiry(token)
        if expiry is None:
            return False
        return time.time() >= (expiry - leeway_seconds)

    def _load_session(self):
        if not self.session_file.exists():
            return None
        try:
            data = json.loads(self.session_file.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return None
            if str(data.get("broker", "")).upper() != "FYERS":
                return None
            token = str(data.get("access_token") or "").strip()
            if not token:
                return None
            if self._is_expired(token):
                logger.info("Cached FYERS session token expired.")
                return None
            data.setdefault("client_id", self.client_id)
            data.setdefault("api_token", to_ws_access_token(self.client_id, token))
            return data
        except Exception as exc:
            logger.warning("Unable to read cached FYERS session: %s", exc)
            return None

    def _save_session(self, session):
        expiry = self._extract_expiry(session.get("access_token", ""))
        payload = dict(session)
        payload["broker"] = "FYERS"
        if expiry:
            payload["expiry"] = expiry
        elif "expiry" not in payload:
            payload["expiry"] = time.time() + 12 * 3600

        self.session_file.write_text(json.dumps(payload), encoding="utf-8")

    def _read_access_token(self) -> str:
        token_env = str(os.getenv("FYERS_ACCESS_TOKEN", "")).strip()
        if token_env:
            return token_env

        if self.access_token_file.exists():
            token = self.access_token_file.read_text(encoding="utf-8").strip()
            if token:
                return token

        raise AuthException(
            "FYERS access token missing. Run `python fyers_auth.py` and ensure `.access_token` exists."
        )

    def _build_session(self, access_token: str) -> dict:
        if self._is_expired(access_token):
            raise AuthException(
                "FYERS access token is expired. Run `python fyers_auth.py` to generate a fresh token."
            )

        return {
            "broker": "FYERS",
            "client_id": self.client_id,
            "access_token": access_token,
            "api_token": to_ws_access_token(self.client_id, access_token),
            "session_token": access_token,
            "session_sid": "",
            "baseUrl": "https://api.fyers.in",
            "dataCenter": "FYERS",
            "token_file": str(self.access_token_file),
        }

    def refresh(self):
        access_token = self._read_access_token()
        self.session_data = self._build_session(access_token)
        self._save_session(self.session_data)
        logger.info("FYERS session refreshed and cached.")

    def get_session(self):
        if not self.session_data:
            self.refresh()
        elif self._is_expired(self.session_data.get("access_token", "")):
            self.refresh()
        return self.session_data

    def get_headers(self):
        session = self.get_session()
        token = session.get("access_token", "")
        return {
            "Authorization": to_ws_access_token(self.client_id, token),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def get_fyers_client(self):
        session = self.get_session()
        return get_fyers_client(session["client_id"], session["access_token"])


# Backward-compatible alias so existing imports keep working.
KotakNeoAuth = FyersAuth
