import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

load_dotenv()

CLIENT_ID = (os.getenv("FYERS_CLIENT_ID") or os.getenv("FYERS_APP_ID") or "").strip()
SECRET_KEY = (os.getenv("FYERS_SECRET_KEY") or "").strip()
REDIRECT_URI = (
    os.getenv("FYERS_REDIRECT_URI")
    or "https://trade.fyers.in/api-login/redirect-uri/index.html"
).strip()
TOKEN_FILE = Path(os.getenv("FYERS_ACCESS_TOKEN_FILE", ".access_token"))

if not CLIENT_ID or not SECRET_KEY:
    print("Error: set FYERS_CLIENT_ID (or FYERS_APP_ID) and FYERS_SECRET_KEY in .env")
    sys.exit(1)


def generate_auth_code_url():
    session = fyersModel.SessionModel(
        client_id=CLIENT_ID,
        secret_key=SECRET_KEY,
        redirect_uri=REDIRECT_URI,
        response_type="code",
        grant_type="authorization_code",
        state="codex_fyers_auth",
    )
    return session.generate_authcode(), session


def get_access_token(auth_code, session):
    session.set_token(auth_code)
    response = session.generate_token()
    if response.get("s") == "ok":
        return response.get("access_token")
    print("Token generation failed:", response)
    return None


def _extract_auth_code(redirect_url):
    if "auth_code=" not in redirect_url:
        return None
    return redirect_url.split("auth_code=")[1].split("&")[0].strip()


def main():
    url, session = generate_auth_code_url()

    print("Open this URL in your browser and complete FYERS login:\n")
    print(url)

    redirect_url = input("\nPaste full redirected URL here: ").strip()
    auth_code = _extract_auth_code(redirect_url)
    if not auth_code:
        print("Invalid URL: auth_code not found")
        sys.exit(1)

    token = get_access_token(auth_code, session)
    if not token:
        sys.exit(1)

    TOKEN_FILE.write_text(token, encoding="utf-8")
    print(f"\nAccess token saved: {TOKEN_FILE}")


if __name__ == "__main__":
    main()
