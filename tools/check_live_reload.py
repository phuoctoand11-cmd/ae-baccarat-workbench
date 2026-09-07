from __future__ import annotations

import argparse
import time
from typing import Any

from playwright.sync_api import sync_playwright


LIVE_TOKENS = ("live-dealer", "ae-live", "sexy casino", "sx~~lobby~baccarat", "sfcdf.")


def _live_pages(browser: Any) -> list[Any]:
    pages: list[Any] = []
    for context in browser.contexts:
        for page in context.pages:
            text = f"{page.url or ''} {_safe_title(page)} {' '.join(_safe_frame_urls(page))}".lower()
            if any(token in text for token in LIVE_TOKENS):
                pages.append(page)
    return pages


def _safe_title(page: Any) -> str:
    try:
        return str(page.title())
    except Exception:
        return ""


def _safe_frame_urls(page: Any) -> list[str]:
    try:
        return [str(frame.url or "") for frame in page.frames]
    except Exception:
        return []


def _page_time_origin(page: Any) -> int | str:
    try:
        return int(page.evaluate("Math.round(performance.timeOrigin)"))
    except Exception as exc:
        return f"ERR:{exc}"


def _snapshot(browser: Any) -> list[tuple[int, str, int | str]]:
    return [(index, page.url, _page_time_origin(page)) for index, page in enumerate(_live_pages(browser))]


def _reload_observed(before: list[tuple[int, str, int | str]], after: list[tuple[int, str, int | str]]) -> bool:
    before_by_url = {url: time_origin for _, url, time_origin in before}
    for _, url, time_origin in after:
        before_time_origin = before_by_url.get(url)
        if isinstance(time_origin, int) and isinstance(before_time_origin, int) and time_origin != before_time_origin:
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Passively check whether live pages reload through CDP.")
    parser.add_argument("--cdp-url", default="http://localhost:9222")
    parser.add_argument("--seconds", type=int, default=65)
    args = parser.parse_args()

    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(args.cdp_url)
        before = _snapshot(browser)
        print(f"live_pages_before={len(before)}")
        print(f"before={before}")
        time.sleep(max(0, args.seconds))
        after = _snapshot(browser)
        print(f"live_pages_after={len(after)}")
        print(f"after={after}")
        print(f"reload_observed={_reload_observed(before, after)}")
        browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
