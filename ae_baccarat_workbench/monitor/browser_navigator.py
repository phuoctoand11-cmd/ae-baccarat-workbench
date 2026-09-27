from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from typing import Any, Callable

logger = logging.getLogger(__name__)

StatusCallback = Callable[[str], None]

# Regex patterns for matching casino menu and AE Sexy lobby
CASINO_MENU_PATTERNS = [
    re.compile(r"live\s*casino", re.IGNORECASE),
    re.compile(r"casino\s*trực\s*tuyến", re.IGNORECASE),
    re.compile(r"sòng\s*bài(\s*trực\s*tuyến)?", re.IGNORECASE),
    re.compile(r"casino", re.IGNORECASE),
]

AE_SEXY_PATTERNS = [
    re.compile(r"ae\s*sexy(\s*baccarat)?", re.IGNORECASE),
    re.compile(r"sexy\s*casino", re.IGNORECASE),
    re.compile(r"sexy\s*gaming", re.IGNORECASE),
    re.compile(r"ae\s*casino", re.IGNORECASE),
    re.compile(r"\bsexy\b", re.IGNORECASE),
]


def build_lobby_patterns(lobby_name: str | None = None) -> list[re.Pattern[str]]:
    """Build list of regex patterns from user-configured lobby name or fallback defaults."""
    patterns: list[re.Pattern[str]] = []
    seen: set[str] = set()

    def add_pattern(p: re.Pattern[str]) -> None:
        if p.pattern not in seen:
            seen.add(p.pattern)
            patterns.append(p)

    if lobby_name and lobby_name.strip():
        parts = [p.strip() for p in re.split(r"[,/|;]+", lobby_name) if p.strip()]
        for part in parts:
            escaped = re.escape(part).replace(r"\ ", r"[\s\-_]*")
            add_pattern(re.compile(rf"\b{escaped}\b", re.IGNORECASE))
            add_pattern(re.compile(escaped, re.IGNORECASE))

    for pat in AE_SEXY_PATTERNS:
        add_pattern(pat)

    return patterns


async def run_browser_automation(
    cdp_url: str,
    target_url: str,
    username: str,
    password: str,
    lobby_name: str | None = None,
    on_status: StatusCallback | None = None,
    custom_casino_selector: str | None = None,
    custom_ae_selector: str | None = None,
    timeout_seconds: float = 60.0,
) -> tuple[bool, str]:
    """Automate login and navigation to AE Sexy Baccarat lobby via Chrome CDP.

    Flow:
    1. Connect to Chrome CDP.
    2. Navigate to target_url if not already there.
    3. Fill ID and password and submit login if login form is present.
    4. Find and click "Live Casino" / "Casino trực tuyến".
    5. Find and click "AE Sexy" / "Sexy Casino" / "Sexy Gaming" (handling new tab/popup).
    """
    callback = on_status or (lambda msg: None)
    callback("Đang kết nối tới trình duyệt qua CDP...")

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        msg = "Thiếu thư viện Playwright. Vui lòng chạy: pip install playwright"
        callback(msg)
        return False, msg

    # Normalize localhost to 127.0.0.1 to avoid Windows IPv6 resolution issues (ECONNREFUSED ::1)
    normalized_cdp_url = cdp_url.replace("localhost", "127.0.0.1")

    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.connect_over_cdp(normalized_cdp_url)
        except Exception as exc:
            msg = f"Không thể kết nối Chrome CDP tại {normalized_cdp_url}: {exc}"
            callback(msg)
            return False, msg

        callback("Đã kết nối Chrome CDP thành công.")
        try:
            context = browser.contexts[0] if browser.contexts else await browser.new_context()
            page = context.pages[0] if context.pages else await context.new_page()
            with contextlib.suppress(Exception):
                await page.bring_to_front()

            # Pre-check: Check if AE Sexy lobby is already open in one of the existing tabs
            for pg in context.pages:
                pg_url = getattr(pg, "url", "")
                if any(x in pg_url for x in ("arrpar.com", "tgmeq.com", "player/webMain", "gamehall.jsp")):
                    callback("Phát hiện tab sảnh AE Sexy đang mở sẵn trên trình duyệt!")
                    with contextlib.suppress(Exception):
                        await pg.bring_to_front()
                    callback("Hoàn tất điều hướng! Sẵn sàng quét dữ liệu bàn.")
                    return True, "Sảnh AE Sexy đã sẵn sàng."

            # 1. Check or Navigate to Target URL
            if target_url and target_url.strip():
                clean_url = target_url.strip()
                if not (clean_url.startswith("http://") or clean_url.startswith("https://")):
                    clean_url = f"https://{clean_url}"

                current_url = getattr(page, "url", "")
                if not current_url or clean_url not in current_url:
                    callback(f"Đang mở trang web: {clean_url}...")
                    try:
                        await page.goto(clean_url, wait_until="domcontentloaded", timeout=25000)
                    except Exception as e:
                        logger.warning("Trang load chậm nhưng vẫn tiếp tục: %s", e)
                        callback("Trang phản hồi chậm, tiếp tục xử lý DOM...")
                await asyncio.sleep(1.5)

            if username or password:
                callback("Đang kiểm tra trạng thái đăng nhập...")
                logged_in = await _check_if_logged_in(page, username)
                if not logged_in and (username and password):
                    login_success = await _perform_login(page, username, password, callback)
                    if not login_success:
                        callback("Chưa đăng nhập tự động được. Bạn có thể kiểm tra trực tiếp trên Chrome.")
                elif logged_in:
                    callback(f"Tài khoản đã đăng nhập sẵn trên trình duyệt ({username or 'Đã có phiên'}).")

            await asyncio.sleep(1.5)

            # 3. Click "Live Casino"
            callback("Đang tìm mục Live Casino / Casino trực tuyến...")
            casino_clicked = False
            if custom_casino_selector and custom_casino_selector.strip():
                casino_clicked = await _click_selector(page, custom_casino_selector.strip(), callback, "Custom Casino")

            if not casino_clicked:
                # First check Bong88 specific HeaderMenu_LiveCasino
                try:
                    header_casino = page.locator('[gtag="HeaderMenu_LiveCasino"], div.c-header-menu__item:has-text("Live Casino")').first
                    if await header_casino.count() > 0:
                        callback("Phát hiện menu Live Casino trên thanh điều hướng Bong88, đang mở...")
                        await header_casino.hover()
                        for _ in range(10):
                            if await page.locator('.c-header-submenu--live-casino').count() > 0:
                                casino_clicked = True
                                break
                            box = await header_casino.bounding_box()
                            if box:
                                await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                            await asyncio.sleep(0.2)
                        casino_clicked = True
                except Exception as exc:
                    logger.debug("Bong88 LiveCasino menu hover error: %s", exc)

            if not casino_clicked:
                casino_clicked = await _find_and_click_matching(
                    page,
                    CASINO_MENU_PATTERNS,
                    callback,
                    label="Mục Live Casino",
                )

            if not casino_clicked:
                callback("Không tự động bấm được nút Live Casino (bạn có thể bấm tay trên Chrome nếu cần).")
            else:
                callback("Đã vào mục Live Casino thành công.")

            await asyncio.sleep(0.5)

            # 4. Click Sảnh (mặc định hoặc cấu hình: AE Sexy / Sexy Casino)
            lobby_patterns = build_lobby_patterns(lobby_name)
            lobby_label = lobby_name.strip() if (lobby_name and lobby_name.strip()) else "AE Sexy / Sexy Casino"
            callback(f"Đang tìm sảnh: {lobby_label}...")
            ae_clicked = False

            # Set up listener for new page popup
            new_page_future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

            def on_page_opened(p: Any) -> None:
                if not new_page_future.done():
                    new_page_future.set_result(p)

            context.on("page", on_page_opened)

            try:
                # Strategy 0: Check Bong88 Live Casino Swiper directly
                try:
                    header_casino = page.locator('[gtag="HeaderMenu_LiveCasino"], div.c-header-menu__item:has-text("Live Casino")').first
                    if await header_casino.count() > 0:
                        for _ in range(10):
                            if await page.locator('#swiper-container-live-casino').count() > 0:
                                break
                            box = await header_casino.bounding_box()
                            if box:
                                await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                            await asyncio.sleep(0.2)

                    swiper_opened = await page.evaluate(r'''() => {
                        const swiperEl = document.querySelector('#swiper-container-live-casino');
                        if (swiperEl && swiperEl.swiper) {
                            const swiper = swiperEl.swiper;
                            const targetEl = document.querySelector('#TopMenu-sport243 [data-index="3"]') || 
                                             document.querySelector('#swiper-container-live-casino [data-index="3"]') ||
                                             document.querySelector('#TopMenu-sport243') ||
                                             Array.from(document.querySelectorAll('#swiper-container-live-casino [data-index]')).find(e => {
                                                 const t = (e.innerText || e.textContent || '').toLowerCase();
                                                 return t.includes('ae') || t.includes('sexy');
                                             });
                            if (targetEl && swiper.eventsListeners && swiper.eventsListeners.click && swiper.eventsListeners.click.length > 0) {
                                const mockEvent = {
                                    target: targetEl,
                                    preventDefault: () => {},
                                    stopPropagation: () => {}
                                };
                                swiper.eventsListeners.click[0](mockEvent);
                                return true;
                            }
                        }
                        return false;
                    }''')
                    if swiper_opened:
                        ae_clicked = True
                        callback("Đã kích hoạt mở sảnh AE Sexy qua thanh menu Bong88.")
                except Exception as exc:
                    logger.debug("Không mở được qua swiper direct: %s", exc)

                if not ae_clicked and custom_ae_selector and custom_ae_selector.strip():
                    ae_clicked = await _click_selector(page, custom_ae_selector.strip(), callback, f"Custom {lobby_label}")

                if not ae_clicked:
                    ae_clicked = await _find_and_click_matching(
                        page,
                        lobby_patterns,
                        callback,
                        label=f"Sảnh {lobby_label}",
                        is_lobby=True,
                        lobby_name=lobby_name,
                    )

                if ae_clicked:
                    callback(f"Đã click vào sảnh {lobby_label}, đang chờ sảnh khởi động...")
                    popup_page = None
                    try:
                        popup_page = await asyncio.wait_for(asyncio.shield(new_page_future), timeout=8.0)
                        callback("Phát hiện sảnh AE Sexy đã mở trong tab mới!")
                    except asyncio.TimeoutError:
                        # Fallback: check if opened in any tab of context
                        for pg in context.pages:
                            pg_url = getattr(pg, "url", "")
                            if any(x in pg_url for x in ("arrpar.com", "tgmeq.com", "player/webMain", "gamehall.jsp")):
                                popup_page = pg
                                callback("Phát hiện sảnh AE Sexy trong danh sách tab trình duyệt!")
                                break

                    if popup_page:
                        with contextlib.suppress(Exception):
                            await popup_page.bring_to_front()
                            await popup_page.wait_for_load_state("domcontentloaded", timeout=10000)
                    else:
                        callback("Sảnh AE Sexy mở trong tab hiện tại hoặc qua iframe.")
                else:
                    callback("Không tìm thấy nút sảnh AE Sexy tự động. Vui lòng bấm vào sảnh trên màn hình.")
            finally:
                context.remove_listener("page", on_page_opened)

            callback("Hoàn tất điều hướng! Sẵn sàng quét dữ liệu bàn.")
            return True, "Điều hướng và chuẩn bị sảnh thành công."

        except Exception as exc:
            msg = f"Lỗi trong quá trình điều hướng: {exc}"
            logger.exception(msg)
            callback(msg)
            return False, msg


async def _check_if_logged_in(page: Any, username: str = "") -> bool:
    """Check if the user is already logged in."""
    try:
        logged_in = await page.evaluate(r"""
            (username) => {
                const text = document.body.innerText.toLowerCase();
                const u = (username || '').toLowerCase().trim();

                const hasLogout = text.includes("đăng xuất") || text.includes("logout") || text.includes("thoát");
                const hasBalance = text.includes("số dư") || text.includes("ví chung") || text.includes("thu ngân") || text.includes("cashier") || text.includes("tổng số dư");
                const hasWelcome = text.includes("chào mừng") || text.includes("welcome");
                const hasUsername = u.length > 0 && text.includes(u);

                const hasPasswordInput = !!document.querySelector("input[type='password']:not([style*='display: none'])");

                if (hasPasswordInput) return false;
                return hasLogout || hasBalance || hasWelcome || hasUsername;
            }
        """, username)
        return bool(logged_in)
    except Exception:
        return False


async def _perform_login(page: Any, username: str, password: str, callback: StatusCallback) -> bool:
    """Find input fields, fill credentials, and submit login according to 5-step flow:
    1. Click header 'Đăng nhập' button if inputs are not yet open/visible.
    2. Fill ID & Password.
    3. Click submit 'Đăng nhập' button.
    """
    callback("Bắt đầu quy trình tự động đăng nhập...")
    try:
        user_selectors = [
            "#username",
            "input[name='username']",
            "input[name='user']",
            "input[name='account']",
            "input[name='login']",
            "input[id*='user']",
            "input[id*='account']",
            "input[placeholder*='tên truy cập' i]",
            "input[placeholder*='tài khoản' i]",
            "input[placeholder*='tên' i]",
            "input[placeholder*='user' i]",
            "input[type='text']",
        ]
        pwd_selectors = [
            "#password",
            "input[name='password']",
            "input[name='pass']",
            "input[id*='pass']",
            "input[type='password']",
            "input[placeholder*='mật khẩu' i]",
            "input[placeholder*='password' i]",
        ]

        async def find_visible(selectors: list[str]) -> Any:
            for sel in selectors:
                try:
                    el = await page.query_selector(sel)
                    if el and await el.is_visible():
                        return el
                except Exception:
                    continue
            return None

        user_input = await find_visible(user_selectors)
        password_input = await find_visible(pwd_selectors)

        # 1. Bấm nút 'Đăng nhập' trên thanh điều hướng nếu form chưa mở
        if not user_input or not password_input:
            callback("Đang bấm nút 'Đăng nhập' trên thanh tiêu đề để mở form...")
            header_login_selectors = [
                "a.btn--secondary",
                "a.btn-login",
                "button.btn-login",
                "[class*='btn--secondary']",
                "[class*='login-btn']",
                "a[href*='login']",
                "button[type='button']",
            ]
            clicked_trigger = False
            for t_sel in header_login_selectors:
                try:
                    t_el = await page.query_selector(t_sel)
                    if t_el and await t_el.is_visible():
                        txt = (await t_el.inner_text()).strip().lower() if hasattr(t_el, "inner_text") else ""
                        if "đăng nhập" in txt or "login" in txt or "sign in" in txt or not txt:
                            await t_el.click()
                            clicked_trigger = True
                            await asyncio.sleep(1.0)
                            break
                except Exception:
                    continue

            if not clicked_trigger:
                # Text-based search across all buttons/links
                buttons = await page.query_selector_all("a, button, [role='button'], div.btn, span.btn")
                for b in buttons:
                    with contextlib.suppress(Exception):
                        if await b.is_visible():
                            txt = (await b.inner_text()).strip().lower()
                            if txt in {"đăng nhập", "login", "sign in"}:
                                callback(f"Bấm nút '{txt}'...")
                                await b.click()
                                clicked_trigger = True
                                await asyncio.sleep(1.0)
                                break

            # Chờ các ô nhập liệu hiển thị sau khi bấm mở form
            for _ in range(6):
                user_input = await find_visible(user_selectors)
                password_input = await find_visible(pwd_selectors)
                if user_input and password_input:
                    break
                await asyncio.sleep(0.5)

        if not user_input:
            callback("Không tìm thấy ô nhập tài khoản (ID/Username).")
            return False

        if not password_input:
            callback("Không tìm thấy ô nhập mật khẩu (Password).")
            return False

        # 2. Điền ID và Password
        callback(f"Đang điền ID ({username}) và Mật khẩu...")
        await user_input.click()
        await user_input.fill("")
        await user_input.fill(username)
        await asyncio.sleep(0.3)

        await password_input.click()
        await password_input.fill("")
        await password_input.fill(password)
        await asyncio.sleep(0.3)

        # Tích chọn 'Nhớ Tên Người Dùng' nếu có
        with contextlib.suppress(Exception):
            remember_cb = await page.query_selector(".login-form .checkbox, label[class*='checkbox']:has-text('Nhớ')")
            if remember_cb and await remember_cb.is_visible():
                aria_checked = await remember_cb.get_attribute("aria-checked")
                if aria_checked != "true":
                    await remember_cb.click()

        # 3. Bấm nút Đăng nhập xác thực
        callback("Đang bấm nút Đăng nhập để xác thực...")
        submit_btn = None
        submit_selectors = [
            ".login-form a.btn--secondary",
            ".login-form .btn",
            ".login-form button",
            "form button[type='submit']",
            "button[type='submit']",
            "input[type='submit']",
            ".btn--secondary",
        ]
        for s_sel in submit_selectors:
            try:
                s_el = await page.query_selector(s_sel)
                if s_el and await s_el.is_visible():
                    txt = (await s_el.inner_text()).strip().lower() if hasattr(s_el, "inner_text") else ""
                    if "đăng nhập" in txt or "login" in txt or "sign in" in txt or "xác nhận" in txt:
                        submit_btn = s_el
                        break
            except Exception:
                continue

        if not submit_btn:
            with contextlib.suppress(Exception):
                form = await password_input.evaluate_handle("e => e.closest('form, .login-form, [class*=\"login\"]')")
                form_el = form.as_element() if form else None
                if form_el:
                    btns = await form_el.query_selector_all("button, a, input[type='button'], div[role='button']")
                    for b in btns:
                        txt = (await b.inner_text()).strip().lower() if hasattr(b, "inner_text") else ""
                        if txt in {"đăng nhập", "login", "sign in", "xác nhận"}:
                            submit_btn = b
                            break

        if submit_btn and await submit_btn.is_visible():
            await submit_btn.click()
        else:
            await password_input.press("Enter")

        callback("Đã gửi thông tin đăng nhập! Đang chờ đăng nhập hoàn tất...")
        for _ in range(10):
            await asyncio.sleep(0.8)
            # Kiểm tra xem trang có báo lỗi đăng nhập (như Login Too Often [397] hoặc sai mật khẩu) không
            with contextlib.suppress(Exception):
                error_msg = await page.evaluate(r"""() => {
                    const failedEl = document.querySelector(".login-form__item--failed, .login-form__item--failed .text-void");
                    if (failedEl && failedEl.innerText && failedEl.innerText.trim()) {
                        return failedEl.innerText.trim();
                    }
                    const voidText = document.querySelector(".text-void");
                    if (voidText && voidText.innerText && voidText.innerText.trim()) {
                        return voidText.innerText.trim();
                    }
                    const redEl = Array.from(document.querySelectorAll("*")).find(el => {
                        const t = (el.innerText || '').trim();
                        return (t.includes('Login Too Often') || t.includes('Wait 5 Minutes') || t.includes('[397]') || t.includes('không chính xác')) && el.children.length === 0;
                    });
                    return redEl ? redEl.innerText.trim() : null;
                }""")
                if error_msg:
                    callback(f"❌ Trang báo lỗi: {error_msg}")
                    return False

            curr = getattr(page, "url", "")
            if "/Sports" in curr or "d.8887799.net" in curr:
                break
            if await _check_if_logged_in(page, username):
                break
        return True
    except Exception as exc:
        logger.warning("Lỗi điền form đăng nhập: %s", exc)
        callback(f"Lỗi đăng nhập: {exc}")
        return False


async def _click_selector(page: Any, selector: str, callback: StatusCallback, label: str) -> bool:
    """Click element by CSS/XPath selector."""
    try:
        el = await page.query_selector(selector)
        if el and await el.is_visible():
            callback(f"Bấm selector: {label}")
            await el.click()
            return True
    except Exception as exc:
        logger.debug("Không click được selector %s: %s", selector, exc)
    return False


async def _find_and_click_matching(
    page: Any,
    patterns: list[re.Pattern[str]],
    callback: StatusCallback,
    label: str,
    *,
    is_lobby: bool = False,
    lobby_name: str | None = None,
) -> bool:
    # Strategy 1: Targeted direct selectors for Casino menu or Lobby search
    direct_selectors: list[str] = []
    if is_lobby:
        if lobby_name and lobby_name.strip():
            for part in re.split(r"[,/|;]+", lobby_name):
                p = part.strip()
                if p:
                    direct_selectors.extend([
                        f'a[data-title*="{p}" i]',
                        f'a[data-popupname*="{p}" i]',
                        f'a[data-game-name*="{p}" i]',
                        f'a[href*="{p.lower().replace(" ", "-")}" i]',
                        f'a[href*="{p.lower().replace(" ", "_")}" i]',
                        f'[data-provider*="{p.lower()}" i]',
                    ])

        direct_selectors.extend([
            'a[data-title*="Sexy Casino" i]',
            'a[data-popupname*="Sexy Casino" i]',
            'a[data-title*="AE Sexy" i]',
            'a[data-popupname*="AE Sexy" i]',
            'a[href*="popup-launch/ae-live" i]',
            'a[href*="ae-live" i]',
            'a[data-game-code*="sx~~lobby" i]',
            'a[href*="sexy-casino" i]',
            'a[href*="sexy" i]',
            '[data-provider*="sexy" i]',
            'a:has-text("AE Sexy")',
            'a:has-text("Sexy Casino")',
            'a:has-text("Sexy Gaming")',
        ])
    else:
        # Direct selectors for Live Casino / Sòng bài menu
        direct_selectors.extend([
            'a:has-text("Sòng bài")',
            'a:has-text("Live Casino")',
            'a:has-text("Casino trực tuyến")',
            'a:has-text("Casino")',
            'a[href*="casino" i]',
            'a[href*="live-dealer" i]',
            'a[href*="sòng-bài" i]',
            'a[href*="song-bai" i]',
            'a[data-title*="casino" i]',
            'a[data-title*="sòng bài" i]',
            '.nav a:has-text("Casino")',
            '.menu a:has-text("Casino")',
        ])

    for sel in direct_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                dt = (await loc.get_attribute("data-title")) or (await loc.get_attribute("data-popupname")) or ""
                hr = (await loc.get_attribute("href")) or ""
                txt = ""
                with contextlib.suppress(Exception):
                    txt = (await loc.inner_text()).strip()
                callback(f"Đã tìm thấy {label} qua selector: {sel} ({txt or dt or hr}), đang bấm...")
                await loc.scroll_into_view_if_needed()
                await asyncio.sleep(0.3)
                await loc.click(force=True)
                return True
        except Exception:
            continue

    # Strategy 2: Card / Tile inspection with leaf/near-leaf text & attributes
    try:
        elements = await page.query_selector_all(
            "span, p, h1, h2, h3, h4, h5, h6, div, a, button, img, li"
        )
        for el in elements:
            try:
                child_count = await el.evaluate("e => e.children.length")
                if child_count > 2:
                    continue

                text = ""
                with contextlib.suppress(Exception):
                    text = (await el.inner_text()).strip()

                alt = ""
                with contextlib.suppress(Exception):
                    alt = (await el.get_attribute("alt") or "").strip()

                title = ""
                with contextlib.suppress(Exception):
                    title = (await el.get_attribute("title") or "").strip()

                data_title = ""
                with contextlib.suppress(Exception):
                    data_title = (await el.get_attribute("data-title") or "").strip()

                data_popup = ""
                with contextlib.suppress(Exception):
                    data_popup = (await el.get_attribute("data-popupname") or "").strip()

                src = ""
                with contextlib.suppress(Exception):
                    src = (await el.get_attribute("src") or "").strip()

                href = ""
                with contextlib.suppress(Exception):
                    href = (await el.get_attribute("href") or "").strip()

                combined = f"{text} {alt} {title} {data_title} {data_popup} {src} {href}"
                matched_pattern = None
                for pattern in patterns:
                    if pattern.search(combined):
                        matched_pattern = pattern
                        break

                if matched_pattern:
                    tag = await el.evaluate("e => e.tagName.toLowerCase()")
                    if tag in {"a", "button"} and href:
                        callback(f"Đã tìm thấy {label} ({text or data_title or href}), đang bấm...")
                        await el.scroll_into_view_if_needed()
                        await asyncio.sleep(0.3)
                        await el.click(force=True)
                        return True

                    # Find parent tile/card
                    tile = await el.evaluate_handle(
                        """e => e.closest('.wrapper-game-list-item, .game-tile, [class*="game-item"], [class*="game-tile"], [class*="card"], li, div')"""
                    )
                    tile_el = tile.as_element() if tile else None
                    if tile_el:
                        play_btn = await tile_el.query_selector(
                            "a.playnow, a[href*='popup-launch'], a[href*='ae'], a[href*='sexy'], button, a, .btn"
                        )
                        target = play_btn or tile_el
                        target_text = (await target.inner_text()).strip() if hasattr(target, "inner_text") else ""
                        target_href = (await target.get_attribute("href") or "") if hasattr(target, "get_attribute") else ""
                        callback(f"Đã tìm thấy {label} ({text or data_title or target_text or target_href}), đang bấm...")
                        await target.scroll_into_view_if_needed()
                        await asyncio.sleep(0.3)
                        await target.click(force=True)
                        return True
            except Exception:
                continue
    except Exception as exc:
        logger.debug("Lỗi tìm kiếm card/tile: %s", exc)

    # Strategy 3: In-browser evaluation with direct JS dispatch
    try:
        raw_patterns = [p.pattern for p in patterns]
        eval_result = await page.evaluate(
            """(pats) => {
            function matchesPattern(str) {
                if (!str) return false;
                const s = str.toLowerCase();
                for (const p of pats) {
                    try {
                        const re = new RegExp(p, 'i');
                        if (re.test(s)) return true;
                    } catch {
                        if (s.includes(p.toLowerCase())) return true;
                    }
                }
                return false;
            }

            const candidates = Array.from(document.querySelectorAll('a, button, [role="button"], div[onclick], span[onclick], .game-tile, .wrapper-game-list-item, [class*="game-item"], [class*="card"]'));
            for (const el of candidates) {
                const dataTitle = el.getAttribute('data-title') || el.getAttribute('data-popupname') || '';
                const href = el.getAttribute('href') || '';
                const text = (el.innerText || '').trim();
                const alt = el.getAttribute('alt') || '';
                const title = el.getAttribute('title') || '';
                const gameCode = el.getAttribute('data-game-code') || '';
                const combined = `${text} ${alt} ${title} ${dataTitle} ${href} ${gameCode}`;

                if (matchesPattern(combined)) {
                    const btn = el.querySelector ? el.querySelector('a.playnow, a[href*="popup-launch"], a[href*="ae-live"], a[href*="sexy"], button, .btn') : null;
                    const target = btn || el;
                    target.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    target.click();
                    return { success: true, matched: combined.slice(0, 80) };
                }
            }
            return { success: false };
        }""",
            raw_patterns,
        )
        if eval_result and eval_result.get("success"):
            callback(f"Đã tìm thấy {label} ({eval_result.get('matched')}), đang bấm JS...")
            return True
    except Exception as exc:
        logger.debug("Lỗi evaluate JS: %s", exc)

    # Strategy 4: Fallback to frames/iframes
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        with contextlib.suppress(Exception):
            frame_elements = await frame.query_selector_all("a, button, li, img")
            for fel in frame_elements:
                text = (await fel.inner_text()).strip() if hasattr(fel, "inner_text") else ""
                alt = (await fel.get_attribute("alt") or "").strip()
                title = (await fel.get_attribute("title") or "").strip()
                data_title = (await fel.get_attribute("data-title") or "").strip()
                href = (await fel.get_attribute("href") or "").strip()
                combined = f"{text} {alt} {title} {data_title} {href}"
                for pattern in patterns:
                    if pattern.search(combined):
                        callback(f"Đã tìm thấy {label} trong iframe ({text or alt or data_title}), đang bấm...")
                        await fel.click(force=True)
                        return True

    return False
