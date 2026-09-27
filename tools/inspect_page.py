import asyncio
import json
import sys

sys.stdout.reconfigure(encoding="utf-8")
from playwright.async_api import async_playwright


async def inspect():
    async with async_playwright() as p:
        try:
            browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        except Exception as e:
            print("Cannot connect:", e)
            return

        print(f"Contexts: {len(browser.contexts)}")
        for ci, ctx in enumerate(browser.contexts):
            print(f"Context {ci} pages: {len(ctx.pages)}")
            for pi, page in enumerate(ctx.pages):
                url = page.url
                title = await page.title()
                print(f"  Page {pi}: title='{title}' url='{url}'")

                # Check for buttons or links with login/sexy/casino/baccarat
                items = await page.evaluate(
                    r"""() => {
                    const res = [];
                    const all = document.querySelectorAll('button, a, div[role="button"], img, span, p, h1, h2, h3, h4');
                    for (const el of all) {
                        const txt = (el.innerText || '').trim();
                        const alt = el.getAttribute('alt') || '';
                        const title = el.getAttribute('title') || '';
                        const src = el.getAttribute('src') || '';
                        const combined = (txt + ' ' + alt + ' ' + title + ' ' + src).toLowerCase();
                        if (combined.includes('đăng nhập') || combined.includes('login') || combined.includes('sexy') || combined.includes('ae') || combined.includes('baccarat')) {
                            res.push({tag: el.tagName, text: txt.slice(0, 50), alt: alt.slice(0, 50), src: src.slice(-40), visible: el.offsetParent !== null});
                        }
                    }
                    return res.slice(0, 40);
                }"""
                )
                print(f"    Matched elements count: {len(items)}")
                for item in items:
                    print("     ", item)


if __name__ == "__main__":
    asyncio.run(inspect())
