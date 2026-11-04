#!/usr/bin/env python3
"""定时刷新上游订阅缓存。

读取 KV 中的 LINK.txt（聚合订阅框），逐个拉取其中的 http(s) 订阅（忽略证书错误），
解析出节点链接后写入 KV 的 cache:<sha256(url)>，供 _worker.js 在实时拉取失败时回退使用。

安全约束（仓库公开，Actions 日志公开）：
- 订阅 URL 与节点内容一律不打印，只输出域名、状态和节点数。
- 每个 URL 先通过 ::add-mask:: 注册为敏感值，作为兜底。

仅使用 Python 标准库。
"""
import base64
import binascii
import hashlib
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

API = "https://api.cloudflare.com/client/v4"
LINK_KEY = "LINK.txt"
UPSTREAM_UA = "v2rayN/6.45"  # 让机场返回 base64 节点列表，而不是 Clash YAML
TIMEOUT = 30

ACCOUNT_ID = os.environ["CF_ACCOUNT_ID"]
NAMESPACE_ID = os.environ["CF_KV_NAMESPACE_ID"]
API_TOKEN = os.environ["CF_API_TOKEN"]


def cache_key(url: str) -> str:
    """必须与 _worker.js 中的 订阅缓存键() 一致。"""
    return "cache:" + hashlib.sha256(url.encode("utf-8")).hexdigest()


def kv_url(key: str) -> str:
    return (f"{API}/accounts/{ACCOUNT_ID}/storage/kv/namespaces/{NAMESPACE_ID}"
            f"/values/{urllib.parse.quote(key, safe='')}")


def kv_request(method: str, key: str, body: bytes | None = None) -> bytes | None:
    req = urllib.request.Request(kv_url(key), data=body, method=method,
                                 headers={"Authorization": f"Bearer {API_TOKEN}"})
    if body is not None:
        req.add_header("Content-Type", "text/plain; charset=utf-8")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        if method == "GET" and e.code == 404:
            return None
        raise RuntimeError(f"KV {method} 失败: HTTP {e.code}") from None


def parse_subscription_urls(link_txt: str) -> list[str]:
    """复刻 _worker.js 中 ADD() + 订阅链接筛选 + 去重的逻辑，保证 URL 与 Worker 看到的一致。"""
    text = re.sub(r"[\t\"'|\r\n]+", "\n", link_txt)
    text = re.sub(r"\n+", "\n", text).strip("\n")
    urls = []
    for line in text.split("\n"):
        if line.lower().startswith("http") and line.strip() and line not in urls:
            urls.append(line)
    return urls


def host_of(url: str) -> str:
    return url.split("://", 1)[1].split("/", 1)[0]


def extract_links(raw: bytes) -> list[str]:
    text = raw.decode("utf-8", errors="ignore").strip()
    if "://" not in text:
        compact = re.sub(r"\s", "", text)
        if not compact or not re.fullmatch(r"[A-Za-z0-9+/=_-]+", compact):
            return []  # Clash YAML、HTML 错误页等，不是节点列表
        try:
            padded = compact + "=" * (-len(compact) % 4)
            text = base64.b64decode(padded, altchars=b"-_" if "-" in compact or "_" in compact else None).decode("utf-8", errors="ignore")
        except (binascii.Error, ValueError):
            return []
    return [line.strip() for line in text.splitlines() if "://" in line]


def fetch_upstream(url: str) -> bytes:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE  # 关键：上游证书过期也能拉取
    req = urllib.request.Request(url, headers={"User-Agent": UPSTREAM_UA})
    with urllib.request.urlopen(req, timeout=TIMEOUT, context=ctx) as resp:
        return resp.read()


def main() -> int:
    link_raw = kv_request("GET", LINK_KEY)
    if not link_raw:
        print("KV 中没有 LINK.txt，无需刷新")
        return 0
    urls = parse_subscription_urls(link_raw.decode("utf-8", errors="ignore"))
    for url in urls:
        print(f"::add-mask::{url}")
    print(f"共 {len(urls)} 个上游订阅")

    failed = 0
    for i, url in enumerate(urls, 1):
        label = f"[{i}] {host_of(url)}"
        try:
            links = extract_links(fetch_upstream(url))
        except Exception as e:  # 只输出异常类型，避免把 URL 带进日志
            print(f"::warning::{label} 拉取失败（{type(e).__name__}），保留旧缓存")
            failed += 1
            continue
        if not links:
            print(f"::warning::{label} 返回内容中没有节点，保留旧缓存")
            failed += 1
            continue

        body = "\n".join(links).encode("utf-8")
        key = cache_key(url)
        if kv_request("GET", key) == body:
            print(f"{label} 节点 {len(links)} 个，无变化")
        else:
            kv_request("PUT", key, body)
            print(f"{label} 节点 {len(links)} 个，已更新缓存")

    if failed:
        print(f"::error::{failed}/{len(urls)} 个上游刷新失败")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
