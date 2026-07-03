import asyncio
import re
import time
import aiohttp

# ==================== 配置区域 ====================
# 这里可以直接填网络上的 M3U 链接，例如：
INPUT_FILE = "https://raw.githubusercontent.com/.../live.m3u"
# 也可以继续填本地文件名，脚本会自动判断
# INPUT_FILE = "live.m3u"

OUTPUT_FILE = "live_active.m3u"
TIMEOUT_SECONDS = 3
CONCURRENT_LIMIT = 50
# ==================================================


async def fetch_m3u_content(session, path_or_url):
    """自动判断是本地文件还是网络链接，并获取内容"""
    if path_or_url.startswith(("http://", "https://")):
        print(f"🌐 正在从网络下载原始 M3U 列表...")
        try:
            async with session.get(path_or_url, timeout=10) as response:
                if response.status == 200:
                    return await response.text(encoding="utf-8")
                else:
                    print(
                        f"❌ 下载失败，HTTP 状态码: {response.status}"
                    )
                    return None
        except Exception as e:
            print(f"❌ 网络请求出错: {e}")
            return None
    else:
        print(f"📁 正在读取本地 M3U 文件...")
        try:
            with open(path_or_url, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            print(f"❌ 找不到本地文件: {path_or_url}")
            return None


def parse_m3u_content(content):
    """解析 M3U 内容"""
    if not content:
        return []
    pattern = re.compile(r"(#EXTINF:[^\n]+)\n([^\n#]+)")
    matches = pattern.findall(content)
    return [
        {"extinf": extinf.strip(), "url": url.strip()}
        for extinf, url in matches
    ]


async def check_url(session, semaphore, channel):
    async with semaphore:
        url = channel["url"]
        try:
            async with session.head(
                url, timeout=TIMEOUT_SECONDS, allow_redirects=True
            ) as response:
                if response.status == 200:
                    return channel
        except Exception:
            try:
                async with session.get(
                    url, timeout=TIMEOUT_SECONDS, allow_redirects=True
                ) as response:
                    if response.status == 200:
                        return channel
            except Exception:
                pass
        return None


async def main():
    start_time = time.time()
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        # 1. 获取 M3U 内容
        content = await fetch_m3u_content(session, INPUT_FILE)
        channels = parse_m3u_content(content)

        if not channels:
            print("❌ 未提取到任何频道，脚本结束。")
            return

        print(f" 共找到 {len(channels)} 个频道源，开始并发检测...")

        # 2. 并发检测
        tasks = [check_url(session, semaphore, ch) for ch in channels]
        results = await asyncio.gather(*tasks)

    # 3. 过滤并保存
    active_channels = [ch for ch in results if ch is not None]

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in active_channels:
            f.write(f"{ch['extinf']}\n{ch['url']}\n")

    end_time = time.time()
    print("--- 检测完成 ---")
    print(f" 耗时: {end_time - start_time:.2f} 秒")
    print(
        f" 原始源数量: {len(channels)} | 剩余可用源数量: {len(active_channels)}"
    )
    print(f" 结果已保存至: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())