import asyncio
import re
import time
import aiohttp

# ==================== 配置区域 ====================
# 支持本地文件、网络URL混搭，数量不限
INPUT_SOURCES = [
    "live.m3u",  # 本地文件1
    "https://raw.githubusercontent.com/.../china.m3u",  # 网络源2
    "https://example.com/playlist.txt",  # 网络源3
]

OUTPUT_FILE = "live_active.m3u"
TIMEOUT_SECONDS = 3
CONCURRENT_LIMIT = 50
# ==================================================


async def fetch_content(session, source):
    """兼顾本地与网络的通用读取函数"""
    if source.startswith(("http://", "https://")):
        try:
            async with session.get(source, timeout=10) as response:
                if response.status == 200:
                    return await response.text(encoding="utf-8")
        except Exception as e:
            print(f"⚠️ 无法下载网络源 {source}: {e}")
            return None
    else:
        try:
            with open(source, "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            print(f"⚠️ 找不到本地文件 {source}")
            return None


def parse_flexible_m3u(content):
    """智能解析器：兼容各种混乱格式，提取核心三要素（名称、URL、元数据）"""
    if not content:
        return []

    channels = []
    lines = content.splitlines()
    current_extinf = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("#EXTINF:"):
            current_extinf = line
        elif line.startswith(("http://", "https://", "rtmp://", "p2p://")):
            if current_extinf:
                # --- 核心逻辑：智能清洗与标准化 ---
                # 1. 提取频道名称（通常在逗号后）
                name_match = re.search(r",([^,]+)$", current_extinf)
                name = (
                    name_match.group(1).strip() if name_match else "未知频道"
                )

                # 2. 提取已有的属性（如 tvg-logo, group-title），没有就留空
                logo_match = re.search(r'tvg-logo="([^"]+)"', current_extinf)
                group_match = re.search(
                    r'group-title="([^"]+)"', current_extinf
                )

                logo = logo_match.group(1) if logo_match else ""
                group = group_match.group(1) if group_match else "未分类"

                channels.append(
                    {
                        "name": name,
                        "url": line,
                        "logo": logo,
                        "group": group,
                    }
                )
                current_extinf = None  # 匹配完一组，重置
    return channels


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

    all_raw_channels = []

    async with aiohttp.ClientSession(headers=headers) as session:
        # 1. 循环读取并解析所有源
        for source in INPUT_SOURCES:
            print(f"正在读取源: {source}...")
            content = await fetch_content(session, source)
            source_channels = parse_flexible_m3u(content)
            all_raw_channels.extend(source_channels)

        if not all_raw_channels:
            print("❌ 所有源均未解析出有效频道。")
            return

        # 2. 严格去重（防止多个文件里有完全一模一样的URL）
        seen_urls = set()
        unique_channels = []
        for ch in all_raw_channels:
            if ch["url"] not in seen_urls:
                seen_urls.add(ch["url"])
                unique_channels.append(ch)

        print(
            f"📦 汇总完成！共收集到 {len(all_raw_channels)} 个源，去重后剩余 {len(unique_channels)} 个，开始测速..."
        )

        # 3. 并发测速
        tasks = [check_url(session, semaphore, ch) for ch in unique_channels]
        results = await asyncio.gather(*tasks)

    # 4. 过滤可用频道
    active_channels = [ch for ch in results if ch is not None]

    # 5. 格式化并统一输出为一个标准新文件
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in active_channels:
            # 重新拼装成最规范、完美的 M3U 格式
            logo_str = f' tvg-logo="{ch["logo"]}"' if ch["logo"] else ""
            group_str = (
                f' group-title="{ch["group"]}"' if ch["group"] else ""
            )
            f.write(
                f'#EXTINF:-1{logo_str}{group_str},{ch["name"]}\n{ch["url"]}\n'
            )

    end_time = time.time()
    print("--- 自动化汇总完成 ---")
    print(f"⏱️ 总耗时: {end_time - start_time:.2f} 秒")
    print(f"📈 最终留下的高可用源数量: {len(active_channels)}")
    print(f"💾 完美规范化的新文件已生成: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
