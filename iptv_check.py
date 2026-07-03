import asyncio
import re
import time
import aiohttp

# ==================== 配置区域 ====================
SOURCES_LIST_FILE = "source_files.txt"
OUTPUT_FILE = "live_active.m3u"
TIMEOUT_SECONDS = 8  # 超过3秒未响应直接淘汰
CONCURRENT_LIMIT = 5  # 并发限制
MAX_SOURCES_PER_CHANNEL = 3  # 每个 tvg-name 最多保留的优质源数量
# ==================================================


def load_sources_from_file(file_path):
    sources = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    sources.append(line)
    except FileNotFoundError:
        print(f"❌ 错误：未能在仓库中找到 {file_path} 文件！")
    return sources


async def fetch_content(session, url):
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                return await response.text(encoding="utf-8")
            else:
                print(f"⚠️ 下载失败，HTTP 状态码: {response.status} -> {url}")
                return None
    except Exception as e:
        print(f"⚠️ 无法下载网络源: {url} | 错误: {e}")
        return None


def parse_flexible_m3u(content):
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
                name_match = re.search(r",([^,]+)$", current_extinf)
                name = (
                    name_match.group(1).strip() if name_match else "未知频道"
                )

                logo_match = re.search(r'tvg-logo="([^"]+)"', current_extinf)
                group_match = re.search(
                    r'group-title="([^"]+)"', current_extinf
                )
                name_attr_match = re.search(
                    r'tvg-name="([^"]+)"', current_extinf
                )

                logo = logo_match.group(1) if logo_match else ""
                group = group_match.group(1) if group_match else "未分类"
                tvg_name = (
                    name_attr_match.group(1) if name_attr_match else name
                )

                channels.append(
                    {
                        "name": name,
                        "url": line,
                        "logo": logo,
                        "group": group,
                        "tvg_name": tvg_name,
                        "speed": 999.0,  # 初始响应时间设定为一个极大值
                    }
                )
                current_extinf = None
    return channels


async def check_url_speed(session, semaphore, channel):
    """测速核心函数：记录连接建立到响应所需的精确时间"""
    async with semaphore:
        url = channel["url"]
        start_time = time.time()
        try:
            async with session.head(
                url, timeout=TIMEOUT_SECONDS, allow_redirects=True
            ) as response:
                if response.status == 200:
                    channel["speed"] = time.time() - start_time
                    return channel
        except Exception:
            try:
                # 如果 HEAD 请求不支持，退回到 GET 请求，只读取前几字节，防止拖慢速度
                async with session.get(
                    url, timeout=TIMEOUT_SECONDS, allow_redirects=True
                ) as response:
                    if response.status == 200:
                        channel["speed"] = time.time() - start_time
                        return channel
            except Exception:
                pass
        return None


async def main():
    start_time = time.time()
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }

    input_sources = load_sources_from_file(SOURCES_LIST_FILE)
    if not input_sources:
        print("❌ 未在配置中找到任何有效的 M3U 订阅链接，脚本结束。")
        return

    print(f"📖 成功读取配置，共发现 {len(input_sources)} 个 M3U 订阅源。")
    all_raw_channels = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for url in input_sources:
            print(f"🌐 正在抓取: {url} ...")
            content = await fetch_content(session, url)
            source_channels = parse_flexible_m3u(content)
            all_raw_channels.extend(source_channels)

        if not all_raw_channels:
            print("❌ 所有订阅源均未解析出任何电视频道。")
            return

        # 全局去重（避免完全一样的URL重复测速）
        seen_urls = set()
        unique_channels = []
        for ch in all_raw_channels:
            if ch["url"] not in seen_urls:
                seen_urls.add(ch["url"])
                unique_channels.append(ch)

        print(
            f"📦 汇总完毕！总计 {len(all_raw_channels)} 个源，去重后剩余 {len(unique_channels)} 个，开始进行精确时延测速..."
        )

        # 并发测速
        tasks = [
            check_url_speed(session, semaphore, ch) for ch in unique_channels
        ]
        results = await asyncio.gather(*tasks)

    # 过滤掉无法访问的死链
    active_channels = [ch for ch in results if ch is not None]

    # --- 核心：按 tvg-name 分组并筛选前 3 名 ---
    grouped_channels = {}
    for ch in active_channels:
        t_name = ch["tvg_name"]
        if t_name not in grouped_channels:
            grouped_channels[t_name] = []
        grouped_channels[t_name].append(ch)

    final_channels = []
    for t_name, ch_list in grouped_channels.items():
        # 按照 speed（响应延迟秒数）从小到大排序，越小代表速度越快
        ch_list.sort(key=lambda x: x["speed"])
        # 使用切片，只取前 MAX_SOURCES_PER_CHANNEL 个（如果少于3个则会全部保留）
        top_sources = ch_list[:MAX_SOURCES_PER_CHANNEL]
        final_channels.extend(top_sources)

    # 6. 标准化写入输出文件（保留绑定的免费EPG）
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(
            '#EXTM3U x-tvg-url="https://epg.112114.xyz/pp.xml,https://live.fanmingming.com/e.xml"\n'
        )
        for ch in final_channels:
            name_str = f' tvg-name="{ch["tvg_name"]}"'
            logo_str = f' tvg-logo="{ch["logo"]}"' if ch["logo"] else ""
            group_str = (
                f' group-title="{ch["group"]}"' if ch["group"] else ""
            )
            f.write(
                f'#EXTINF:-1{name_str}{logo_str}{group_str},{ch["name"]}\n{ch["url"]}\n'
            )

    end_time = time.time()
    print("--- 自动化精简筛选完成 ---")
    print(f"⏱️ 总耗时: {end_time - start_time:.2f} 秒")
    print(
        f"📈 测速存活共 {len(active_channels)} 个源 | 优胜劣汰后最终保留: {len(final_channels)} 个源"
    )
    print(f"💾 最终高精简订阅文件已更新: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
