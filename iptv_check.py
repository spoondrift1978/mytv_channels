import asyncio
import re
import time
import aiohttp

# ==================== 配置区域 ====================
# 存放M3U网络地址的文本文件
SOURCES_LIST_FILE = "source_files.txt"

OUTPUT_FILE = "live_active.m3u"
TIMEOUT_SECONDS = 3
CONCURRENT_LIMIT = 50
# ==================================================


def load_sources_from_file(file_path):
    """从文本文件中读取所有非空的 M3U 链接"""
    sources = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # 排除空行和以 # 开头的注释行
                if line and not line.startswith("#"):
                    sources.append(line)
    except FileNotFoundError:
        print(f"❌ 错误：未能在仓库中找到 {file_path} 文件！")
    return sources


async def fetch_content(session, url):
    """下载网络 M3U 内容"""
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
    """智能解析器：兼容各类不规范格式，提取名称、URL、Logo、分组"""
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
                # 提取频道名称（逗号后面的文字）
                name_match = re.search(r",([^,]+)$", current_extinf)
                name = (
                    name_match.group(1).strip() if name_match else "未知频道"
                )

                # 提取属性
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
                current_extinf = None
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
    # 模拟一个完全真实的 Chrome 浏览器请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.google.com/",
    }

    # 1. 从本地文本读取上游链接列表
    input_sources = load_sources_from_file(SOURCES_LIST_FILE)
    if not input_sources:
        print("❌ 未在配置中找到任何有效的 M3U 订阅链接，脚本结束。")
        return

    print(f"📖 成功读取配置，共发现 {len(input_sources)} 个 M3U 订阅源。")
    all_raw_channels = []

    async with aiohttp.ClientSession(headers=headers) as session:
        # 2. 遍历抓取所有订阅源内容
        for url in input_sources:
            print(f"🌐 正在抓取: {url} ...")
            content = await fetch_content(session, url)
            source_channels = parse_flexible_m3u(content)
            all_raw_channels.extend(source_channels)

        if not all_raw_channels:
            print("❌ 所有订阅源均未解析出任何电视频道。")
            return

        # 3. 全局去重（依据播放 URL）
        seen_urls = set()
        unique_channels = []
        for ch in all_raw_channels:
            if ch["url"] not in seen_urls:
                seen_urls.add(ch["url"])
                unique_channels.append(ch)

        print(
            f"📦 汇总完毕！总计 {len(all_raw_channels)} 个源，去重后剩余 {len(unique_channels)} 个，开始并发测速..."
        )

        # 4. 并发测速
        tasks = [check_url(session, semaphore, ch) for ch in unique_channels]
        results = await asyncio.gather(*tasks)

    # 5. 过滤出活着的频道
    active_channels = [ch for ch in results if ch is not None]

    # 6. 标准化写入输出文件
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        for ch in active_channels:
            logo_str = f' tvg-logo="{ch["logo"]}"' if ch["logo"] else ""
            group_str = (
                f' group-title="{ch["group"]}"' if ch["group"] else ""
            )
            f.write(
                f'#EXTINF:-1{logo_str}{group_str},{ch["name"]}\n{ch["url"]}\n'
            )

    end_time = time.time()
    print("--- 自动化扫描清洗完成 ---")
    print(f"⏱️ 总耗时: {end_time - start_time:.2f} 秒")
    print(f"📈 筛选后的高可用频道总数: {len(active_channels)}")
    print(f"💾 最终订阅文件已生成/更新: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
