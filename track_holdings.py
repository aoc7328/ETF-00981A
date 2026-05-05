"""
統一投信 ETF 系列 — 每日持股追蹤（多檔 ETF 共用，由環境變數選擇）
執行邏輯：
  1. 抓 ezmoney 今日持股
  2. 跟 repo 的 previous_holdings.json 比對
  3. TranDate 沒變 → 跳過
  4. 有變動 → 在 Notion Database 新增一筆 row，row 內頁含詳細異動
  5. 把今日持股寫回 previous_holdings.json（由 Actions commit 回 repo）
"""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

# ── 設定 ────────────────────────────────────────────────────────────────────
NOTION_TOKEN       = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
FUND_CODE          = os.environ.get("FUND_CODE", "49YTW")
ETF_NAME           = os.environ.get("ETF_NAME", "00981A")
EZMONEY_URL        = f"https://www.ezmoney.com.tw/ETF/Fund/Info?FundCode={FUND_CODE}"
PREV_FILE          = f"previous_holdings_{ETF_NAME}.json"
SECTORS_FILE       = f"sectors_{ETF_NAME}.json"

TW = timezone(timedelta(hours=8))

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


# ── 1. 抓資料 ────────────────────────────────────────────────────────────────
def fetch_holdings() -> dict:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(EZMONEY_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    m = re.search(r'id="DataAsset"\s+data-content="([^"]+)"', resp.text)
    if not m:
        raise RuntimeError("找不到 DataAsset，網頁結構可能已變更")

    raw = m.group(1).replace("&quot;", '"').replace("&amp;", "&").replace("&#39;", "'")
    asset_array = json.loads(raw)

    st = next((a for a in asset_array if a.get("AssetCode") == "ST"), None)
    if not st or not st.get("Details"):
        raise RuntimeError("找不到股票持股明細")

    holdings  = st["Details"]
    tran_date = holdings[0]["TranDate"][:10]   # "2026-04-24"

    nav_obj = next((a for a in asset_array if a.get("AssetCode") == "P_UNIT"), None)
    nav = float(nav_obj["Value"]) if nav_obj else 0.0

    return {"tran_date": tran_date, "holdings": holdings, "nav": nav}


# ── 2. 本地快照 ──────────────────────────────────────────────────────────────
def load_previous() -> dict | None:
    if not os.path.exists(PREV_FILE):
        return None
    with open(PREV_FILE, encoding="utf-8") as f:
        return json.load(f)


def save_current(data: dict):
    with open(PREV_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ 已更新 {PREV_FILE}")


# ── 3. 比對差異 ──────────────────────────────────────────────────────────────
def compare(today: list, yesterday: list) -> dict:
    t_map = {h["DetailCode"]: h for h in today}
    y_map = {h["DetailCode"]: h for h in yesterday}

    added, removed, increased, decreased = [], [], [], []

    for code, th in t_map.items():
        if code not in y_map:
            added.append(th)
        else:
            yh    = y_map[code]
            diff  = th["Share"] - yh["Share"]
            wdiff = round(th["NavRate"] - yh["NavRate"], 2)
            if diff > 0:
                increased.append({**th, "share_diff": diff,  "weight_diff": wdiff})
            elif diff < 0:
                decreased.append({**th, "share_diff": diff, "weight_diff": wdiff})

    for code, yh in y_map.items():
        if code not in t_map:
            removed.append(yh)

    return {"added": added, "removed": removed,
            "increased": increased, "decreased": decreased}


# ── 4. 組 Notion Blocks ──────────────────────────────────────────────────────
def _txt(content: str, bold=False, color="default") -> dict:
    obj = {"type": "text", "text": {"content": content}}
    ann = {}
    if bold:               ann["bold"]  = True
    if color != "default": ann["color"] = color
    if ann:                obj["annotations"] = ann
    return obj

def _bullet(rich_text: list) -> dict:
    return {"object": "block", "type": "bulleted_list_item",
            "bulleted_list_item": {"rich_text": rich_text}}

def _h2(text: str) -> dict:
    return {"object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [_txt(text)]}}

def _h3(text: str) -> dict:
    return {"object": "block", "type": "heading_3",
            "heading_3": {"rich_text": [_txt(text)]}}

def _divider() -> dict:
    return {"object": "block", "type": "divider", "divider": {}}

def _callout(text: str, icon="❗", color="blue_background") -> dict:
    return {
        "object": "block", "type": "callout",
        "callout": {
            "rich_text": [_txt(text, bold=True)],
            "icon": {"type": "emoji", "emoji": icon},
            "color": color,
        },
    }

def _shares_str(share_diff: int) -> str:
    """把股數差轉成「±X 張 Y 股」格式"""
    abs_diff = abs(share_diff)
    sign     = "+" if share_diff > 0 else "-"
    張 = abs_diff // 1000
    股 = abs_diff % 1000
    if 股 == 0:
        return f"{sign}{張} 張"
    elif 張 == 0:
        return f"{sign}{股} 股"
    else:
        return f"{sign}{張} 張 {股} 股"

def build_summary_blocks(diff: dict) -> list:
    """加碼／減碼／新進／出清 詳細 blocks"""
    blocks  = []
    has_any = any(diff[k] for k in diff)

    if not has_any:
        blocks.append(_callout(
            "今日持股股數與前一交易日完全相同，無主動買賣異動。",
            icon="⏭️", color="gray_background"))
        return blocks

    # 摘要 callout
    parts = []
    if diff["increased"]: parts.append(f"加碼 {len(diff['increased'])} 檔")
    if diff["decreased"]: parts.append(f"減碼 {len(diff['decreased'])} 檔")
    if diff["added"]:     parts.append(f"新進 {len(diff['added'])} 檔")
    if diff["removed"]:   parts.append(f"出清 {len(diff['removed'])} 檔")
    blocks.append(_callout("、".join(parts), icon="❗", color="blue_background"))

    # 加碼
    if diff["increased"]:
        blocks.append(_h2("加碼"))
        for h in sorted(diff["increased"], key=lambda x: x["share_diff"], reverse=True):
            blocks.append(_bullet([
                _txt(f"{h['DetailName']} ({h['DetailCode']})", bold=True),
                _txt("　"),
                _txt(_shares_str(h["share_diff"]), color="red"),
                _txt(f"　權重 {h['weight_diff']:+.2f}%", color="red"),
                _txt(f"　→ {h['NavRate']}%"),
            ]))

    # 減碼
    if diff["decreased"]:
        blocks.append(_h2("減碼"))
        for h in sorted(diff["decreased"], key=lambda x: x["share_diff"]):
            blocks.append(_bullet([
                _txt(f"{h['DetailName']} ({h['DetailCode']})", bold=True),
                _txt("　"),
                _txt(_shares_str(h["share_diff"]), color="green"),
                _txt(f"　權重 {h['weight_diff']:+.2f}%", color="green"),
                _txt(f"　→ {h['NavRate']}%"),
            ]))

    # 新進
    if diff["added"]:
        blocks.append(_h2("新進"))
        for h in diff["added"]:
            blocks.append(_bullet([
                _txt(f"{h['DetailName']} ({h['DetailCode']})", bold=True),
                _txt(f"　{h['NavRate']}%　{_shares_str(h['Share'])}"),
            ]))

    # 出清
    if diff["removed"]:
        blocks.append(_h2("出清"))
        for h in diff["removed"]:
            blocks.append(_bullet([
                _txt(f"{h['DetailName']} ({h['DetailCode']})", bold=True)]))

    return blocks


def fetch_industry_map() -> dict:
    """從 sectors_{ETF_NAME}.json 讀取 {股票代號: 族群} 對應表，找不到就無分類"""
    try:
        sectors_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), SECTORS_FILE)
        with open(sectors_path, encoding="utf-8") as f:
            data = json.load(f)
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except FileNotFoundError:
        print(f"ℹ️  {SECTORS_FILE} 不存在，採用無分類模式")
        return {}
    except Exception as e:
        print(f"⚠️  讀取 {SECTORS_FILE} 失敗：{e}，改用無分類模式")
        return {}


def _is_taiwan_stock(detail_name: str) -> bool:
    """股名含中文字 → 臺股；全英文 → 非臺股"""
    return any('\u4e00' <= c <= '\u9fff' for c in detail_name)


def build_holdings_table(holdings: list, industry_map: dict = None) -> list:
    """完整持股表：
    - 若 industry_map 有資料：按細分族群分組（00981A 模式）
    - 若 industry_map 為空：依股名語言分「臺股 / 非臺股」兩組（00988A 模式）
    每組內按權重由高到低排序。
    """
    def row(cells):
        return {
            "object": "block", "type": "table_row",
            "table_row": {"cells": [[_txt(c)] for c in cells]},
        }

    def make_table(stocks):
        header = row(["代號", "名稱", "張數", "權重 %"])
        rows   = [row([h["DetailCode"], h["DetailName"],
                       (f"{h['Share']//1000:,}" if h['Share']%1000==0 else f"{h['Share']/1000:,.3f}"), str(h["NavRate"])])
                  for h in stocks]
        return {
            "object": "block", "type": "table",
            "table": {
                "table_width": 4, "has_column_header": True,
                "has_row_header": False, "children": [header] + rows,
            },
        }

    blocks = [_h2("完整持股明細")]

    if industry_map:
        # 模式一：細分族群（依 sectors_<ETF>.json）
        groups = defaultdict(list)
        for h in holdings:
            industry = industry_map.get(h["DetailCode"], "其他")
            groups[industry].append(h)
        sorted_groups = sorted(
            groups.items(),
            key=lambda x: sum(h["NavRate"] for h in x[1]),
            reverse=True
        )
        for industry, stocks in sorted_groups:
            stocks_sorted = sorted(stocks, key=lambda x: x["NavRate"], reverse=True)
            total_weight = sum(h["NavRate"] for h in stocks_sorted)
            blocks.append(_h3(f"{industry}　{total_weight:.2f}%"))
            blocks.append(make_table(stocks_sorted))
    else:
        # 模式二：臺股 / 非臺股（依股名語言）
        tw, non_tw = [], []
        for h in holdings:
            (tw if _is_taiwan_stock(h["DetailName"]) else non_tw).append(h)
        for label, stocks in [("臺股", tw), ("非臺股", non_tw)]:
            if not stocks:
                continue
            stocks_sorted = sorted(stocks, key=lambda x: x["NavRate"], reverse=True)
            total_weight = sum(h["NavRate"] for h in stocks_sorted)
            blocks.append(_h3(f"{label}　{total_weight:.2f}%"))
            blocks.append(make_table(stocks_sorted))

    return blocks


# ── 5. 呼叫 Notion API ───────────────────────────────────────────────────────
def _append_blocks(page_id: str, blocks: list):
    """分批 append（Notion 單次上限 100 blocks）"""
    for i in range(0, len(blocks), 100):
        r = requests.patch(
            f"https://api.notion.com/v1/blocks/{page_id}/children",
            headers=NOTION_HEADERS,
            json={"children": blocks[i: i + 100]},
            timeout=30,
        )
        r.raise_for_status()


def create_notion_row(tran_date: str, nav: float, diff: dict,
                      holdings: list, is_first_run: bool,
                      industry_map: dict = None):
    has_change = any(diff[k] for k in diff)
    status = "✅ 有異動" if has_change else "⏭️ 無新資料"
    title  = f"{tran_date} 持股快照"
    if is_first_run:
        title += "（首次建立）"

    # 建立 Database row
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS,
        json={
            "parent": {"database_id": NOTION_DATABASE_ID},
            "icon":   {"type": "emoji", "emoji": "📊"},
            "properties": {
                "快照日期":   {"title":  [{"text": {"content": title}}]},
                "資料日期":   {"date":   {"start": tran_date}},
                "總持股檔數": {"number": len(holdings)},
                "加碼":      {"number": len(diff["increased"])},
                "減碼":      {"number": len(diff["decreased"])},
                "新進":      {"number": len(diff["added"])},
                "出清":      {"number": len(diff["removed"])},
                "每單位淨值": {"number": nav},
                "狀態":      {"select": {"name": status}},
            },
        },
        timeout=30,
    )
    r.raise_for_status()
    page_id  = r.json()["id"]
    page_url = r.json().get("url", "")
    print(f"✅ Notion row 建立：{page_url}")

    # 寫入內頁：異動摘要
    _append_blocks(page_id, build_summary_blocks(diff))

    # 寫入內頁：分隔線 + 完整持股表（按產業分組）
    _append_blocks(page_id, [_divider()] + build_holdings_table(holdings, industry_map))

    print("✅ 內頁詳細資訊寫入完成")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print(f"🚀 開始執行　{datetime.now(TW).strftime('%Y-%m-%d %H:%M:%S (台灣時間)')}")

    print("🔄 抓取 ezmoney 持股資料...")
    today = fetch_holdings()
    print(f"✅ 資料日期：{today['tran_date']}　"
          f"持股：{len(today['holdings'])} 檔　淨值：{today['nav']}")

    prev = load_previous()
    if prev is None:
        print("⚠️  首次執行，全部視為新進")
        diff     = compare(today["holdings"], [])
        is_first = True
    else:
        last_date = prev.get("tran_date", "")
        if last_date == today["tran_date"]:
            print(f"⏭️  TranDate 未變動（{today['tran_date']}），跳過")
            sys.exit(0)
        print(f"📖 上次快照：{last_date}　比對差異...")
        diff     = compare(today["holdings"], prev.get("holdings", []))
        is_first = False

    print(f"📊 加碼 {len(diff['increased'])}　減碼 {len(diff['decreased'])}　"
          f"新進 {len(diff['added'])}　出清 {len(diff['removed'])}")

    print("🏭 讀取族群分類...")
    industry_map = fetch_industry_map()
    print(f"✅ 讀取 {len(industry_map)} 支股票族群資料")

    print("📝 寫入 Notion...")
    create_notion_row(today["tran_date"], today["nav"], diff,
                      today["holdings"], is_first, industry_map)

    save_current(today)
    print("🎉 完成！")


if __name__ == "__main__":
    main()
