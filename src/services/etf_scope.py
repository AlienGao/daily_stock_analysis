# -*- coding: utf-8 -*-
"""ETF 专题的品种范围与同主题代表品种选择规则。"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


# 更具体的词优先；匹配后再通过别名合并明显同义主题。
ETF_THEME_KEYWORDS: Tuple[str, ...] = (
    # 宽基
    "上证50", "沪深300", "中证500", "中证1000", "中证2000", "上证指数",
    # 科创板
    "科创50", "科创100", "科创AI", "科创芯片", "科创生物医药",
    "科创新能源", "科创人工智能", "科创材料", "科创信息", "科创成长",
    "科创板综合",
    # 创业板
    "创业板新能源", "创业板50", "创业板300", "创业板200", "创业板综合",
    "创业板增强", "创业板成长", "创业板动量", "创业板低波", "创业板",
    # 红利
    "红利低波动", "红利低波", "红利质量", "红利价值", "红利",
    # 金融
    "证券保险", "券商", "证券", "银行", "保险", "金融地产", "金融",
    # 国防军工
    "军工",
    # 科技
    "人工智能", "信息技术", "数字经济", "工业互联网", "软件服务", "软件",
    "计算机", "互联网", "半导体", "芯片", "机器人", "AI",
    # 医药医疗
    "生物医药", "医疗器械", "创新药", "恒生生物科技", "医药", "医疗",
    "医美", "疫苗", "中药",
    # 消费
    "消费50", "消费80", "消费龙头", "可选消费", "主要消费", "食品饮料",
    "白酒", "消费", "酒",
    # 新能源
    "新能源车电池", "新能源汽车", "新能源车", "绿色电力", "电池", "光伏",
    "新能源", "电力",
    # 周期与制造
    "石油天然气", "有色金属", "稀有金属", "稀土", "钢铁", "煤炭", "化工",
    "矿业", "油气", "石化", "工程机械", "高端制造",
    # 农业、地产、文旅
    "畜牧养殖", "养殖", "农业", "畜牧", "房地产", "地产", "基建", "建材",
    "央企", "旅游", "传媒", "游戏",
    # 跨境
    "中概互联", "恒生互联网", "港股通互联网", "恒生科技", "港股通科技",
    "恒生医疗", "恒生消费", "恒生中国企业", "港股通50", "香港银行", "沪港深",
    "标普500", "日经225", "日经", "纳指",
    # 商品
    "黄金", "豆粕", "能源化工",
)

ETF_THEME_ALIASES = {
    "新能源汽车": "新能源车",
    "新能源车电池": "新能源车",
    "红利低波动": "红利低波",
    "软件服务": "软件",
    "主要消费": "消费",
    "消费80": "消费",
    "房地产": "地产",
    "畜牧养殖": "养殖",
    "券商": "证券",
    "证券保险": "证券",
}

ETF_EXCLUDED_NAME_PARTS: Tuple[str, ...] = (
    "ETF联接", "ETF-LOF", "ETF(LOF)", "ETF-FOF", "联接", "LOF", "REIT",
    "定开", "混合", "指数增强", "FOF",
)


def normalize_etf_code(value: Any) -> str:
    """Return a six-digit bare ETF code."""
    return str(value or "").split(".")[0].strip().zfill(6)


def is_pure_etf_name(name: Any) -> bool:
    """排除联接、LOF、REIT 及被 fund_basic 混入的普通指数基金。"""
    normalized = str(name or "").strip().upper()
    if "ETF" not in normalized:
        return False
    return not any(part.upper() in normalized for part in ETF_EXCLUDED_NAME_PARTS)


def get_etf_theme(name: Any) -> Optional[str]:
    """Return a stable topic key for one ETF name, or None for non-ETF rows."""
    normalized = str(name or "").strip()
    if not is_pure_etf_name(normalized):
        return None
    matches = [keyword for keyword in ETF_THEME_KEYWORDS if keyword.upper() in normalized.upper()]
    if matches:
        keyword = max(matches, key=len)
        return ETF_THEME_ALIASES.get(keyword, keyword)
    # 未覆盖的新主题独立保留，避免因关键词表滞后误删新品种。
    return f"未分类:{normalized.upper()}"


def select_representative_etfs(
    candidates: Iterable[Mapping[str, Any]],
    min_history_days: int = 20,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """每个主题保留一只 ETF，优先近期成交额高且历史长度足够的品种。

    Returns: (theme -> selected candidate, excluded/non-representative candidates)
    """
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    excluded: List[Dict[str, Any]] = []
    for raw in candidates:
        item = dict(raw)
        item["code"] = normalize_etf_code(item.get("code"))
        item["name"] = str(item.get("name") or "").strip()
        theme = get_etf_theme(item["name"])
        if theme is None:
            excluded.append(item)
            continue
        item["theme"] = theme
        groups[theme].append(item)

    selected: Dict[str, Dict[str, Any]] = {}
    for theme, items in groups.items():
        ranked = sorted(
            items,
            key=lambda item: (
                -int(int(item.get("history_days") or 0) >= min_history_days),
                -float(item.get("avg_amount") or 0),
                -int(item.get("history_days") or 0),
                item["code"],
            ),
        )
        selected[theme] = ranked[0]
        excluded.extend(ranked[1:])
    return selected, excluded
