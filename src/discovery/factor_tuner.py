# -*- coding: utf-8 -*-
"""因子权重自动调谐器（Factor Tuner）。

根据 FactorMonitor 的 performance.json 数据，在满足最小交易日要求后，
自动调整因子权重，写入 .env 文件。

调整规则（保守版）：
  - 有效因子：avg_return > 1.5% 且 win_rate > 60%  → 权重 × 1.15
  - 反向因子：avg_return < -1% 或 win_rate < 40%  → 权重 × 0.85
  - 其他：维持不变

上下限：min=3, max=25
"""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# .env 备份文件名
_ENV_BAK_SUFFIX = ".bak"

# 调权报告目录
_REPORT_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "discovery_reports"
    / "factor_monitor"
)


class FactorTuner:
    """根据 FactorMonitor 数据自动调优因子权重。"""

    # 有效因子阈值
    _MIN_AVG_RETURN_GOOD = 0.015   # 平均收益 > 1.5%
    _MIN_WIN_RATE_GOOD = 0.60     # 胜率 > 60%

    # 反向因子阈值
    _MAX_AVG_RETURN_BAD = -0.01   # 平均收益 < -1%
    _MAX_WIN_RATE_BAD = 0.40      # 胜率 < 40%

    # 调整幅度
    _UP_RATIO = 1.15
    _DOWN_RATIO = 0.85

    # 权重边界
    _WEIGHT_MIN = 3.0
    _WEIGHT_MAX = 25.0

    def __init__(self, perf_file: Path, env_file: Path, min_days: int = 5):
        self.perf_file = perf_file
        self.env_file = env_file
        self.min_days = min_days

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    def tune(self, mode: str) -> dict:
        """对指定 mode 的因子执行权重调优。

        Args:
            mode: "intraday" 或 "postmarket"

        Returns:
            dict: {factor_name: {"old": float, "new": float, "reason": str}}
        """
        import json

        if not self.perf_file.exists():
            logger.info("[FactorTuner] performance.json 不存在，跳过调优")
            return {}

        try:
            history = json.loads(self.perf_file.read_text())
            if not isinstance(history, list):
                history = history.get("history", [])
        except Exception as e:
            logger.warning("[FactorTuner] 读取 performance.json 失败: %s", e)
            return {}

        # 筛选当前 mode 且已回填的记录
        # 旧数据无 mode 字段，视为 postmarket
        mode_entries = [e for e in history if (e.get("mode") or "postmarket") == mode]
        if not mode_entries:
            logger.info("[FactorTuner] mode=%s 无历史数据，跳过调优", mode)
            return {}

        # 统计每个因子在近 N 个窗口的表现
        all_factors = set()
        for e in mode_entries:
            all_factors.update(e.get("factors", {}).keys())

        factor_stats = {}
        for fname in sorted(all_factors):
            avg_returns = []
            win_rates = []
            valid = 0
            for e in mode_entries:
                fd = e.get("factors", {}).get(fname)
                if fd and fd.get("n_valid", 0) >= 5:
                    avg_returns.append(fd["avg_return"])
                    win_rates.append(fd["win_rate"])
                    valid += 1

            if valid < self.min_days:
                factor_stats[fname] = None  # 数据不足
            else:
                arr_r = np.array(avg_returns)
                arr_w = np.array(win_rates)
                factor_stats[fname] = {
                    "valid_days": valid,
                    "avg_return": float(np.mean(arr_r)),
                    "win_rate": float(np.mean(arr_w)),
                }

        # 读取当前 .env 配置
        env_lines = []
        if self.env_file.exists():
            env_lines = self.env_file.read_text().splitlines()

        # 生成 key → line_index 映射
        env_key_map = {}
        for i, line in enumerate(env_lines):
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                k = stripped.split("=", 1)[0].strip()
                env_key_map[k] = i

        # 计算新权重并写入
        changes = {}
        for fname, stats in factor_stats.items():
            old_weight = self._read_weight_from_env(env_lines, env_key_map, fname)
            if old_weight is None:
                continue

            new_weight, reason = self._calc_new_weight(fname, old_weight, stats)
            if new_weight is not None:
                new_weight = round(new_weight, 2)
                if abs(new_weight - old_weight) < 0.1:
                    continue  # 变化太小跳过
                changes[fname] = {"old": old_weight, "new": new_weight, "reason": reason}
                self._write_weight_to_env(env_lines, env_key_map, fname, new_weight)

        if not changes:
            logger.info("[FactorTuner] mode=%s 无需调整", mode)
            return {}

        # 备份并写入
        bak_path = Path(str(self.env_file) + _ENV_BAK_SUFFIX)
        shutil.copy2(self.env_file, bak_path)
        self.env_file.write_text("\n".join(env_lines) + "\n")
        logger.info("[FactorTuner] 已备份 .env → %s", bak_path.name)

        self._log_report(mode, changes, factor_stats)
        return changes

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    def _calc_new_weight(
        self, fname: str, old: float, stats: dict | None
    ) -> tuple:
        """计算因子新权重。返回 (new_weight, reason)。"""
        if stats is None:
            return None, "数据不足"

        avg_r = stats["avg_return"]
        win_r = stats["win_rate"]

        # 有效因子
        if avg_r > self._MIN_AVG_RETURN_GOOD and win_r > self._MIN_WIN_RATE_GOOD:
            new = old * self._UP_RATIO
            reason = f"有效(收益{avg_r:+.2%},胜率{win_r:.0%})"
        # 反向因子
        elif avg_r < self._MAX_AVG_RETURN_BAD or win_r < self._MAX_WIN_RATE_BAD:
            new = old * self._DOWN_RATIO
            reason = f"反向(收益{avg_r:+.2%},胜率{win_r:.0%})"
        else:
            return None, "正常"

        # 边界裁剪
        new = float(np.clip(new, self._WEIGHT_MIN, self._WEIGHT_MAX))
        return new, reason

    def _read_weight_from_env(
        self, lines: list, key_map: dict, fname: str
    ) -> float | None:
        """从 .env 行列表读取 DISCOVER_WEIGHT_{FNAME} 的值。"""
        key = f"DISCOVER_WEIGHT_{fname.upper()}"
        idx = key_map.get(key)
        if idx is None:
            return None
        val_str = lines[idx].split("=", 1)[1].strip().split("#")[0].strip()
        try:
            return float(val_str)
        except ValueError:
            return None

    def _write_weight_to_env(
        self, lines: list, key_map: dict, fname: str, value: float
    ):
        """修改 .env 行列表中 DISCOVER_WEIGHT_{FNAME} 的值。"""
        key = f"DISCOVER_WEIGHT_{fname.upper()}"
        idx = key_map.get(key)
        if idx is None:
            logger.warning("[FactorTuner] .env 中未找到 %s，跳过", key)
            return
        line = lines[idx]
        # 保留行内注释
        comment = ""
        if "#" in line:
            comment = "  " + line.split("#", 1)[1].strip()
        lines[idx] = f"{key}={value}{comment}"

    def _log_report(self, mode: str, changes: dict, factor_stats: dict):
        lines = [f"[FactorTuner] 权重调整报告（{mode}）"]
        for fname, info in changes.items():
            stats = factor_stats.get(fname)
            stats_str = ""
            if stats:
                stats_str = f" | 收益{stats['avg_return']:+.2%} | 胜率{stats['win_rate']:.0%}"
            lines.append(
                f"  {fname}: {info['old']:.1f} → {info['new']:.1f} "
                f"({info['reason']}){stats_str}"
            )
        logger.info("\n".join(lines))

        # 持久化报告到 JSON
        try:
            _REPORT_DIR.mkdir(parents=True, exist_ok=True)
            date_str = datetime.now().strftime("%Y%m%d")
            report_path = _REPORT_DIR / f"tune_report_{mode}_{date_str}.json"
            report = {
                "mode": mode,
                "date": date_str,
                "tuned_at": datetime.now().isoformat(),
                "changes": changes,
                "factor_stats": {
                    k: v for k, v in factor_stats.items() if v is not None
                },
            }
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
            logger.info("[FactorTuner] 报告已保存 → %s", report_path.name)
        except Exception as e:
            logger.debug("[FactorTuner] 报告保存失败: %s", e)