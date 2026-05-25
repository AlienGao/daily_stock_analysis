# -*- coding: utf-8 -*-
"""
FinBERT Sentiment Analysis Service

Uses a pre-trained Chinese financial BERT model to classify news text
sentiment.  Runs inference in an isolated subprocess so the BERT model
(~400 MB) does not bloat the main server process.

Model: hw2942/bert-base-chinese-finetuning-financial-news-sentiment-v2

Optional — requires transformers + torch. Gracefully degrades.
"""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "hw2942/bert-base-chinese-finetuning-financial-news-sentiment-v2"
_MAX_LENGTH = 512
_BATCH_SIZE = 4

# Standalone subprocess script that loads BERT, infers, prints JSON, and exits.
# The 400+ MB model is freed when the subprocess terminates.
_FINBERT_WORKER = """\
import json, os, sys

os.environ.setdefault("TQDM_DISABLE", "1")

import torch
torch.set_num_threads(2)

from transformers import pipeline
pipe = pipeline(
    "text-classification",
    model=os.environ["FINBERT_MODEL"],
    truncation=True,
    max_length=512,
    device=-1,
)

texts = json.loads(sys.stdin.read())
results = []
with torch.no_grad():
    for i in range(0, len(texts), 4):
        batch = texts[i:i+4]
        preds = pipe(batch)
        for t, p in zip(batch, preds):
            if isinstance(p, list):
                p = max(p, key=lambda x: x["score"])
            results.append({"text": t[:100], "label": p["label"], "score": round(p["score"], 4)})

pos = [d for d in results if d["label"] == "positive"]
neg = [d for d in results if d["label"] == "negative"]
neu = [d for d in results if d["label"] == "neutral"]
total = len(results)
ws = sum((1.0 if d["label"]=="positive" else -1.0 if d["label"]=="negative" else 0.0)*d["score"] for d in results)
overall = ws/total if total else 0.0
label = "positive" if overall>0.1 else ("negative" if overall<-0.1 else "neutral")
sp = [f"共分析{total}条新闻"]
if pos: sp.append(f"正面{len(pos)}条")
if neg: sp.append(f"负面{len(neg)}条")
if neu: sp.append(f"中性{len(neu)}条")
sp.append(f"综合情感: {label} ({overall:+.2f})")

print(json.dumps({
    "overall_score": round(overall,4),
    "overall_label": label,
    "positive_count": len(pos),
    "negative_count": len(neg),
    "neutral_count": len(neu),
    "details": results,
    "summary": "，".join(sp),
}))
"""


def get_finbert_service() -> "FinBERTSentimentService":
    """Get or create the shared FinBERT service singleton."""
    global _finbert_instance
    if _finbert_instance is None:
        _finbert_instance = FinBERTSentimentService()
    return _finbert_instance


# Standalone subprocess script for batch inference across multiple stock groups.
# Loads the model once, infers all groups, prints JSON, and exits.
_FINBERT_WORKER_BATCH = """\
import json, os, sys

os.environ.setdefault("TQDM_DISABLE", "1")

import torch
torch.set_num_threads(2)

from transformers import pipeline
pipe = pipeline(
    "text-classification",
    model=os.environ["FINBERT_MODEL"],
    truncation=True,
    max_length=512,
    device=-1,
)

data = json.loads(sys.stdin.read())
results = []

for group in data.get("groups", []):
    texts = [t.strip() for t in group.get("texts", []) if t and len(t.strip()) >= 5]
    if not texts:
        results.append(None)
        continue

    group_results = []
    with torch.no_grad():
        for i in range(0, len(texts), 4):
            batch = texts[i:i + 4]
            preds = pipe(batch)
            for t, p in zip(batch, preds):
                if isinstance(p, list):
                    p = max(p, key=lambda x: x["score"])
                group_results.append({"text": t[:100], "label": p["label"], "score": round(p["score"], 4)})

    pos = [d for d in group_results if d["label"] == "positive"]
    neg = [d for d in group_results if d["label"] == "negative"]
    neu = [d for d in group_results if d["label"] == "neutral"]
    total = len(group_results)
    ws = sum((1.0 if d["label"] == "positive" else -1.0 if d["label"] == "negative" else 0.0) * d["score"] for d in group_results)
    overall = ws / total if total else 0.0
    label = "positive" if overall > 0.1 else ("negative" if overall < -0.1 else "neutral")
    sp = [f"共分析{total}条新闻"]
    if pos:
        sp.append(f"正面{len(pos)}条")
    if neg:
        sp.append(f"负面{len(neg)}条")
    if neu:
        sp.append(f"中性{len(neu)}条")
    sp.append(f"综合情感: {label} ({overall:+.2f})")

    results.append({
        "overall_score": round(overall, 4),
        "overall_label": label,
        "positive_count": len(pos),
        "negative_count": len(neg),
        "neutral_count": len(neu),
        "details": group_results,
        "summary": "，".join(sp),
    })

print(json.dumps({"results": results}))
"""

_finbert_instance: Optional["FinBERTSentimentService"] = None


class FinBERTSentimentService:
    """Chinese financial news sentiment classifier via subprocess isolation.

    Each call to analyze_news_sentiment() spawns a short-lived Python
    subprocess that loads the model, runs inference, prints JSON, and
    exits — freeing all BERT memory after each analysis.
    """

    def __init__(self, model_name: Optional[str] = None):
        self._model_name = model_name or os.getenv("FINBERT_MODEL_NAME", _DEFAULT_MODEL)
        self._available: Optional[bool] = None

    @property
    def is_available(self) -> bool:
        if self._available is None:
            self._available = self._probe_available()
        return self._available

    def _probe_available(self) -> bool:
        """Quickly check if transformers+torch can be imported (no model load)."""
        try:
            result = subprocess.run(
                [sys.executable, "-c", "import transformers, torch; print('ok')"],
                capture_output=True, text=True, timeout=15,
                env={**os.environ, "TQDM_DISABLE": "1"},
            )
            return result.returncode == 0 and "ok" in result.stdout
        except Exception:
            return False

    def analyze_news_sentiment(self, news_texts: List[str]) -> Optional[Dict]:
        """Run FinBERT inference in an isolated subprocess.

        Returns aggregated sentiment dict, or None on failure.
        """
        if not self.is_available:
            return None
        if not news_texts:
            return None

        texts = [t.strip() for t in news_texts if t and len(t.strip()) >= 5]
        if not texts:
            return None

        try:
            env = {**os.environ, "FINBERT_MODEL": self._model_name, "TQDM_DISABLE": "1"}
            proc = subprocess.run(
                [sys.executable, "-c", _FINBERT_WORKER],
                input=json.dumps(texts, ensure_ascii=False),
                capture_output=True, text=True, timeout=120,
                env=env,
            )
            if proc.returncode != 0:
                logger.warning("[FinBERT] 子进程退出码 %d: %s", proc.returncode,
                               proc.stderr[:300] if proc.stderr else "")
                return None
            return json.loads(proc.stdout)
        except subprocess.TimeoutExpired:
            logger.warning("[FinBERT] 子进程超时（120s）")
            return None
        except Exception as e:
            logger.warning("[FinBERT] 子进程失败: %s", e)
            return None

    def analyze_news_sentiment_batch(self, groups: List[List[str]]) -> List[Optional[Dict]]:
        """Run FinBERT inference on multiple text groups in a single subprocess.

        This avoids loading the ~400 MB model multiple times when analyzing
        several stocks.  Returns a list aligned with *groups*.
        """
        if not self.is_available:
            return [None] * len(groups)
        if not groups:
            return []

        # Filter empty groups
        payload_groups = []
        for texts in groups:
            filtered = [t.strip() for t in texts if t and len(t.strip()) >= 5]
            payload_groups.append({"texts": filtered})

        if not any(g["texts"] for g in payload_groups):
            return [None] * len(groups)

        try:
            env = {**os.environ, "FINBERT_MODEL": self._model_name, "TQDM_DISABLE": "1"}
            proc = subprocess.run(
                [sys.executable, "-c", _FINBERT_WORKER_BATCH],
                input=json.dumps({"groups": payload_groups}, ensure_ascii=False),
                capture_output=True, text=True, timeout=180,
                env=env,
            )
            if proc.returncode != 0:
                logger.warning("[FinBERT] batch 子进程退出码 %d: %s", proc.returncode,
                               proc.stderr[:300] if proc.stderr else "")
                return [None] * len(groups)
            data = json.loads(proc.stdout)
            results = data.get("results", [])
            # Ensure length matches input
            while len(results) < len(groups):
                results.append(None)
            return results[:len(groups)]
        except subprocess.TimeoutExpired:
            logger.warning("[FinBERT] batch 子进程超时（180s）")
            return [None] * len(groups)
        except Exception as e:
            logger.warning("[FinBERT] batch 子进程失败: %s", e)
            return [None] * len(groups)
