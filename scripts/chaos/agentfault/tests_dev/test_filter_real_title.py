# -*- coding: utf-8 -*-
"""agentfault v2 §(C) 服务侧候选过滤 —— _filter_real_title 离线单测。

不触网、不读 266MB electronics.item：直接猴补 _title_cache。
"""
import sys
import unittest
import logging
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / 'services' / 'recommendation_agent' / 'agents'))

import tools  # noqa: E402


def _mk(item_id, rank, score=0.9, title=None):
    d = {"rank": rank, "item_id": item_id, "score": score}
    if title is not None:
        d["title"] = title
    return d


class TestFilterRealTitle(unittest.TestCase):
    def setUp(self):
        # 猴补缓存：绕过 _load_title_cache 的文件读取
        tools._title_cache = {
            "A1": "Real Widget Alpha",
            "A2": "Product_A2",       # 占位符 -> 剔
            "A3": "",                 # 空串 -> 剔
            "A4": "Real Gadget Beta",
            "A5": "Real Gizmo Gamma",
        }

    def tearDown(self):
        tools._title_cache = None

    def test_mixed_filtering(self):
        items = [
            _mk("A1", 1),
            _mk("A2", 2),   # 占位符
            _mk("A3", 3),   # 空标题
            _mk("MISSING", 4),  # 不在 cache
            _mk("A4", 5),
            _mk("A5", 6),
        ]
        out = tools._filter_real_title(items)
        self.assertEqual([it["item_id"] for it in out], ["A1", "A4", "A5"])

    def test_order_preserved_and_items_unmodified(self):
        items = [_mk("A5", 1), _mk("A1", 2)]
        out = tools._filter_real_title(items)
        self.assertEqual([it["item_id"] for it in out], ["A5", "A1"])
        self.assertIs(out[0], items[0])  # 不复制不改写

    def test_empty_input(self):
        self.assertEqual(tools._filter_real_title([]), [])

    def test_all_filtered(self):
        items = [_mk("A2", 1), _mk("A3", 2), _mk("NOPE", 3)]
        self.assertEqual(tools._filter_real_title(items), [])


class TestToolOverfetchAndTruncate(unittest.TestCase):
    """走整个工具函数（mock requests.post），验证过采 ×3、截断、告警路径。"""

    def setUp(self):
        tools._title_cache = {
            "A1": "Real Widget Alpha",
            "A2": "Product_A2",
            "A3": "",
            "A4": "Real Gadget Beta",
            "A5": "Real Gizmo Gamma",
            "A6": "Real Doohickey Delta",
        }
        self.captured = {}
        self._orig_post = tools.requests.post

    def tearDown(self):
        tools._title_cache = None
        tools.requests.post = self._orig_post

    def _mock_post(self, recs):
        captured = self.captured

        class FakeResp:
            status_code = 200

            def json(self):
                return {"success": True, "recommendations": recs,
                        "inference_time": 0.123}

        def fake_post(url, json=None, timeout=None):
            captured["url"] = url
            captured["payload"] = json
            return FakeResp()

        tools.requests.post = fake_post

    def test_overfetch_x3_and_truncate(self):
        # 6 个候选里 3 个真标题 (A1,A4,A5,A6=4 个真)，top_k=2 应恰好截 2 个
        recs = [_mk("A2", 1), _mk("A1", 2), _mk("A3", 3),
                _mk("A4", 4), _mk("A5", 5), _mk("A6", 6)]
        self._mock_post(recs)
        out = tools.get_sequence_recommendations.func(
            item_sequence=["X1", "X2"], top_k=2)
        self.assertEqual(self.captured["payload"]["top_k"], 6)  # 2*3=6
        self.assertIn("排名1: A1", out)
        self.assertIn("排名2: A4", out)
        self.assertNotIn("A5", out)   # 截断回 top_k=2
        self.assertNotIn("A2", out)   # 占位符已滤
        self.assertNotIn("推荐失败", out)

    def test_overfetch_cap_50(self):
        self._mock_post([_mk("A1", 1)])
        tools.get_sequence_recommendations.func(
            item_sequence=["X1"], top_k=20)
        self.assertEqual(self.captured["payload"]["top_k"], 50)  # min(60,50)

    def test_insufficient_warns_and_returns_partial(self):
        recs = [_mk("A2", 1), _mk("A1", 2), _mk("A3", 3)]  # 仅 1 个真标题
        self._mock_post(recs)
        with self.assertLogs(tools.logger, level=logging.WARNING) as cm:
            out = tools.get_sequence_recommendations.func(
                item_sequence=["X1", "X2", "X3"], top_k=3)
        self.assertIn("排名1: A1", out)
        self.assertNotIn("排名2", out)  # 有多少返回多少，不补占位
        joined = "\n".join(cm.output)
        self.assertIn("seq_len=3", joined)
        self.assertIn("过滤前=3", joined)
        self.assertIn("过滤后=1", joined)


if __name__ == "__main__":
    unittest.main(verbosity=2)
