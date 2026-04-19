import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


class Reporter:
    def __init__(self, config: dict):
        self.output_dir = Path(config["data"]["output_dir"])
        stem = Path(config["data"]["output_filename"]).stem
        suffix = Path(config["data"]["output_filename"]).suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._csv_name = f"{stem}_{timestamp}{suffix}"
        self._companies_dir = self.output_dir / "companies"
        self._index_path = self.output_dir / "index.md"

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._companies_dir.mkdir(parents=True, exist_ok=True)

        # in-memory index: score -> list of entry dicts
        self._index_entries: dict[int, list[dict]] = {s: [] for s in range(1, 11)}
        self._all_results: list[dict] = []

    def add_result(self, result: dict):
        """Called once per company as soon as analysis finishes."""
        self._all_results.append(result)
        row = pd.Series(result)
        self._write_company_page(row)
        self._index_entries[int(row["score"])].append({
            "company_name": row["company_name"],
            "stock_code": row["stock_code"],
            "role": row.get("role", "-"),
            "filename": self._company_filename(row),
        })
        self._flush_index()

    def finalize(self) -> Path:
        """Write CSV and print summary after all companies are done."""
        df = pd.DataFrame(self._all_results)
        column_order = [
            "company_name", "stock_code", "industry_33", "industry_17",
            "market", "scale", "is_ai_related", "score", "role", "summary", "error",
        ]
        df = df[[c for c in column_order if c in df.columns]]
        csv_path = self.output_dir / self._csv_name
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logging.info(f"CSV saved: {csv_path}")
        self._print_summary(df)
        return csv_path

    def _company_filename(self, row: pd.Series) -> str:
        safe_name = re.sub(r'[\\/:*?"<>|]', "_", str(row["company_name"]))
        return f"{row['stock_code']}_{safe_name}.md"

    def _write_company_page(self, row: pd.Series):
        filename = self._company_filename(row)
        path = self._companies_dir / filename
        summary = row.get("summary", "") or ""
        error = row.get("error")

        lines = [
            f"# {row['company_name']} ({row['stock_code']})",
            "",
            f"| 项目 | 内容 |",
            f"|------|------|",
            f"| 行业 (33分类) | {row['industry_33']} |",
            f"| 行业 (17分类) | {row.get('industry_17', '-')} |",
            f"| 市场 | {row['market']} |",
            f"| 规模 | {row.get('scale', '-')} |",
            f"| AI/半导体相关 | {'是' if row.get('is_ai_related') else '否'} |",
            f"| 角色 | {row.get('role', '-')} |",
            f"| 评分 | {row['score']} / 10 |",
            "",
            "## 分析摘要",
            "",
            summary,
            "",
        ]

        if error:
            lines += ["## 错误信息", "", f"```", str(error), "```", ""]

        lines += [f"[← 返回 Index](../index.md)", ""]
        path.write_text("\n".join(lines), encoding="utf-8")

    def _flush_index(self):
        total = len(self._all_results)
        lines = [
            "# AI/半導体サプライチェーン スコアインデックス",
            "",
            f"- 分析済み: **{total}** 社",
            f"- 更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]

        for score in range(10, 0, -1):
            entries = self._index_entries[score]
            if not entries:
                continue
            if score >= 7:
                label = f"🔴 Score {score}"
            elif score >= 4:
                label = f"🟡 Score {score}"
            else:
                label = f"⚪ Score {score}"
            lines += [f"## {label} — {len(entries)} 社", ""]
            for e in entries:
                lines.append(f"- [{e['company_name']} ({e['stock_code']})](companies/{e['filename']}) — {e['role']}")
            lines.append("")

        self._index_path.write_text("\n".join(lines), encoding="utf-8")

    def _print_summary(self, df: pd.DataFrame):
        total = len(df)
        ai_count = int(df["is_ai_related"].sum())
        avg_score = df["score"].mean()
        error_count = int(df["error"].notna().sum())

        print(f"\n{'='*50}")
        print(f"Analysis Complete")
        print(f"  Total:           {total}")
        print(f"  AI/Semi related: {ai_count} ({ai_count/total*100:.1f}%)")
        print(f"  Avg score:       {avg_score:.2f}/10")
        print(f"  API errors:      {error_count}")
        print(f"\nTop roles (AI-related):")
        role_counts = df[df["is_ai_related"]]["role"].value_counts().head(10)
        for role, count in role_counts.items():
            print(f"  {role}: {count}")
        print("=" * 50)
