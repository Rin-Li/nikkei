import logging
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
        self._md_name = f"{stem}_{timestamp}.md"

    def save_results(self, results: list[dict]) -> Path:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(results)

        column_order = [
            "company_name", "stock_code", "industry_33", "industry_17",
            "market", "scale", "is_ai_related", "score", "role", "summary", "error",
        ]
        df = df[[c for c in column_order if c in df.columns]]

        csv_path = self.output_dir / self._csv_name
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logging.info(f"CSV saved: {csv_path}")

        md_path = self.output_dir / self._md_name
        self._write_markdown(df, md_path)
        logging.info(f"Markdown saved: {md_path}")

        self._print_summary(df)
        return csv_path

    def _write_markdown(self, df: pd.DataFrame, path: Path):
        total = len(df)
        ai_count = df["is_ai_related"].sum()
        avg_score = df["score"].mean()

        lines = [
            "# 日本上市企業 AI/半導体サプライチェーン分析レポート",
            "",
            f"- 分析対象: {total} 社",
            f"- AI/半導体関連: {ai_count} 社 ({ai_count/total*100:.1f}%)",
            f"- 平均スコア: {avg_score:.2f}/10",
            f"- 生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]

        # Score 7-10: core related
        high = df[df["score"] >= 7].sort_values("score", ascending=False)
        lines += [f"## 🔴 核心相关 (Score 7-10) — {len(high)} 社", ""]
        for _, row in high.iterrows():
            lines += self._company_detail_block(row)

        # Score 4-6: indirect related
        mid = df[(df["score"] >= 4) & (df["score"] <= 6)].sort_values("score", ascending=False)
        lines += [f"## 🟡 间接相关 (Score 4-6) — {len(mid)} 社", ""]
        for _, row in mid.iterrows():
            lines += self._company_detail_block(row)

        # Score 1-3: unrelated — table only
        low = df[df["score"] <= 3].sort_values(["industry_33", "score"], ascending=[True, False])
        lines += [f"## ⚪ 不相关 (Score 1-3) — {len(low)} 社", ""]
        lines += ["| 代码 | 公司名 | 行业 | 角色 | 评分 |", "|------|--------|------|------|------|"]
        for _, row in low.iterrows():
            lines.append(
                f"| {row['stock_code']} | {row['company_name']} | {row['industry_33']} | {row.get('role', '-')} | {row['score']} |"
            )
        lines.append("")

        path.write_text("\n".join(lines), encoding="utf-8")

    def _company_detail_block(self, row: pd.Series) -> list[str]:
        summary = row.get("summary", "") or ""
        return [
            f"### {row['company_name']} ({row['stock_code']})",
            f"**行业**: {row['industry_33']} | **角色**: {row.get('role', '-')} | **评分**: {row['score']}/10",
            f"> {summary}",
            "",
            "---",
            "",
        ]

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
