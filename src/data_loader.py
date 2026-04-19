import logging
import pandas as pd
from pathlib import Path


class DataLoader:
    def __init__(self, config: dict):
        self.input_file = Path(config["data"]["input_file"])
        self.target_sectors = config["target_sectors"]

    def load_and_filter(self) -> pd.DataFrame:
        df = pd.read_excel(
            self.input_file,
            engine="xlrd",
            dtype={"コード": str},
        )
        df = df.rename(columns={
            "日付": "date",
            "コード": "stock_code",
            "銘柄名": "company_name",
            "市場・商品区分": "market",
            "33業種コード": "industry_code_33",
            "33業種区分": "industry_33",
            "17業種コード": "industry_code_17",
            "17業種区分": "industry_17",
            "規模コード": "scale_code",
            "規模区分": "scale",
        })
        df["stock_code"] = df["stock_code"].apply(self._clean_stock_code)

        mask = df["industry_33"].isin(self.target_sectors)
        filtered = df[mask].copy().reset_index(drop=True)

        logging.info(f"Total rows: {len(df)}, after sector filter: {len(filtered)}")
        for sector in self.target_sectors:
            count = (filtered["industry_33"] == sector).sum()
            logging.info(f"  {sector}: {count} companies")

        return filtered

    def _clean_stock_code(self, code: str) -> str:
        try:
            return str(int(float(code)))
        except (ValueError, TypeError):
            return str(code)
