import asyncio
import logging

import yaml

from src.analyzer import Analyzer
from src.data_loader import DataLoader
from src.reporter import Reporter


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    config = load_config()
    provider = config["active_provider"]
    model = config["providers"][provider]["model"]
    logging.info(f"Provider: {provider} | Model: {model}")

    df = DataLoader(config).load_and_filter()
    logging.info(f"Loaded {len(df)} companies to analyze")

    reporter = Reporter(config)
    asyncio.run(Analyzer(config).analyze_all(df, on_result=reporter.add_result))
    reporter.finalize()


if __name__ == "__main__":
    main()
