import asyncio
import json
import logging
import os

import anthropic
import openai
import pandas as pd
from tqdm.asyncio import tqdm_asyncio


class Analyzer:
    def __init__(self, config: dict):
        self.config = config
        self.concurrency = config["concurrency"]
        provider_name = config["active_provider"]
        provider_cfg = config["providers"][provider_name]

        api_key = os.environ.get(provider_cfg["api_key_env"])
        if not api_key:
            raise ValueError(f"Environment variable {provider_cfg['api_key_env']} not set")

        if provider_name == "anthropic":
            self._provider = "anthropic"
            self._client = anthropic.AsyncAnthropic(api_key=api_key)
        else:
            self._provider = "openai_compat"
            self._client = openai.AsyncOpenAI(
                api_key=api_key,
                base_url=provider_cfg.get("base_url"),
            )

        self._model = provider_cfg["model"]
        self._max_tokens = provider_cfg["max_tokens"]
        self._semaphore = asyncio.Semaphore(self.concurrency["max_concurrent_requests"])
        self._system_prompt = config["prompts"]["system"]
        self._user_template = config["prompts"]["user_template"]

    async def analyze_all(self, df: pd.DataFrame) -> list[dict]:
        tasks = [self._analyze_with_retry(row) for _, row in df.iterrows()]
        return await tqdm_asyncio.gather(*tasks, desc="Analyzing companies", total=len(tasks))

    async def _analyze_with_retry(self, row: pd.Series) -> dict:
        max_attempts = self.concurrency["retry_max_attempts"]
        base_delay = self.concurrency["retry_base_delay"]
        max_delay = self.concurrency["retry_max_delay"]

        for attempt in range(max_attempts):
            try:
                async with self._semaphore:
                    return await self._analyze_company(row)
            except (anthropic.RateLimitError, openai.RateLimitError) as e:
                headers = getattr(getattr(e, "response", None), "headers", {}) or {}
                delay = min(float(headers.get("retry-after", base_delay * (2 ** attempt))), max_delay)
                logging.warning(f"Rate limited on {row['company_name']}, waiting {delay:.1f}s")
                await asyncio.sleep(delay)
            except (anthropic.APIStatusError, openai.APIStatusError) as e:
                status = getattr(e, "status_code", 500)
                if status >= 500:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logging.warning(f"Server error {status} on {row['company_name']}, retry {attempt + 1}")
                    await asyncio.sleep(delay)
                else:
                    logging.error(f"Client error {status} on {row['company_name']}")
                    return self._error_result(row, str(e))
            except (asyncio.TimeoutError, anthropic.APIConnectionError, openai.APIConnectionError) as e:
                delay = min(base_delay * (2 ** attempt), max_delay)
                logging.warning(f"Connection error on {row['company_name']}, retry {attempt + 1}: {e}")
                await asyncio.sleep(delay)

        logging.error(f"All retries exhausted for {row['company_name']}")
        return self._error_result(row, "max_retries_exceeded")

    async def _analyze_company(self, row: pd.Series) -> dict:
        user_prompt = self._user_template.format(
            company_name=row["company_name"],
            stock_code=row["stock_code"],
            industry_33=row["industry_33"],
            industry_17=row.get("industry_17", "-"),
            market=row["market"],
            scale=row.get("scale", "-"),
        )

        timeout = self.concurrency["request_timeout"]

        if self._provider == "anthropic":
            response = await asyncio.wait_for(
                self._client.messages.create(
                    model=self._model,
                    max_tokens=self._max_tokens,
                    system=self._system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                ),
                timeout=timeout,
            )
            text = next(b.text for b in response.content if b.type == "text")
        else:
            response = await asyncio.wait_for(
                self._client.chat.completions.create(
                    model=self._model,
                    max_tokens=self._max_tokens,
                    messages=[
                        {"role": "system", "content": self._system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                ),
                timeout=timeout,
            )
            text = response.choices[0].message.content or ""

        parsed = self._parse_response(text, row)
        return {
            "company_name": row["company_name"],
            "stock_code": row["stock_code"],
            "industry_33": row["industry_33"],
            "industry_17": row.get("industry_17", "-"),
            "market": row["market"],
            "scale": row.get("scale", "-"),
            **parsed,
        }

    def _parse_response(self, text: str, row: pd.Series) -> dict:
        try:
            text = text.strip()
            if text.startswith("```"):
                parts = text.split("```")
                text = parts[1]
                if text.startswith("json"):
                    text = text[4:]
            data = json.loads(text.strip())
            return {
                "summary": str(data.get("summary", "")),
                "is_ai_related": bool(data.get("is_ai_related", False)),
                "role": str(data.get("role", "unrelated")),
                "score": max(1, min(10, int(data.get("score", 1)))),
                "error": None,
            }
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logging.warning(f"JSON parse error for {row['company_name']}: {e} | raw: {text[:200]}")
            return {
                "summary": text[:500],
                "is_ai_related": False,
                "role": "parse_error",
                "score": 1,
                "error": str(e),
            }

    def _error_result(self, row: pd.Series, error_msg: str) -> dict:
        return {
            "company_name": row["company_name"],
            "stock_code": row["stock_code"],
            "industry_33": row["industry_33"],
            "industry_17": row.get("industry_17", "-"),
            "market": row["market"],
            "scale": row.get("scale", "-"),
            "summary": "",
            "is_ai_related": False,
            "role": "api_error",
            "score": 1,
            "error": error_msg,
        }
