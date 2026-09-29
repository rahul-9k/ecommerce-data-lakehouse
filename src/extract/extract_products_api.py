"""Fetch every products page; retain API payloads and write metadata-enriched JSONL.

Live data comes from DummyJSON, which is itself a test-data API, not a shop.
Offline mode is explicit and uses a much smaller hand-authored test fixture.
"""
from __future__ import annotations

import json
import logging
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests

from src.common import METADATA_COLUMNS, PipelineError, read_json

API_URL = "https://dummyjson.com/products"
LOG = logging.getLogger(__name__)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def request_page(session: requests.Session, skip: int, page_size: int, attempts: int = 4) -> requests.Response:
    """Bounded retries for GET only, with connect/read timeouts and verified HTTPS."""
    if attempts < 1:
        raise PipelineError("HTTP attempts must be positive.")
    for attempt in range(attempts):
        delay = min(2 ** attempt, 8)
        try:
            response = session.get(
                API_URL,
                params={"limit": page_size, "skip": skip},
                timeout=(5, 30),
            )
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < attempts - 1:
                # Honor a numeric Retry-After where reasonable. Date-form values
                # use our bounded backoff instead; this is not a rate-limit scheduler.
                try:
                    delay = min(max(float(response.headers.get("Retry-After", delay)), 0), 30)
                except (TypeError, ValueError):
                    pass
                LOG.warning("HTTP %s; retrying page after %.1fs", response.status_code, delay)
                response.close()
                time.sleep(delay)
                continue
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                response.close()
                raise PipelineError(f"Product API returned HTTP {response.status_code}.") from exc
            return response
        except requests.exceptions.SSLError as exc:
            raise PipelineError("HTTPS certificate validation failed. Do not disable verification.") from exc
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == attempts - 1:
                raise PipelineError(
                    "Could not reach the product API after retries. Check your connection; "
                    "use --offline only for an explicitly labelled local test run."
                ) from exc
            LOG.warning("Temporary API connection failure; retrying after %ss", delay)
            time.sleep(delay)
    raise PipelineError("HTTP retry loop ended unexpectedly.")


def validate_product(product: Any) -> int:
    if not isinstance(product, dict):
        raise PipelineError("Product must be a JSON object.")
    product_id = product.get("id")
    if isinstance(product_id, bool) or not isinstance(product_id, int) or product_id <= 0:
        raise PipelineError("Product id must be a positive integer.")
    for field in ("title", "category"):
        if not isinstance(product.get(field), str) or not product[field].strip():
            raise PipelineError(f"Product {product_id} has an empty/invalid {field}.")
    try:
        price = Decimal(str(product["price"]))
        if not price.is_finite() or price < 0:
            raise ValueError("Invalid price")
    except (KeyError, ValueError, InvalidOperation) as exc:
        raise PipelineError(f"Product {product_id} has an invalid price.") from exc
    return product_id


def extract_products(
    destination: Path,
    batch_id: str,
    ingested_at: str,
    page_size: int = 100,
    offline_fixture: Path | None = None,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not 1 <= page_size <= 100:
        raise PipelineError("API page size must be between 1 and 100.")
    destination.mkdir(parents=True, exist_ok=False)
    pages_directory = destination / "pages"
    pages_directory.mkdir()
    products: list[dict[str, Any]] = []
    seen: set[int] = set()
    total: int | None = None
    skip = 0
    pages = 0
    source = "local_test_fixture" if offline_fixture is not None else "dummyjson_products_api"
    own_session = session is None and offline_fixture is None
    if own_session:
        session = requests.Session()
        session.headers.update({"User-Agent": "ecommerce-data-lakehouse-phase1/1.0"})
    try:
        while True:
            if offline_fixture is not None:
                raw_body = offline_fixture.read_bytes()
                payload = read_json(offline_fixture)
            else:
                assert session is not None
                response = request_page(session, skip, page_size)
                try:
                    raw_body = response.content
                    payload = response.json()
                except ValueError as exc:
                    raise PipelineError("Product API returned invalid JSON.") from exc
                finally:
                    response.close()
            if not isinstance(payload, dict) or not isinstance(payload.get("products"), list):
                raise PipelineError("Product API payload must contain a products list.")
            page_total = payload.get("total")
            if isinstance(page_total, bool) or not isinstance(page_total, int) or page_total <= 0:
                raise PipelineError("Product API total must be a positive integer.")
            if total is None:
                total = page_total
            elif page_total != total:
                raise PipelineError("Catalogue size changed during pagination; rerun a fresh batch.")
            if payload.get("skip") != skip:
                raise PipelineError("API page offset did not match the requested skip.")
            page_products = payload["products"]
            if not page_products:
                raise PipelineError("API returned an empty page before the catalogue was complete.")
            for product in page_products:
                product_id = validate_product(product)
                if product_id in seen:
                    raise PipelineError(f"Duplicate product id across API pages: {product_id}")
                if any(field in product for field in METADATA_COLUMNS):
                    raise PipelineError("Product payload collides with reserved metadata columns.")
                seen.add(product_id)
                products.append(product)
            (pages_directory / f"page_{pages:04d}.json").write_bytes(raw_body)
            pages += 1
            skip += len(page_products)
            if skip > total:
                raise PipelineError("Product count exceeded the API's reported total.")
            LOG.info("Products page %s: %s / %s", pages, skip, total)
            if skip == total:
                break
            if offline_fixture is not None or pages >= 1000:
                raise PipelineError("Incomplete fixture or excessive API page count.")
        products.sort(key=lambda product: product["id"])
        with (destination / "products.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
            for product in products:
                record = {
                    **product,
                    "_batch_id": batch_id,
                    "_ingested_at": ingested_at,
                    "_source": source,
                }
                handle.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
        return products, {
            "rows": len(products), "pages": pages, "source": source,
            "endpoint": None if offline_fixture is not None else API_URL,
            "pagination_note": "Offset pagination; no transactional snapshot guarantee from upstream.",
        }
    finally:
        if own_session and session is not None:
            session.close()
