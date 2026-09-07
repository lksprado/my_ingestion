import json
from urllib.parse import urlencode

from .extractor import Extractor


class AtacadaoScraper:
    def __init__(self, store_cfg: dict, extractor: Extractor):
        self.store_id = store_cfg["id"]
        self.cfg = store_cfg
        self.extractor = extractor

    def search(self, keyword: str) -> list[dict]:
        url = self._build_url(keyword)
        data = self.extractor.make_request(url=url, mode="json")
        return self._parse_products(data)

    def _build_url(self, keyword: str) -> str:
        variables = {
            "first": 100,
            "after": "0",
            "sort": "score_desc",
            "term": keyword,
            "selectedFacets": [
                {"key": "region-id", "value": self.cfg["region_id"]},
                {
                    "key": "channel",
                    "value": json.dumps(
                        {
                            "salesChannel": self.cfg["sales_channel"],
                            "seller": self.cfg["seller"],
                            "regionId": self.cfg["region_id"],
                        }
                    ),
                },
                {"key": "locale", "value": self.cfg["locale"]},
            ],
        }

        params = {
            "operationName": self.cfg["operation"],
            "variables": json.dumps(variables, ensure_ascii=False),
        }
        return f"{self.cfg['search_url']}?{urlencode(params)}"

    def _parse_products(self, data: dict | None) -> list[dict]:
        if not data:
            return []

        products = []
        edges = (
            data.get("data", {}).get("search", {}).get("products", {}).get("edges", [])
        )

        for edge in edges:
            node = edge.get("node", {})
            breadcrumb = (node.get("breadcrumbList") or {}).get("itemListElement", [])
            category = breadcrumb[0].get("name") if len(breadcrumb) > 0 else None
            sub_category = breadcrumb[1].get("name") if len(breadcrumb) > 1 else None

            products.append(
                {
                    "store_id": self.store_id,
                    "sku": node.get("sku"),
                    "category": category,
                    "sub_category": sub_category,
                    "product_name": node.get("name"),
                    "brand_name": (node.get("brand") or {}).get("brandName"),
                    "high_price": (node.get("offers") or {}).get("highPrice"),
                    "low_price": (node.get("offers") or {}).get("lowPrice"),
                }
            )

        return products
