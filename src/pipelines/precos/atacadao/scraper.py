import json
from urllib.parse import urlencode

from core.http import HttpClient


class AtacadaoScraper:
    def __init__(self, store_cfg: dict, extractor: HttpClient):
        self.store_id = store_cfg["id"]
        self.cfg = store_cfg
        self.extractor = extractor

    # Páginas de 100 itens; a API usa cursor numérico em ``after``.
    PAGE_SIZE = 100
    PAGES = 2

    def search(self, keyword: str) -> list[dict]:
        """Busca até ``PAGES`` páginas da keyword, deduplicando por SKU."""
        all_products: list[dict] = []
        seen_skus: set[str] = set()

        for page in range(self.PAGES):
            url = self._build_url(keyword, after=page * self.PAGE_SIZE)
            data = self.extractor.make_request(url=url, mode="json")
            products = self._parse_products(data)
            if not products:
                break

            for product in products:
                sku = product.get("sku")
                if not sku or sku in seen_skus:
                    continue
                seen_skus.add(sku)
                all_products.append(product)

        return all_products

    def _build_url(self, keyword: str, after: int = 0) -> str:
        variables = {
            "first": self.PAGE_SIZE,
            "after": str(after),
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
        # ``or {}`` em cada nível: a API devolve null (não ausência) para campos vazios.
        search = (data.get("data") or {}).get("search") or {}
        edges = (search.get("products") or {}).get("edges") or []

        for edge in edges:
            node = edge.get("node") or {}
            breadcrumb = (node.get("breadcrumbList") or {}).get("itemListElement") or []
            category = (
                (breadcrumb[0] or {}).get("name") if len(breadcrumb) > 0 else None
            )
            sub_category = (
                (breadcrumb[1] or {}).get("name") if len(breadcrumb) > 1 else None
            )

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
