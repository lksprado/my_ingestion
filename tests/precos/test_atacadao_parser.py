from pipelines.precos.atacadao.scraper import AtacadaoScraper

STORE = {
    "id": 1,
    "region_id": "r",
    "sales_channel": "1",
    "seller": "s",
    "locale": "pt-BR",
    "search_url": "https://x/graphql",
    "operation": "Search",
}


def _node(sku, breadcrumb=None):
    return {
        "node": {
            "sku": sku,
            "name": f"p{sku}",
            "breadcrumbList": breadcrumb,
            "brand": None,
            "offers": {"highPrice": 2.0, "lowPrice": 1.0},
        }
    }


class FakeHttp:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def make_request(self, url, mode="json"):
        self.calls.append(url)
        return (
            self.pages[len(self.calls) - 1]
            if len(self.calls) <= len(self.pages)
            else None
        )


def test_parse_tolerates_nulls():
    scraper = AtacadaoScraper(STORE, FakeHttp([]))
    assert scraper._parse_products({"data": {"search": None}}) == []
    products = scraper._parse_products(
        {"data": {"search": {"products": {"edges": [_node("a", None)]}}}}
    )
    assert products[0]["category"] is None
    assert products[0]["brand_name"] is None


def test_search_paginates_and_dedups_by_sku():
    page1 = {"data": {"search": {"products": {"edges": [_node("a"), _node("b")]}}}}
    page2 = {"data": {"search": {"products": {"edges": [_node("b"), _node("c")]}}}}
    http = FakeHttp([page1, page2])
    products = AtacadaoScraper(STORE, http).search("arroz")

    assert [p["sku"] for p in products] == ["a", "b", "c"]
    assert len(http.calls) == 2
    assert "%22after%22%3A+%22100%22" in http.calls[1]


def test_search_stops_on_empty_page():
    page1 = {"data": {"search": {"products": {"edges": [_node("a")]}}}}
    http = FakeHttp([page1, {"data": None}])
    assert len(AtacadaoScraper(STORE, http).search("x")) == 1
