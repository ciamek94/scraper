import unittest
import pandas as pd

from main import (
    merge_excel_records,
    normalize_price,
    should_fetch_listing_details,
    upsert_listing,
)


class NormalizePriceTests(unittest.TestCase):
    def test_parses_prices_with_nonbreaking_spaces(self):
        self.assertEqual(normalize_price("1\u00a0500 zł"), 1500)
        self.assertEqual(normalize_price("1\u202f500 zł"), 1500)


class UpsertListingTests(unittest.TestCase):
    def test_updates_existing_record_without_duplicate_or_field_loss(self):
        existing = {
            "Link": "https://www.olx.pl/d/oferta/item/",
            "NormLink": "https://www.olx.pl/d/oferta/item",
            "Price": "250 zł",
            "Notified": True,
            "CustomField": "preserve me",
        }
        records = [existing]

        updated = upsert_listing(
            records,
            "https://www.olx.pl/d/oferta/item",
            {"Price": "275 zł", "Notified": False},
        )

        self.assertIs(updated, existing)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["Price"], "275 zł")
        self.assertFalse(records[0]["Notified"])
        self.assertEqual(records[0]["CustomField"], "preserve me")

    def test_new_record_is_appended_without_changing_existing_records(self):
        existing = {"NormLink": "https://www.olx.pl/d/oferta/existing"}
        records = [existing]
        new_record = {"Link": "https://www.olx.pl/d/oferta/new", "Title": "New"}

        added = upsert_listing(
            records,
            "https://www.olx.pl/d/oferta/new",
            new_record,
        )

        self.assertIs(added, new_record)
        self.assertEqual(records, [existing, new_record])

    def test_moves_record_between_lists_without_losing_extra_fields(self):
        rejected = [{
            "NormLink": "https://www.olx.pl/d/oferta/item",
            "Reason": "old classification",
        }]
        accepted = []

        moved = upsert_listing(
            accepted,
            "https://www.olx.pl/d/oferta/item",
            {"Title": "Now accepted"},
            rejected,
        )

        self.assertEqual(accepted, [{
            "NormLink": "https://www.olx.pl/d/oferta/item",
            "Reason": "old classification",
            "Title": "Now accepted",
        }])
        self.assertEqual(rejected, [])
        self.assertIs(moved, accepted[0])


class ExcelRecordMapTests(unittest.TestCase):
    def test_excel_records_are_merged_without_overwriting_json_for_each_list(self):
        accepted_excel = pd.DataFrame([
            {
                "Link": "https://www.olx.pl/d/oferta/existing?search_reason=organic",
                "Price": "90 zł",
            },
            {
                "Link": "https://www.olx.pl/d/oferta/from-accepted-excel?search_reason=organic",
                "Price": "200 zł",
            },
            {"Link": None, "Price": "300 zł"},
        ])
        rejected_excel = pd.DataFrame([
            {
                "Link": "https://www.olx.pl/d/oferta/from-rejected-excel?search_reason=organic",
                "Price": "50 zł",
            },
        ])

        accepted = [{
            "Link": "https://www.olx.pl/d/oferta/existing",
            "NormLink": "https://www.olx.pl/d/oferta/existing",
            "Price": "100 zł",
        }]
        rejected = [{
            "Link": "https://www.olx.pl/d/oferta/rejected",
            "NormLink": "https://www.olx.pl/d/oferta/rejected",
            "Price": "75 zł",
        }]

        merge_excel_records(accepted, accepted_excel)
        merge_excel_records(rejected, rejected_excel)

        self.assertEqual(len(accepted), 2)
        self.assertEqual(accepted[0]["Price"], "100 zł")
        self.assertEqual(
            accepted[1]["NormLink"],
            "https://www.olx.pl/d/oferta/from-accepted-excel",
        )
        self.assertEqual(len(rejected), 2)
        self.assertEqual(
            rejected[1]["NormLink"],
            "https://www.olx.pl/d/oferta/from-rejected-excel",
        )


class ListingDetailsDecisionTests(unittest.TestCase):
    def test_rejected_listings_are_skipped_regardless_of_price(self):
        link = "https://www.olx.pl/d/oferta/rejected"
        rejected_map = {link: {"Price": "50 zł"}}

        self.assertFalse(should_fetch_listing_details(link, "50 zł", {}, rejected_map))
        self.assertFalse(should_fetch_listing_details(link, "75 zł", {}, rejected_map))

    def test_accepted_listing_is_skipped_only_when_price_is_unchanged(self):
        link = "https://www.olx.pl/d/oferta/accepted"
        accepted_map = {link: {"Price": "100 zł"}}

        self.assertFalse(should_fetch_listing_details(link, "100 zł", accepted_map, {}))
        self.assertTrue(should_fetch_listing_details(link, "125 zł", accepted_map, {}))
        self.assertTrue(should_fetch_listing_details("https://www.olx.pl/d/oferta/new", "50 zł", accepted_map, {}))


if __name__ == "__main__":
    unittest.main()