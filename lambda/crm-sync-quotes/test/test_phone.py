import unittest

from phone import normalize_to_e164


class TestNormalizeToE164(unittest.TestCase):
    def test_movil_with_ladam_happy_path(self):
        # MOVIL 10 digits, no LADAM needed
        self.assertEqual(
            normalize_to_e164("8112345678", "", "", "", "", ""),
            "+528112345678",
        )

    def test_movil_ten_digits_with_populated_ladam(self):
        # Regression: 10-digit MOVIL with a non-empty LADAM should NOT be
        # concatenated. The number is already complete; LADAM is ignored.
        self.assertEqual(
            normalize_to_e164("8112345678", "811", "", "", "", ""),
            "+528112345678",
        )

    def test_movil_with_short_movil_and_ladam(self):
        # 7-digit MOVIL + 3-digit LADAM => 10-digit national => +52 prefix
        self.assertEqual(
            normalize_to_e164("1234567", "811", "", "", "", ""),
            "+528111234567",
        )

    def test_movil_already_country_prefixed(self):
        self.assertEqual(
            normalize_to_e164("528112345678", "", "", "", "", ""),
            "+528112345678",
        )

    def test_area_code_plus_number_already_qualified(self):
        # area_code='52811' + number='2345678' => combined='528112345678' => +528112345678
        self.assertEqual(
            normalize_to_e164("2345678", "52811", "", "", "", ""),
            "+528112345678",
        )

    def test_movil_with_punctuation_stripped(self):
        self.assertEqual(
            normalize_to_e164("(811) 234-5678", "", "", "", "", ""),
            "+528112345678",
        )

    def test_falls_through_to_tel1_when_movil_empty(self):
        self.assertEqual(
            normalize_to_e164("", "", "1234567", "", "", "811"),
            "+528111234567",
        )

    def test_falls_through_to_tel2_when_tel1_unrecoverable(self):
        # TEL1 missing LADA -> rejected; TEL2 has its own LADA pair
        self.assertEqual(
            normalize_to_e164("", "", "1234567", "8112345678", "", ""),
            "+528112345678",
        )

    def test_falls_through_to_tel3(self):
        self.assertEqual(
            normalize_to_e164("", "", "", "", "8112345678", ""),
            "+528112345678",
        )

    def test_seven_digit_landline_without_lada_returns_none(self):
        # The "5121-855" sample from the design doc - rejected, not guessed
        self.assertIsNone(normalize_to_e164("", "", "5121855", "", "", ""))

    def test_all_empty_returns_none(self):
        self.assertIsNone(normalize_to_e164("", "", "", "", "", ""))

    def test_leading_zeros_stripped(self):
        self.assertEqual(
            normalize_to_e164("08112345678", "", "", "", "", ""),
            "+528112345678",
        )

    def test_non_digit_garbage_returns_none(self):
        self.assertIsNone(normalize_to_e164("abc---", "", "", "", "", ""))

    def test_too_few_digits_after_concat_returns_none(self):
        # 4 digits total - cannot be a valid Mexican mobile
        self.assertIsNone(normalize_to_e164("1234", "", "", "", "", ""))

    def test_too_many_digits_returns_none(self):
        # 15 digits is out of range
        self.assertIsNone(normalize_to_e164("123456789012345", "", "", "", "", ""))

    def test_custom_default_country_code(self):
        self.assertEqual(
            normalize_to_e164(
                "5551234567", "", "", "", "", "", default_country_code="1"
            ),
            "+15551234567",
        )


if __name__ == "__main__":
    unittest.main()
