import unittest

from extractor import EmailExtractor


class TestEmailExtractor(unittest.TestCase):
    def setUp(self) -> None:
        self.ex = EmailExtractor()

    def test_single_email_with_newlines(self) -> None:
        self.assertEqual(
            self.ex.extract_first("alpha.user@example.com\n"), "alpha.user@example.com"
        )
        self.assertEqual(
            self.ex.extract_first("\n  beta_team@sample.org  \n"),
            "beta_team@sample.org",
        )

    def test_multiple_emails_mixed_separators(self) -> None:
        self.assertEqual(
            self.ex.extract_first("ops@acme.mx;billing@acme.mx"), "ops@acme.mx"
        )
        self.assertEqual(
            self.ex.extract_first("sales@company.com, ceo@company.com,"),
            "sales@company.com",
        )
        self.assertEqual(
            self.ex.extract_first("service_dept@domain.net; alt.contact@domain.net"),
            "service_dept@domain.net",
        )
        self.assertEqual(
            self.ex.extract_first("admin@subco.io, support@subco.io"), "admin@subco.io"
        )
        self.assertEqual(self.ex.extract_first("a@x.co|b@y.co"), "a@x.co")

    def test_weird_spacing_between_emails(self) -> None:
        self.assertEqual(
            self.ex.extract_first("first.last@foo.com,  second.person@bar.com"),
            "first.last@foo.com",
        )

    def test_uppercase_domain_is_lowercased_local_preserved(self) -> None:
        self.assertEqual(
            self.ex.extract_first("NOTIFY@EXAMPLE.COM"), "NOTIFY@example.com"
        )
        self.assertEqual(
            self.ex.extract_first("MiXeDCaSe@EXAMPLE.COM"), "MiXeDCaSe@example.com"
        )
        self.assertEqual(
            self.ex.extract_first("TEAM@MY-DOMAIN.ORG"), "TEAM@my-domain.org"
        )

    def test_name_angle_brackets(self) -> None:
        self.assertEqual(
            self.ex.extract_first("FAKECO <person@fakeco.com>; other@x.com"),
            "person@fakeco.com",
        )

    def test_trailing_punctuation_and_embedded_text(self) -> None:
        self.assertEqual(self.ex.extract_first("person@fake.com, "), "person@fake.com")
        self.assertEqual(
            self.ex.extract_first("hello.world@fake.net. second@fake.net"),
            "hello.world@fake.net",
        )
        self.assertEqual(
            self.ex.extract_first("front@fake.org, notes: back@fake.org"),
            "front@fake.org",
        )

    def test_accents_removed_from_local_part(self) -> None:
        self.assertEqual(self.ex.extract_first("Almacén2@fake.mx"), "Almacen2@fake.mx")
        self.assertEqual(
            self.ex.extract_first("facturación@fake.com"), "facturacion@fake.com"
        )
        self.assertEqual(
            self.ex.extract_first("Lalo_garcía-i@fakemail.com"),
            "Lalo_garcia-i@fakemail.com",
        )

    def test_edge_case_accent_with_trailing_comma(self) -> None:
        self.assertEqual(self.ex.extract_first("Almacén2@fake.mx,"), "Almacen2@fake.mx")
        self.assertEqual(
            self.ex.extract_first("facturación@fake.com,"), "facturacion@fake.com"
        )

    def test_control_chars_and_zero_width_removed(self) -> None:
        self.assertEqual(
            self.ex.extract_first("ventas\u00a0@\nfake.mx"), "ventas@fake.mx"
        )
        self.assertEqual(
            self.ex.extract_first("  Almac\u200bén2@fake.mx  "), "Almacen2@fake.mx"
        )

    def test_garbage_tokens_then_valid_email(self) -> None:
        self.assertEqual(
            self.ex.extract_first("AUTOSOMETHING user@fake.com"), "user@fake.com"
        )
        self.assertEqual(
            self.ex.extract_first("carlos, legit@fake.com"), "legit@fake.com"
        )
        self.assertEqual(self.ex.extract_first("1; ok@fake.com"), "ok@fake.com")
        self.assertEqual(self.ex.extract_first("SN notify@fake.com"), "notify@fake.com")

    def test_invalid_emails_return_none(self) -> None:
        invalid_inputs = [
            "",
            "   ",
            "carlos",
            "todomaquinaria",
            "pending",
            "AUTOSOMETHING",
            "user@domain",  # missing TLD
            "invoice@corp.c",  # 1-char TLD
            "x@y",  # too short
            "@example.com",  # missing local
            "name <notanemail>",  # parseaddr yields empty
            "foo@@bar.com",
        ]
        for raw in invalid_inputs:
            with self.subTest(raw=raw):
                self.assertIsNone(self.ex.extract_first(raw))

    def test_prefers_first_valid_when_first_candidate_invalid(self) -> None:
        self.assertEqual(
            self.ex.extract_first("invoice@corp.c, alpha.user@example.com"),
            "alpha.user@example.com",
        )

    def test_keeps_plus_and_underscore(self) -> None:
        self.assertEqual(
            self.ex.extract_first("tag+name_test@fake.com, other@x.com"),
            "tag+name_test@fake.com",
        )

    def test_edge_case_trailing_comma_after_email(self) -> None:
        # Explicit edge case: "email," should still be recognized as the email
        self.assertEqual(self.ex.extract_first("someone@fake.com,"), "someone@fake.com")
        # And still works when followed by more text
        self.assertEqual(
            self.ex.extract_first("someone@fake.com, extra words here"),
            "someone@fake.com",
        )


if __name__ == "__main__":
    unittest.main()
