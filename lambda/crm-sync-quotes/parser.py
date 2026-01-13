from typing import List, Dict, Optional
from model import (
    Quote,
    Prospect,
    QuoteStatus,
    SalesRep,
    CustomerType,
    BaseProduct,
    Product,
)
from utils import extract_email, find_file
import logging
import tempfile
import zipfile
import csv
import os
from dbfread import DBF
from datetime import timedelta, datetime
from extractor import EmailExtractor

logger = logging.getLogger(__name__)

COTIZAC_FILENAME = "cotizac.DBF"
COTIZAD_FILENAME = "cotizad.DBF"
CLIENTES_FILENAME = "clientes.DBF"
PROSPECTS_FILENAME = "prospect.DBF"
SALES_REP_FILENAME = "sales_rep.csv"

STATUS_MAPPING = {
    "CANCELADA": QuoteStatus.CANCELLED,
    "PEDIDA": QuoteStatus.ORDERED,
    "EMITIDA": QuoteStatus.SENT,
}


class QuoteParser:
    def __init__(
        self, zip_file_path: str, sales_reps_path: str, products_path: str
    ) -> None:
        self.zip_file_path = zip_file_path
        self.sales_reps: Dict[str, SalesRep] = self._load_sales_reps(sales_reps_path)
        self.products_map: Dict[str, BaseProduct] = self._parse_products_from_csv(
            products_path
        )
        self.email_extractor = EmailExtractor()

    def _load_sales_reps(self, sales_reps_path: str) -> Dict[str, SalesRep]:
        sales_reps: Dict[str, SalesRep] = {}
        assets_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), sales_reps_path
        )
        if not os.path.exists(assets_path):
            logger.warning("Sales rep CSV not found at %s", assets_path)
            return sales_reps
        with open(assets_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                rep_id = str(row.get("AGENTE", "")).strip()
                if not rep_id:
                    continue
                sales_reps[rep_id] = SalesRep(
                    id=rep_id,
                    name=str(row.get("NOMBRE", "")).strip(),
                    email=str(row.get("EMAIL", "")).strip(),
                    phone_number=str(row.get("TEL", "")).strip(),
                )
        return sales_reps

    def read_quotes_from_zip(self) -> list[Quote]:
        """Read quotes from a ZIP file containing DBF files."""
        quotes: List[Quote] = []
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(self.zip_file_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)
            cotizac_path = find_file(temp_dir, COTIZAC_FILENAME)
            cotizad_path = find_file(temp_dir, COTIZAD_FILENAME)
            clientes_path = find_file(temp_dir, CLIENTES_FILENAME)
            prospects_path = find_file(temp_dir, PROSPECTS_FILENAME)

            if not all([cotizac_path, cotizad_path, clientes_path, prospects_path]):
                logger.error("Required DBF files are missing in the ZIP archive.")
                return []
            cotizac_records = list(
                DBF(cotizac_path, encoding="latin1", ignore_missing_memofile=True)
            )
            cotizad_records = list(
                DBF(cotizad_path, encoding="latin1", ignore_missing_memofile=True)
            )

            clientes_dict: Dict[str, Dict] = {}
            if clientes_path:
                clientes_records = list(
                    DBF(clientes_path, encoding="latin1", ignore_missing_memofile=True)
                )
                clientes_dict = {rec["CVE_CTE"]: rec for rec in clientes_records}

            prospects_dict: Dict[str, Dict] = {}
            if prospects_path:
                prospects_records = list(
                    DBF(prospects_path, encoding="latin1", ignore_missing_memofile=True)
                )
                prospects_dict = {rec["CVE_PROS"]: rec for rec in prospects_records}
            products_by_quote = self._group_products_by_quote(cotizad_records)
            for cotizac_rec in cotizac_records:
                try:
                    quote = self._parse_quote(
                        cotizac_rec,
                        products_by_quote,
                        clientes_dict,
                        prospects_dict,
                    )
                    if quote:
                        quotes.append(quote)
                except Exception as e:
                    logger.error(
                        f"Error parsing quote record {cotizac_rec.get('NO_COT')}: {e}",
                        exc_info=True,
                    )
                    continue
        logger.info(f"Parsed {len(quotes)} quotes from ZIP file")
        return quotes

    def _group_products_by_quote(
        self, cotizad_records: list
    ) -> Dict[str, List[Product]]:
        products_by_quote: Dict[str, List[Product]] = {}

        for rec in cotizad_records:
            no_cot = rec.get("NO_COT")
            cve_prod = rec.get("CVE_PROD")
            if not no_cot or not cve_prod:
                continue

            quote_id = str(int(no_cot)).strip()
            product_id = str(cve_prod).strip()

            quantity = rec.get("CANT_PROD")
            price = rec.get("VALOR_PROD")

            if quantity is None or price is None:
                raise ValueError(
                    f"Invalid product data for quote {quote_id}, "
                    f"product {product_id}, missing quantity or price."
                )

            vat_perc = rec.get("IVA_PROD") or 0
            discount_1 = rec.get("DCTO1") or 0
            discount_2 = rec.get("DCTO2") or 0

            base_product = self.products_map.get(
                product_id, BaseProduct.get_empty(product_id)
            )

            line_subtotal = quantity * price

            d1 = float(discount_1) / 100.0
            d2 = float(discount_2) / 100.0
            discounted_subtotal = line_subtotal * (1.0 - d1) * (1.0 - d2)

            vat_rate = float(vat_perc) / 100.0
            vat_amount = discounted_subtotal * vat_rate

            total_price = round(discounted_subtotal + vat_amount, 2)

            product = Product(
                product_id=product_id,
                description=base_product.description,
                product_type=base_product.product_type,
                quantity=quantity,
                price=price,
                vat_perc=vat_perc,
                vat=round(vat_amount, 2),
                discount_1=discount_1,
                discount_2=discount_2,
                total_price=total_price,
            )

            products_by_quote.setdefault(quote_id, []).append(product)

        return products_by_quote

    def _parse_prospect_from_prospect_dbf(
        self, prospect_rec: Dict
    ) -> Optional[Prospect]:
        """Parse a prospect record into a Prospect object."""
        cve_pros = prospect_rec.get("CVE_PROS")
        nom_pros = prospect_rec.get("NOM_PROS", "").strip()
        email_pros = prospect_rec.get("EMAIL_PROS", "").strip()
        email = self.email_extractor.extract_first_or_empty(email_pros)
        if not email:
            return None
        return Prospect(id=str(cve_pros).strip(), name=nom_pros, email=email)

    def _parse_prospect_from_cliente_dbf(self, client_rec: Dict) -> Optional[Prospect]:
        """Parse a client record into a Prospect object."""
        cve_cte = client_rec.get("CVE_CTE")
        nom_cte = client_rec.get("NOM_CTE", "").strip()
        email_cte = client_rec.get("EMAIL_CTE", "").strip()
        email = self.email_extractor.extract_first_or_empty(email_cte)
        if not email:
            return None
        return Prospect(id=str(cve_cte).strip(), name=nom_cte, email=email)

    def _map_status(self, status_str: str) -> QuoteStatus:
        """Map the status string from DBF to QuoteStatus enum value."""
        status_upper = status_str.strip().upper()
        return STATUS_MAPPING.get(status_upper, QuoteStatus.SENT)

    def _map_customer_type(self, tipo_cte: str) -> Optional[CustomerType]:
        t = (tipo_cte or "").strip().upper()
        if t == CustomerType.PROSPECT.value:
            return CustomerType.PROSPECT
        if t == CustomerType.CLIENT.value:
            return CustomerType.CLIENT
        return None

    def _parse_products_from_csv(self, file_path: str) -> Dict[str, BaseProduct]:
        """
        Reads products from a CSV file.
        """
        products = {}
        with open(file_path, mode="r", encoding="latin-1") as csvfile:
            reader = csv.reader(csvfile)
            header_found = False
            for row in reader:
                if not row:
                    continue
                if len(row) > 3 and row[3] == "Clave":
                    header_found = True
                    break

            if not header_found:
                return {}
            for row in reader:
                if not row:
                    continue
                if len(row) <= 14:
                    continue

                id_ = row[3].strip()
                if not id_:
                    continue

                description = row[4].strip()
                product_type = row[14].strip()
                products[id_] = BaseProduct(
                    product_id=id_, description=description, product_type=product_type
                )

        return products

    def _parse_quote(
        self,
        cotizac_rec: Dict,
        products_by_quote: Dict[str, List[Product]],
        clientes_dict: Dict,
        prospects_dict: Dict,
    ) -> Optional[Quote]:
        """Parse a single quote record into a Quote object."""
        no_cot = str(cotizac_rec.get("NO_COT"))
        cve_cte = cotizac_rec.get("CVE_CTE")
        tipo_cte = cotizac_rec.get("TIPO_CTE", "").strip().upper()
        customer_type = self._map_customer_type(tipo_cte)
        if not customer_type:
            logger.debug(f"Skipping quote {no_cot}: Unknown customer type {tipo_cte}")
            return None
        cve_age = str(cotizac_rec.get("CVE_AGE", "")).strip()
        total_cot = cotizac_rec.get("TOTAL_COT")
        status_str = cotizac_rec.get("STATUS", "").strip().upper()
        f_alta_cot = (cotizac_rec.get("F_ALTA_COT") or datetime.now()) - timedelta(
            days=1
        )
        prospect = None
        if tipo_cte == "P":
            prospect_rec = prospects_dict.get(cve_cte)
            if prospect_rec:
                prospect = self._parse_prospect_from_prospect_dbf(prospect_rec)
        elif tipo_cte == "C":
            cliente_rec = clientes_dict.get(cve_cte)
            if cliente_rec:
                prospect = self._parse_prospect_from_cliente_dbf(cliente_rec)

        if not prospect:
            logger.debug(f"Skipping quote {no_cot}: No prospect information found")
            return None
        status = self._map_status(status_str)
        created_at = f_alta_cot
        sales_rep = self.sales_reps.get(cve_age)
        if not sales_rep:
            logger.debug("Sales rep %s not found in CSV; using empty details", cve_age)
            sales_rep = SalesRep(id=cve_age, name="", email="", phone_number="")
        return Quote(
            id=no_cot,
            customer_type=customer_type,
            prospect=prospect,
            sales_rep=sales_rep,
            products=products_by_quote.get(no_cot, []),
            amount=float(total_cot) if total_cot is not None else 0.0,
            status=status,
            created_at=str(created_at),
        )
