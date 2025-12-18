from typing import List, Dict, Optional
from model import Quote, Prospect, QuoteStatus, SalesRep
from utils import extract_email, find_file
import logging
import tempfile
import zipfile
import csv
import os
from dbfread import DBF
from datetime import timedelta, datetime

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
    def __init__(self, zip_file_path: str, sales_reps_path: str) -> None:
        self.zip_file_path = zip_file_path
        self.sales_reps: Dict[str, SalesRep] = self._load_sales_reps(sales_reps_path)

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
            items_by_quote = self._group_items_by_quote(cotizad_records)
            for cotizac_rec in cotizac_records:
                try:
                    quote = self._parse_quote(
                        cotizac_rec,
                        items_by_quote,
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

    def _group_items_by_quote(self, cotizad_records: list) -> Dict[str, List[str]]:
        items_by_quote: Dict[str, List[str]] = {}
        for rec in cotizad_records:
            no_cot = rec.get("NO_COT")
            cve_prod = rec.get("CVE_PROD")
            if no_cot is not None and cve_prod:
                quote_keys = str(int(no_cot)).strip()
                if no_cot not in items_by_quote:
                    items_by_quote[quote_keys] = []
                items_by_quote[quote_keys].append(str(cve_prod).strip())
        return items_by_quote

    def _parse_prospect_from_prospect_dbf(
        self, prospect_rec: Dict
    ) -> Optional[Prospect]:
        """Parse a prospect record into a Prospect object."""
        cve_pros = prospect_rec.get("CVE_PROS")
        nom_pros = prospect_rec.get("NOM_PROS", "").strip()
        email_pros = prospect_rec.get("EMAIL_PROS", "").strip()
        email = extract_email(email_pros)
        if not email:
            return None
        return Prospect(id=str(cve_pros).strip(), name=nom_pros, email=email)

    def _parse_prospect_from_cliente_dbf(self, client_rec: Dict) -> Optional[Prospect]:
        """Parse a client record into a Prospect object."""
        cve_cte = client_rec.get("CVE_CTE")
        nom_cte = client_rec.get("NOM_CTE", "").strip()
        email_cte = client_rec.get("EMAIL_CTE", "").strip()
        email = extract_email(email_cte)
        if not email:
            return None
        return Prospect(id=str(cve_cte).strip(), name=nom_cte, email=email)

    def _map_status(self, status_str: str) -> QuoteStatus:
        """Map the status string from DBF to QuoteStatus enum value."""
        status_upper = status_str.strip().upper()
        return STATUS_MAPPING.get(status_upper, QuoteStatus.SENT)

    def _parse_quote(
        self,
        cotizac_rec: Dict,
        items_by_quote: Dict[str, List[str]],
        clientes_dict: Dict,
        prospects_dict: Dict,
    ) -> Optional[Quote]:
        """Parse a single quote record into a Quote object."""
        no_cot = str(cotizac_rec.get("NO_COT"))
        cve_cte = cotizac_rec.get("CVE_CTE")
        tipo_cte = cotizac_rec.get("TIPO_CTE", "").strip().upper()
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
        item_ids = items_by_quote.get(no_cot, [])
        status = self._map_status(status_str)
        created_at = f_alta_cot
        sales_rep = self.sales_reps.get(cve_age)
        if not sales_rep:
            logger.debug("Sales rep %s not found in CSV; using empty details", cve_age)
            sales_rep = SalesRep(id=cve_age, name="", email="", phone_number="")
        return Quote(
            id=no_cot,
            prospect=prospect,
            sales_rep=sales_rep,
            item_ids=item_ids,
            amount=float(total_cot) if total_cot is not None else 0.0,
            status=status,
            created_at=str(created_at),
        )
