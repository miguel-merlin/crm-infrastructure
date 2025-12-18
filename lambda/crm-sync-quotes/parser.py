from typing import List, Dict, Optional
from model import Quote, Prospect, QuoteStatus
from utils import extract_email, find_file
import logging
import tempfile
import zipfile
from dbfread import DBF

logger = logging.getLogger(__name__)

COTIZAC_FILENAME = "cotizac.DBF"
COTIZAD_FILENAME = "cotizad.DBF"
CLIENTES_FILENAME = "clientes.DBF"
PROSPECTS_FILENAME = "prospect.DBF"

STATUS_MAPPING = {
    "CANCELADA": QuoteStatus.CANCELLED,
    "PEDIDA": QuoteStatus.ORDERED,
    "EMITIDA": QuoteStatus.SENT,
}


class QuoteParser:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def read_quotes_from_zip(self) -> list[Quote]:
        """Read quotes from a ZIP file containing DBF files."""
        quotes: List[Quote] = []
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(self.file_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)
            cotizac_path = find_file(temp_dir, COTIZAC_FILENAME)
            cotizad_path = find_file(temp_dir, COTIZAD_FILENAME)
            clientes_path = find_file(temp_dir, CLIENTES_FILENAME)
            prospects_path = find_file(temp_dir, PROSPECTS_FILENAME)

            if not cotizac_path or not cotizad_path:
                logger.error("Required DBF files are missing in the ZIP archive.")
                return []
            cotizac_records = list(DBF(cotizac_path, encoding="latin1"))
            cotizad_records = list(DBF(cotizad_path, encoding="latin1"))

            clientes_dict: Dict[str, Dict] = {}
            if clientes_path:
                clientes_records = list(DBF(clientes_path, encoding="latin1"))
                clientes_dict = {rec["CVE_CTE"]: rec for rec in clientes_records}

            prospects_dict: Dict[str, Dict] = {}
            if prospects_path:
                prospects_records = list(DBF(prospects_path, encoding="latin1"))
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
                if no_cot not in items_by_quote:
                    items_by_quote[no_cot] = []
                items_by_quote[no_cot].append(str(cve_prod).strip())
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
        cve_age = cotizac_rec.get("CVE_AGE", "").strip()
        total_cot = cotizac_rec.get("TOTAL_COT")
        status_str = cotizac_rec.get("STATUS_COT", "").strip().upper()
        f_alta_cot = cotizac_rec.get("F_ALTA_COT")

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
        created_at = str(f_alta_cot).strip() if f_alta_cot else ""
        return Quote(
            id=no_cot,
            prospect=prospect,
            sales_rep_id=cve_age,
            item_ids=item_ids,
            amount=float(total_cot) if total_cot is not None else 0.0,
            status=status,
            created_at=created_at,
        )
