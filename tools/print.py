"""Utilities for generating and printing invoices from the Kthimi database.

Upgrades applied (no env required):
- Concurrency-safe claiming (0→2 Processing → 1 Printed / revert to 0 on failure)
- Decimal-based money math (no float drift)
- Email sending with retry/backoff
- Printer IP discovery cache with TTL (avoid repeated subnet scans)
- Rotating file logs
"""

import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyodbc
import pdfkit
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import socket
import os
import time
import qrcode
import json
import zipfile
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
import base64
import logging
from logging.handlers import RotatingFileHandler
import re
import smtplib
from decimal import Decimal, ROUND_HALF_UP, getcontext
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from qrcode.constants import ERROR_CORRECT_M
import zlib

# ------------------------------- CONFIG (no env) ---------------------------------
config = pdfkit.configuration(
    wkhtmltopdf=r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
)

DB2_CONFIG = {
    "driver": "{ODBC Driver 17 for SQL Server}",
    "server": "192.168.100.35",
    "database": "Printimi",
    "uid": "user1",
    "pwd": "user1.",
}

LOG_FILE = "kthimi_invoices.log"
POLL_SECONDS = 10
MAX_WORKERS = 5
PRINTER_PORT = 9100
PRINTER_SCAN_TIMEOUT = 2
PRINTER_CACHE_TTL_SEC = 30 * 60  # 30 minutes

# ------------------------------- LOGGING -----------------------------------------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
_rot = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
_rot.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.getLogger().addHandler(_rot)

# ------------------------------- HELPERS -----------------------------------------
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-']+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

getcontext().prec = 28  # high precision for intermediate money math


def D(x) -> Decimal:
    """Decimal ctor with safe fallback for None/empty."""
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x if x is not None else "0"))


def q2(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def q4(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def format_date(value, date_format="%Y-%m-%d"):
    if isinstance(value, datetime):
        return value.strftime(date_format)
    try:
        dt = datetime.strptime(value, "%Y-%m-%d")
        return dt.strftime(date_format)
    except Exception:
        return value


def create_connection(config_dict):
    conn_str = (
        f"DRIVER={config_dict['driver']};"
        f"SERVER={config_dict['server']};"
        f"DATABASE={config_dict['database']};"
        f"UID={config_dict['uid']};"
        f"PWD={config_dict['pwd']}"
    )
    return pyodbc.connect(conn_str)


def _parse_email_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    items = re.split(r"[;,]", raw)
    out, seen = [], set()
    for it in (x.strip() for x in items):
        key = it.lower()
        if it and EMAIL_REGEX.match(it) and key not in seen:
            out.append(it)
            seen.add(key)
    return out


def _s(x):
    return x.strip() if isinstance(x, str) else x


def _blank_if_none_or_none_text(val) -> str:
    """Empty for None/''/'none'/'n/a'/'null' (case-insensitive), else trimmed."""
    if val is None:
        return ""
    if isinstance(val, str):
        s = val.strip()
        return "" if s.lower() in {"", "none", "n/a", "null"} else s
    return str(val)


# ------------------------ CLAIM / FINALIZE (idempotency) -------------------------
def claim_invoices(batch_size: int = 20) -> list[int]:
    """Atomically move a batch from Pending(0) to Processing(2) and return IDs."""
    with create_connection(DB2_CONFIG) as conn:
        cur = conn.cursor()
        cur.execute("SET DEADLOCK_PRIORITY HIGH;")
        cur.execute(
            f"""
            UPDATE TOP ({batch_size}) dbo.Kthimi_InvoiceStatus WITH (ROWLOCK, READPAST, UPDLOCK)
            SET Printed = 2
            OUTPUT inserted.ID_Fatura
            WHERE Printed = 0 AND Status = 1;
            """
        )
        rows = cur.fetchall()
        conn.commit()
    ids = [int(r[0]) for r in rows]
    if ids:
        logging.info(f"Claimed {len(ids)} invoices: {ids[:5]}{'...' if len(ids)>5 else ''}")
    return ids


def finalize_invoice(id_fatura: int) -> None:
    with create_connection(DB2_CONFIG) as conn:
        cur = conn.cursor()
        cur.execute("SET DEADLOCK_PRIORITY HIGH;")
        cur.execute(
            "UPDATE dbo.Kthimi_InvoiceStatus WITH (ROWLOCK, UPDLOCK) "
            "SET Printed=1 WHERE ID_Fatura=? AND Printed=2;",
            (id_fatura,),
        )
        conn.commit()


def revert_invoice(id_fatura: int) -> None:
    with create_connection(DB2_CONFIG) as conn:
        cur = conn.cursor()
        cur.execute("SET DEADLOCK_PRIORITY HIGH;")
        cur.execute(
            "UPDATE dbo.Kthimi_InvoiceStatus WITH (ROWLOCK, UPDLOCK) "
            "SET Printed=0 WHERE ID_Fatura=? AND Printed=2;",
            (id_fatura,),
        )
        conn.commit()


# ------------------------------- DATA ACCESS -------------------------------------
def get_contacts_for_furnitor(furnitori_id: str) -> tuple[list[str], list[str]]:
    if not furnitori_id:
        return [], []
    try:
        conn = create_connection(DB2_CONFIG)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ToEmail, CcEmail
            FROM dbo.Kthimi_FurnitorContacts WITH (NOLOCK)
            WHERE FurnitoriID = ? AND IsActive = 1
        """,
            (furnitori_id,),
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return [], []
        return _parse_email_list(row.ToEmail), _parse_email_list(row.CcEmail)
    except Exception as e:
        logging.error(f"Contacts lookup failed for FurnitoriID={furnitori_id}: {e}")
        return [], []


def get_invoice_details(id_fatura):
    conn = create_connection(DB2_CONFIG)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT ID_Fatura, Emri_Furnitorit, Data, Njesia, Identify
        FROM Kthimi_FaturaTransfers
        WHERE ID_Fatura=?
    """,
        (id_fatura,),
    )
    fatura_row = cursor.fetchone()
    if not fatura_row:
        conn.close()
        return None

    cursor.execute(
        """
        SELECT TOP 1 ID_Dokument, Tipi_Dokument, NR_Rendor, NjesiaOrg, FurnitoriID, FurnitoriEmri, FurnitoriAdresa,
               FurnitoriQyteti, FurnitoriZipcode, FurnitoriSheti, FurnitoriDataA, FurnitoriKontakt, FurnitoriNui,
               FurnitoriEmriKontakt, Pranuesi, Njesia, Tekst1, Tekst2, NUII
        FROM Kthimi_DokumentTransfers
        WHERE ID_Fatura=?
    """,
        (id_fatura,),
    )
    dok_row = cursor.fetchone()

    dok_data = {}
    if dok_row:
        dok_data = {
            "ID_Dokument": dok_row.ID_Dokument,
            "Tipi_Dokument": dok_row.Tipi_Dokument,
            "NR_Rendor": dok_row.NR_Rendor,
            "NjesiaOrg": _s(dok_row.NjesiaOrg),
            "FurnitoriID": _s(dok_row.FurnitoriID),
            "FurnitoriEmri": _s(dok_row.FurnitoriEmri),
            "FurnitoriAdresa": _s(dok_row.FurnitoriAdresa),
            "FurnitoriQyteti": _s(dok_row.FurnitoriQyteti),
            "FurnitoriZipcode": _s(dok_row.FurnitoriZipcode),
            "FurnitoriSheti": _s(dok_row.FurnitoriSheti),
            "FurnitoriDataA": dok_row.FurnitoriDataA,
            "FurnitoriKontakt": _s(dok_row.FurnitoriKontakt),
            "FurnitoriNui": _s(dok_row.FurnitoriNui),
            "FurnitoriEmriKontakt": _s(dok_row.FurnitoriEmriKontakt),
            "Pranuesi": _s(dok_row.Pranuesi),
            "NjesiaDok": dok_row.Njesia,
            "Kom1": _s(dok_row.Tekst1),
            "Kom2": _s(dok_row.Tekst2),
            "NUII": _s(dok_row.NUII),
        }

    fatura_data = {
        "ID_Fatura": fatura_row.ID_Fatura,
        "Emri_Furnitorit": fatura_row.Emri_Furnitorit,
        "Data": fatura_row.Data,
        "NjesiaFatura": fatura_row.Njesia,
        "Identify": fatura_row.Identify,
    }

    conn.close()
    return {**fatura_data, **dok_data}


def get_invoice_products(id_fatura):
    conn = create_connection(DB2_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT EmertimiA, NjesiaM, ShifraF, Sasia, CmimiDokument, TaxRate, Zbritje
        FROM Kthimi_ProduktTransfers WHERE ID_Fatura=?
    """,
        (id_fatura,),
    )
    rows = cursor.fetchall()
    conn.close()

    items = []
    for row in rows:
        qty = D(row.Sasia)
        price = D(row.CmimiDokument)
        tax_rate = D(row.TaxRate)
        disc = D(row.Zbritje)

        discount_amount = price * (disc / D(100))
        price_after_discount = price - discount_amount
        line_total = qty * price_after_discount
        tax_amount = line_total * (tax_rate / D(100))
        grand_total = line_total + tax_amount

        items.append(
            {
                "EmertimiA": row.EmertimiA,
                "NjesiaM": row.NjesiaM,
                "ShifraF": row.ShifraF,
                "Sasia": f"{q2(qty):.2f}",
                "CmimiDokument": f"{q4(price):.4f}",
                "TaxRate": f"{q2(tax_rate):.2f}",
                "Zbritje": f"{q2(disc):.2f}",
                "tax_amount": f"{q4(tax_amount):.4f}",
                "line_total": f"{q4(grand_total):.4f}",
            }
        )
    return items


def map_data_for_template(invoice_data, items):
    invoice_number = invoice_data.get("ID_Fatura", "N/A")
    invoice_date = invoice_data.get(
        "FurnitoriDataA", invoice_data.get("Data", datetime.now().strftime("%Y-%m-%d"))
    )
    tipidokumentit = invoice_data.get("Tipi_Dokument", "N/A")
    njesia = invoice_data.get("NjesiaDok", invoice_data.get("NjesiaFatura", "N/A"))
    viva_furnitori = invoice_data.get("FurnitoriEmri", invoice_data.get("Emri_Furnitorit", "N/A"))
    viva_pranuesi = invoice_data.get("Pranuesi", "N/A")
    viva_shifrafurnitorit = invoice_data.get("FurnitoriID", "N/A")
    currency = "EUR"

    total_quantity = sum(D(i["Sasia"]) for i in items)
    subtotal = sum(D(i["line_total"]) - D(i["tax_amount"]) for i in items)
    total_tax = sum(D(i["tax_amount"]) for i in items)
    grand_total = subtotal + total_tax

    return {
        "invoice_number": invoice_number,
        "invoice_date": invoice_date,
        "njesia": njesia,
        "tipidokumentit": tipidokumentit,
        "viva_pranuesi": viva_pranuesi,
        "viva_furnitori": viva_furnitori,
        "viva_shifrafurnitorit": viva_shifrafurnitorit,
        "currency": currency,
        "items": items,
        "total_quantity": f"{q2(total_quantity):.2f}",
        "subtotal": f"{q4(subtotal):.4f}",
        "total_tax": f"{q4(total_tax):.4f}",
        "grand_total": f"{q4(grand_total):.4f}",
        "FurnitoriID": invoice_data.get("FurnitoriID", "N/A"),
        "FurnitoriEmri": invoice_data.get("FurnitoriEmri", "N/A"),
        "FurnitoriAdresa": invoice_data.get("FurnitoriAdresa", "N/A"),
        "FurnitoriQyteti": invoice_data.get("FurnitoriQyteti", "N/A"),
        "FurnitoriZipcode": invoice_data.get("FurnitoriZipcode", "N/A"),
        "FurnitoriSheti": invoice_data.get("FurnitoriSheti", "N/A"),
        "FurnitoriDataA": invoice_data.get("FurnitoriDataA", "N/A"),
        "FurnitoriKontakt": invoice_data.get("FurnitoriKontakt", "N/A"),
        "FurnitoriNui": invoice_data.get("FurnitoriNui", "N/A"),
        "FurnitoriEmriKontakt": invoice_data.get("FurnitoriEmriKontakt", "N/A"),
        "Kom1": _blank_if_none_or_none_text(invoice_data.get("Kom1")),
        "Kom2": _blank_if_none_or_none_text(invoice_data.get("Kom2")),
        "NUII": invoice_data.get("NUII", "N/A"),
        "Identify": invoice_data.get("Identify", "N/A"),
    }


def sanitize_filename_component(component):
    import re as _re

    return _re.sub(r"[^A-Za-z0-9\-_.]", "_", str(component))


def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


# ----------------------------- QR (encrypt & save) -------------------------------
def create_encrypted_qr(data, password, qr_dir="FATURA_template/qr_codes"):
    try:
        if not os.path.exists(qr_dir):
            os.makedirs(qr_dir)
            logging.info(f"Created QR code directory at {qr_dir}")

        if isinstance(data.get("invoice_date"), datetime):
            data["invoice_date"] = data["invoice_date"].strftime("%Y-%m-%d %H:%M:%S")

        raw_json = json.dumps(
            data, default=default_serializer, separators=(",", ":")
        ).encode("utf-8")
        compressed = zlib.compress(raw_json)

        salt = os.urandom(16)
        kdf = Scrypt(salt=salt, length=32, n=2**14, r=8, p=1, backend=default_backend())
        key = kdf.derive(password.encode("utf-8"))
        fernet_key = Fernet(base64.urlsafe_b64encode(key))

        encrypted = fernet_key.encrypt(compressed)
        combined_payload = base64.urlsafe_b64encode(salt + encrypted).decode("ascii")

        invoice_number = data.get("invoice_number", "unknown_invoice")
        njesia = data.get("njesia", "unknown_invoice")
        invoice_date = data.get("invoice_date", "unknown_date")

        qr_filename = f"{sanitize_filename_component(njesia)}_{sanitize_filename_component(invoice_number)}_{sanitize_filename_component(invoice_date)}.png"
        qr_path = os.path.join(qr_dir, qr_filename)

        qr = qrcode.QRCode(version=None, error_correction=ERROR_CORRECT_M, box_size=10, border=4)
        qr.add_data(combined_payload)
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_path)

        logging.info(f"Saved QR code to {qr_path}")
        return qr_path
    except Exception as e:
        logging.error(f"Failed to create encrypted QR code: {e}", exc_info=True)
        raise


# ----------------------------- PDF RENDER ----------------------------------------
def populate_html_and_generate_pdf(template_path, output_pdf_path, data, configuration, options=None):
    env = Environment(loader=FileSystemLoader(os.path.dirname(template_path)))
    env.filters["date"] = format_date
    template_name = os.path.basename(template_path)
    template = env.get_template(template_name)
    populated_html = template.render(data)
    pdfkit.from_string(populated_html, output_pdf_path, configuration=configuration, options=options)
    logging.info(f"PDF generated: {output_pdf_path}")


# ----------------------------- PRINTER CACHE -------------------------------------
_PRINTER_CACHE: dict[str, tuple[str, float]] = {}  # njesia -> (ip, expiry_ts)


def _ip_reachable(ip: str, port: int, timeout: int) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False


def find_printer_ip_cached(njesia: str, printer_port=PRINTER_PORT, timeout=PRINTER_SCAN_TIMEOUT, ttl_sec=PRINTER_CACHE_TTL_SEC):
    now = time.time()
    cached = _PRINTER_CACHE.get(str(njesia))
    if cached and cached[1] > now:
        return cached[0]

    try:
        n = int(njesia)
        if n in (200, 201) or not (0 <= n <= 254):
            logging.warning(f"[Printer Scan] Njesia={njesia} excluded or invalid.")
            return None
    except ValueError:
        logging.warning(f"[Printer Scan] Njesia={njesia} invalid.")
        return None

    base = f"192.168.{n}."
    ips = [f"{base}{i}" for i in range(1, 255)]
    logging.info(f"[Printer Scan] Scanning {base}1–254...")

    with ThreadPoolExecutor(max_workers=50) as ex:
        futures = {ex.submit(_ip_reachable, ip, printer_port, timeout): ip for ip in ips}
        for fut in as_completed(futures):
            if fut.result():
                ip = futures[fut]
                _PRINTER_CACHE[str(njesia)] = (ip, now + ttl_sec)
                return ip

    logging.warning(f"[Printer Scan] No printer found in 192.168.{n}.x")
    return None


def send_raw_to_printer(printer_ip, printer_port, file_path, copies=1, chunk_size=8192):
    try:
        with socket.create_connection((printer_ip, printer_port), timeout=10) as printer_socket:
            printer_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            for _ in range(max(1, copies)):
                with open(file_path, "rb") as file:
                    while True:
                        chunk = file.read(chunk_size)
                        if not chunk:
                            break
                        printer_socket.sendall(chunk)
        logging.info(f"Sent {copies} copy(ies) to printer {printer_ip}:{printer_port}")
    except Exception as e:
        logging.error(f"Printer send error {printer_ip}:{printer_port}: {e}")


# ----------------------------- EMAIL (retry) -------------------------------------
DEFAULT_TO = []
DEFAULT_CC_ALWAYS = ["",]
INTERNAL_TEST_CC = [

]


def send_email_with_attachment(
    receiver_emails: list[str],
    subject: str,
    html_body: str,
    attachment_path: str,
    cc_emails: list[str] | None = None,
    max_retries: int = 3,
    base_delay: float = 1.0,
):
    sender_email = "kthime@returns-vivafresh.com"
    sender_password = "ppoCX5Ac54pglwtP"
    smtp_server = "mail.smtp2go.com"
    smtp_port = 2525

    to_list = [e for e in (receiver_emails or []) if EMAIL_REGEX.match(e)]
    cc_list = [e for e in (cc_emails or []) if EMAIL_REGEX.match(e)]

    if not to_list:
        raise ValueError("No valid recipient in 'receiver_emails'.")

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    with open(attachment_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(attachment_path)}")
        msg.attach(part)

    recipients = to_list + cc_list

    for attempt in range(1, max_retries + 1):
        try:
            with smtplib.SMTP(smtp_server, smtp_port, timeout=15) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipients, msg.as_string())
            logging.info(f"Email sent (attempt {attempt}) to {recipients}")
            return
        except Exception as e:
            logging.warning(f"SMTP attempt {attempt} failed: {e}")
            if attempt == max_retries:
                raise
            time.sleep(base_delay * (2 ** (attempt - 1)) + random.random() * 0.25)


# ----------------------------- PIPELINE ------------------------------------------
def process_and_print_invoice(id_fatura, encryption_password, template_path, pdf_dir):
    # 1) Fetch data
    invoice_data = get_invoice_details(id_fatura)
    if not invoice_data:
        raise RuntimeError(f"Invoice {id_fatura} details not found")

    items = get_invoice_products(id_fatura)
    data = map_data_for_template(invoice_data, items)

    # 2) Prepare QR payloads
    qr_data_minified = {
        "inv": data["invoice_number"],
        "dt": data["invoice_date"],
        "nt": data["subtotal"],
        "tax": data["total_tax"],
        "tot": data["grand_total"],
        "n": data["njesia"],
    }

    sanitized_inv = sanitize_filename_component(str(data["invoice_number"]))
    sanitized_date = sanitize_filename_component(str(data["invoice_date"]))
    sanitized_njesia = sanitize_filename_component(str(data["njesia"]))
    base_name = f"{sanitized_njesia}_{sanitized_inv}_{sanitized_date}"

    qr_path = create_encrypted_qr(qr_data_minified, encryption_password)
    qr_full_path = os.path.abspath(qr_path).replace("\\", "/")
    data["qr_code_image"] = f"file:///{qr_full_path}"

    # 3) PDF render
    os.makedirs(pdf_dir, exist_ok=True)
    pdf_filename = f"{base_name}.pdf"
    output_pdf_path = os.path.join(pdf_dir, pdf_filename)
    options = {"encoding": "UTF-8", "enable-local-file-access": ""}
    populate_html_and_generate_pdf(template_path, output_pdf_path, data, configuration=config, options=options)

    # 4) ZIP artifacts
    zip_dir = os.path.join(pdf_dir, "zipped")
    os.makedirs(zip_dir, exist_ok=True)
    zip_path = os.path.join(zip_dir, f"{base_name}.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(output_pdf_path, arcname=os.path.basename(output_pdf_path))
        zipf.write(qr_path, arcname=os.path.basename(qr_path))

    # 5) Email body (kept inline; can be moved to Jinja)
    html_body = f"""
    <html>
      <head>
        <style>
          body {{ font-family: Arial, sans-serif; color: #333; background-color: #f9f9f9; padding: 30px; }}
          .container {{ max-width: 700px; margin: auto; background-color: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }}
          .header {{ font-size: 18px; margin-bottom: 20px; }}
          .info-table {{ font-size: 15px; line-height: 1.6; width: 100%; border-collapse: collapse; }}
          .info-table td {{ padding: 6px 8px; }}
          .footer {{ margin-top: 30px; font-size: 14px; color: #666; }}
        </style>
      </head>
      <body>
        <div class="container">
          <p class="header">Përshëndetje,</p>
          <p>Ju lutem gjeni të bashkëngjitur faturën e kthimit:</p>
          <table class="info-table" cellpadding="5" cellspacing="0">
            <tr><td><strong>Fatura Nr:</strong></td><td>{data['tipidokumentit']}/{data['invoice_number']}/{data['njesia']}</td></tr>
            <tr><td><strong>Data:</strong></td><td>{data['invoice_date']}</td></tr>
            <tr><td><strong>Furnitori:</strong></td><td>{data['viva_furnitori']} ({data['viva_shifrafurnitorit']})</td></tr>
            <tr><td><strong>Pranuesi:</strong></td><td>{data['viva_pranuesi']}</td></tr>
            <tr><td><strong>Nëntotali:</strong></td><td>{data['subtotal']} {data['currency']}</td></tr>
            <tr><td><strong>TVSH:</strong></td><td>{data['total_tax']} {data['currency']}</td></tr>
            <tr><td><strong>Totali:</strong></td><td>{data['grand_total']} {data['currency']}</td></tr>
            <tr><td><strong>Komercialisti:</strong></td><td>{data['Kom1']} {data['Kom2']}</td></tr>
          </table>
          <p class="footer">Faleminderit,<br /><strong>Viva Fresh</strong></p>
        </div>
      </body>
    </html>
    """

    # 6) Email routing
    furn_id = str(data.get("FurnitoriID") or data.get("viva_shifrafurnitorit") or "").strip()
    to_supplier, cc_supplier = get_contacts_for_furnitor(furn_id)

    if to_supplier:
        final_to = to_supplier
        final_cc = list({*cc_supplier, *DEFAULT_CC_ALWAYS, *INTERNAL_TEST_CC})
    else:
        final_to = DEFAULT_TO
        final_cc = list({*DEFAULT_CC_ALWAYS, *INTERNAL_TEST_CC})

    # Email is considered CRITICAL for success -> raise on failure
    send_email_with_attachment(
        receiver_emails=final_to,
        subject=f"Kthimi - Fatura #{data['invoice_number']}",
        html_body=html_body,
        attachment_path=output_pdf_path,
        cc_emails=final_cc,
    )

    # 7) Printing (non-critical; log errors but do not fail pipeline)
    njesia_value = data.get("njesia")
    printer_ip = find_printer_ip_cached(str(njesia_value)) if njesia_value else None
    if printer_ip:
        try:
            send_raw_to_printer(printer_ip, PRINTER_PORT, output_pdf_path, copies=2)
        except Exception as e:
            logging.error(f"Printer error for invoice {id_fatura}: {e}")
    else:
        logging.error(f"No printer found for Njesia={njesia_value}. Skipping print.")

    # 8) Cleanup
    try:
        if os.path.exists(output_pdf_path):
            os.remove(output_pdf_path)
        if os.path.exists(qr_path):
            os.remove(qr_path)
        logging.info(f"Cleaned artifacts for invoice {id_fatura}")
    except OSError as e:
        logging.error(f"Cleanup failed for invoice {id_fatura}: {e}")


# ----------------------------- MAIN LOOP -----------------------------------------
if __name__ == "__main__":
    template_path = "FATURA_template/template.html"
    pdf_dir = "FATURA_template"
    os.makedirs(pdf_dir, exist_ok=True)

    encryption_password = "MySecretPassword"

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    while True:
        try:
            # Claim a batch = number of workers (avoid over-claiming)
            invoice_ids = claim_invoices(batch_size=MAX_WORKERS)
            if not invoice_ids:
                logging.info("No invoices to process.")
            else:
                future_to_id = {
                    executor.submit(
                        process_and_print_invoice,
                        inv_id,
                        encryption_password,
                        template_path,
                        pdf_dir,
                    ): inv_id
                    for inv_id in invoice_ids
                }

                for future in as_completed(future_to_id):
                    inv_id = future_to_id[future]
                    try:
                        future.result()
                        finalize_invoice(inv_id)
                    except Exception as ex:
                        logging.error(f"Invoice {inv_id} failed: {ex}")
                        revert_invoice(inv_id)
        except Exception as ex:
            logging.error(f"Critical error in main loop: {ex}", exc_info=True)

        time.sleep(POLL_SECONDS)
