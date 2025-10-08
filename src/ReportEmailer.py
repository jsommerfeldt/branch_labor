from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Union
from pathlib import Path
from io import BytesIO
import smtplib
from email.message import EmailMessage

import pandas as pd


DataSource = Union[pd.DataFrame, str, Path]


@dataclass
class SMTPConfig:
    """
    Configuration for SMTP transport.

    Parameters
    ----------
    host : str
        SMTP server hostname (e.g., "smtp.office365.com").
    port : int
        SMTP port. Common options: 587 (STARTTLS), 465 (SSL).
    username : Optional[str]
        Username for authentication. If None, no login is attempted.
    password : Optional[str]
        Password (or app password). If None, no login is attempted.
    use_starttls : bool
        If True, connect plain then upgrade with STARTTLS (typ. port 587).
    use_ssl : bool
        If True, connect via SMTP over SSL (typ. port 465).
    sender : Optional[str]
        Default "From" address for messages. If None, falls back to `username`.
    timeout : int
        Socket timeout in seconds.
    """
    host: str
    port: int = 587
    username: Optional[str] = None
    password: Optional[str] = None
    use_starttls: bool = True
    use_ssl: bool = False
    sender: Optional[str] = None
    timeout: int = 60


class ReportEmailer:
    """
    Build an Excel workbook from a DataFrame or CSV and email it to recipients.

    Typical flow:
      1) Create ReportEmailer(smtp_config)
      2) Call send_xlsx_report(data_source, recipients, subject, body, ...)

    Notes
    -----
    - `data_source` can be:
        * a pandas.DataFrame
        * a string/Path to a CSV file (read via pandas.read_csv)
    - The Excel is created in-memory using openpyxl via pandas.ExcelWriter.

    Dependencies
    ------------
    pandas, openpyxl (Excel writer engine), Python standard library
    """

    def __init__(self, smtp: SMTPConfig):
        self.smtp = smtp

    # -------------------------------
    # Public API
    # -------------------------------
    def send_xlsx_report(
        self,
        data_source: DataSource,
        recipients: Union[str, Iterable[str]],
        subject: str,
        body_text: str,
        *,
        sheet_name: str = "Data",  # Used only when we rebuild from DataFrame(s)
        index: bool = False,
        attachment_filename: Optional[str] = "report.xlsx",
        cc: Optional[Union[str, Iterable[str]]] = None,
        bcc: Optional[Union[str, Iterable[str]]] = None,
        reply_to: Optional[Union[str, Iterable[str]]] = None,
        source_sheet: Optional[Union[str, int]] = None,  # only for rebuild
        include_all_sheets: bool = False,                # only for rebuild
        pass_through_excel: bool = True,                 # NEW: attach .xlsx as-is
    ) -> None:
        """
        If `data_source` is:
        - DataFrame -> rebuild single-sheet attachment.
        - CSV path   -> read CSV, rebuild single-sheet attachment.
        - XLSX/XLSM path:
            * pass_through_excel=True  -> attach original file (includes all sheets).
            * pass_through_excel=False -> rebuild from one/all sheets (data only).
        """

        # If a path-like input is given, inspect extension
        if not isinstance(data_source, pd.DataFrame):
            p = Path(str(data_source))
            if p.exists() and p.is_file():
                ext = p.suffix.lower()
                if ext in {".xlsx", ".xlsm"} and pass_through_excel:
                    # Attach original Excel bytes: preserves all worksheets/formatting
                    xlsx_bytes = p.read_bytes()
                    # Use original name if caller didn't override attachment_filename
                    attach_name = attachment_filename or p.name
                    msg, all_rcpts = self._compose_email(
                        recipients=recipients,
                        subject=subject,
                        body_text=body_text,
                        attachment=xlsx_bytes,
                        attachment_filename=attach_name,
                        cc=cc,
                        bcc=bcc,
                        reply_to=reply_to,
                    )
                    self._send(msg, all_rcpts)
                    return
                # else: fall through to rebuild mode

        # Rebuild mode (single sheet or multi-sheet from XLSX)
        df_or_dict = self._load_dataframe(
            data_source,
            excel_sheet=source_sheet,
            read_all_sheets=include_all_sheets
        )
        xlsx_bytes = self._to_excel_bytes(df_or_dict, sheet_name=sheet_name, index=index)
        msg, all_rcpts = self._compose_email(
            recipients=recipients,
            subject=subject,
            body_text=body_text,
            attachment=xlsx_bytes,
            attachment_filename=attachment_filename or "report.xlsx",
            cc=cc,
            bcc=bcc,
            reply_to=reply_to,
        )
        self._send(msg, all_rcpts)

    # -------------------------------
    # Internals
    # -------------------------------
    def _load_dataframe(self, source: DataSource, *, excel_sheet: Optional[Union[str, int]] = None) -> pd.DataFrame:
        """
        Accept a DataFrame or read a CSV/XLSX path into a DataFrame.

        Parameters
        ----------
        source : pd.DataFrame | str | Path
            Either a DataFrame, or a file path to CSV/XLSX.
        excel_sheet : str | int | None
            If reading from XLSX/XLSM, which sheet to load.
            - str: worksheet name
            - int: 0-based worksheet index
            - None: first worksheet (default)
        """
        if isinstance(source, pd.DataFrame):
            return source

        # Treat strings/Paths as file paths
        p = Path(str(source))
        if not p.exists() or not p.is_file():
            raise FileNotFoundError(f"File not found: {p}")

        ext = p.suffix.lower()
        if ext in {".csv", ".txt"}:
            # Adjust options as needed (encoding, sep, dtype, parse_dates, etc.)
            return pd.read_csv(p)
        elif ext in {".xlsx", ".xlsm"}:
            # Requires openpyxl (already used for writing in this class)
            sheet_to_read = 0 if excel_sheet is None else excel_sheet
            return pd.read_excel(p, sheet_name=sheet_to_read, engine="openpyxl")
        else:
            raise ValueError(
                f"Unsupported file extension '{ext}'. "
                "Supported inputs: DataFrame, .csv/.txt, .xlsx/.xlsm"
            )

    def _dataframe_to_excel_bytes(
        self, df: pd.DataFrame, *, sheet_name: str = "Data", index: bool = False
    ) -> bytes:
        """Serialize a DataFrame to XLSX bytes (in-memory) with openpyxl."""
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=index)
        return buf.getvalue()

    def _compose_email(
        self,
        *,
        recipients: Union[str, Iterable[str]],
        subject: str,
        body_text: str,
        attachment: Optional[bytes],
        attachment_filename: str,
        cc: Optional[Union[str, Iterable[str]]] = None,
        bcc: Optional[Union[str, Iterable[str]]] = None,
        reply_to: Optional[Union[str, Iterable[str]]] = None,
    ) -> tuple[EmailMessage, List[str]]:
        """Build an EmailMessage and return it along with the full recipient list."""
        msg = EmailMessage()

        sender = self.smtp.sender or self.smtp.username
        if not sender:
            raise ValueError("No 'sender' available. Provide SMTPConfig.sender or SMTPConfig.username.")
        msg["From"] = sender

        to_list = self._coerce_recipients(recipients)
        if not to_list:
            raise ValueError("'recipients' is empty.")
        msg["To"] = ", ".join(to_list)

        cc_list: List[str] = self._coerce_recipients(cc) if cc else []
        if cc_list:
            msg["Cc"] = ", ".join(cc_list)

        if reply_to:
            reply_to_list = self._coerce_recipients(reply_to)
            if reply_to_list:
                msg["Reply-To"] = ", ".join(reply_to_list)

        msg["Subject"] = subject
        msg.set_content(body_text)

        if attachment is not None:
            msg.add_attachment(
                attachment,
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=attachment_filename,
            )

        # BCC is not included in headers; only in the envelope recipients
        bcc_list: List[str] = self._coerce_recipients(bcc) if bcc else []

        all_recipients = to_list + cc_list + bcc_list
        return msg, all_recipients

    def _send(self, msg: EmailMessage, all_recipients: List[str]) -> None:
        """Send an EmailMessage using the configured SMTP transport."""
        # Choose SSL or plain/STARTTLS based on config
        if self.smtp.use_ssl:
            with smtplib.SMTP_SSL(self.smtp.host, self.smtp.port, timeout=self.smtp.timeout) as server:
                self._login_and_send(server, msg, all_recipients)
        else:
            with smtplib.SMTP(self.smtp.host, self.smtp.port, timeout=self.smtp.timeout) as server:
                if self.smtp.use_starttls:
                    server.starttls()
                self._login_and_send(server, msg, all_recipients)

    def _login_and_send(self, server: Union[smtplib.SMTP, smtplib.SMTP_SSL], msg: EmailMessage, all_recipients: List[str]) -> None:
        """Perform optional login then send the message."""
        if self.smtp.username and self.smtp.password:
            server.login(self.smtp.username, self.smtp.password)
        server.send_message(msg, to_addrs=all_recipients)

    @staticmethod
    def _coerce_recipients(value: Optional[Union[str, Iterable[str]]]) -> List[str]:
        """
        Normalize recipients into a list of non-empty strings.
        Accepts a list/tuple/set or a comma/semicolon-separated string.
        """
        if value is None:
            return []
        if isinstance(value, str):
            # Support comma or semicolon separation
            parts = [p.strip() for p in value.replace(";", ",").split(",")]
            return [p for p in parts if p]
        # Iterable of anything -> strings
        result = [str(x).strip() for x in value if str(x).strip()]
        return result
