"""El Paso post-processor implementing the logic for post processing el paso permits."""

from __future__ import annotations

import numpy as np
import pandas as pd


from permits_post_processing.base import BasePostProcessor, PostProcessingResult
from typing import Optional, Dict, List, Tuple
from pathlib import Path
import re


class ElPasoDefaultPostProcessor(BasePostProcessor):
    """El Paso post-processor implementing the logic for post processing el paso permits."""

    name: str = "El Paso Default Processor"

    def process(self, df: pd.DataFrame, output_path: Optional[Path | str] = None) -> PostProcessingResult:  # type: ignore[override]
        """Process the El Paso permits dataset."""
        # Track permits count before
        before_permits = self._infer_unique_permit_count(df)

        # Required output columns
        cols: List[str] = [
            "permit_number",
            "record_type",
            "status",
            "application_date",
            "project_name",
            "description",
            "applicant_first_name",
            "applicant_last_name",
            "applicant_company_name",
            "applicant_address",
            "applicant_phone",
            "applicant_work_phone",
            "applicant_mobile_phone",
            "applicant_email",
            "licensed_professional_first_name",
            "licensed_professional_last_name",
            "licensed_professional_company_name",
            "licensed_professional_address",
            "licensed_professional_home_phone_number",
            "licensed_professional_mobile_phone_number",
            "owner_address",
            "owner_company_name",
            "owner_first_name",
            "owner_last_name",
            "third_party_first_name",
            "third_party_last_name",
            "third_party_company_name",
            "third_party_address",
            "third_party_phone_number",
            "third_party_mobile_phone_number",
            "third_party_email",
            "job_value",
        ]

        # Ensure needed inputs exist
        for c in [
            "permit_number",
            "applicant",
            "licensed_professional",
            "third_party",
            "owner.address",
            "owner.company_name",
            "owner.first_name",
            "owner.last_name",
            "job_value",
        ]:
            if c not in df.columns:
                df[c] = None

        def _extract_email(text: Optional[str]) -> Optional[str]:
            s = text if isinstance(text, str) else ""
            m = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", s)
            return m.group(0) if m else None

        def _extract_phones(text: Optional[str]) -> Dict[str, Optional[str]]:
            phones: Dict[str, Optional[str]] = {"phone": None, "work": None, "mobile": None, "home": None}
            s = text if isinstance(text, str) else ""
            patterns = [
                (r"Home Phone:\s*([0-9\-\(\)\s]+)", "home"),
                (r"Work Phone:\s*([0-9\-\(\)\s]+)", "work"),
                (r"Mobile Phone:\s*([0-9\-\(\)\s]+)", "mobile"),
                (r"Phone:\s*([0-9\-\(\)\s]+)", "phone"),
            ]
            for pat, key in patterns:
                m = re.search(pat, s, re.I)
                if m:
                    digits = re.sub(r"\D", "", m.group(1))
                    if digits:
                        phones[key] = digits
            return phones

        def _split_lines(text: Optional[str]) -> List[str]:
            if not text or not isinstance(text, str):
                return []
            return [ln.strip() for ln in text.splitlines() if ln.strip()]

        def _split_name_company(lines: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str], List[str]]:
            first_name: Optional[str] = None
            last_name: Optional[str] = None
            company: Optional[str] = None
            rest: List[str] = []
            if not lines:
                return None, None, None, []
            # First line as person full name
            name_tokens = lines[0].split()
            if len(name_tokens) >= 2:
                first_name = name_tokens[0].title()
                last_name = " ".join(name_tokens[1:]).title()
            elif len(name_tokens) == 1:
                first_name = name_tokens[0].title()
            # Second line as potential company
            idx = 1
            if len(lines) > 1 and not re.match(r"^(Phone|Work Phone|Mobile Phone|Home Phone|E-?mail|Contractor General|LCCR)", lines[1], re.I):
                company = lines[1]
                idx = 2
            rest = lines[idx:]
            return first_name, last_name, company, rest

        def _normalize_address(lines: List[str]) -> Optional[str]:
            if not lines:
                return None
            addr = ", ".join([p.strip(", ") for p in lines if p and p.strip()])
            addr = re.sub(r"\s+", " ", addr).strip()
            return addr.lower() if addr else None

        def _parse_contact_block(text: Optional[str]) -> Dict[str, Optional[str]]:
            result: Dict[str, Optional[str]] = {
                "first_name": None,
                "last_name": None,
                "company": None,
                "address": None,
                "email": None,
                "phone": None,
                "work": None,
                "mobile": None,
                "home": None,
            }
            lines = _split_lines(text)
            # Remove non-address/control lines from address lines
            address_lines: List[str] = []
            for ln in lines:
                if ln.startswith("Contractor General") or re.match(r"^LCCR", ln):
                    continue
                if re.match(r"^(Phone|Work Phone|Mobile Phone|Home Phone|E-?mail)", ln, re.I):
                    continue
                # Exclude bare email lines from address
                if re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", ln):
                    continue
                address_lines.append(ln)

            fn, ln, company, rest = _split_name_company(address_lines)
            result["first_name"] = fn
            result["last_name"] = ln
            result["company"] = company
            result["address"] = _normalize_address(rest)
            result["email"] = _extract_email(text)
            phones = _extract_phones(text)
            result["phone"] = phones["phone"]
            result["work"] = phones["work"]
            result["mobile"] = phones["mobile"]
            result["home"] = phones["home"]
            return result

        out_rows: List[Dict[str, Optional[str]]] = []
        for _, r in df.iterrows():
            row: Dict[str, Optional[str]] = {k: None for k in cols}
            row["permit_number"] = str(r.get("permit_number")) if pd.notna(r.get("permit_number")) else None
            row["job_value"] = r.get("job_value") if pd.notna(r.get("job_value")) else None

            # Applicant
            app = _parse_contact_block(r.get("applicant"))
            row["applicant_first_name"] = app["first_name"]
            row["applicant_last_name"] = app["last_name"]
            row["applicant_company_name"] = app["company"]
            row["applicant_address"] = app["address"]
            row["applicant_phone"] = app["phone"]
            row["applicant_work_phone"] = app["work"]
            row["applicant_mobile_phone"] = app["mobile"]
            row["applicant_email"] = app["email"]

            # Licensed professional
            lic = _parse_contact_block(r.get("licensed_professional"))
            row["licensed_professional_first_name"] = lic["first_name"]
            row["licensed_professional_last_name"] = lic["last_name"]
            row["licensed_professional_company_name"] = lic["company"]
            row["licensed_professional_address"] = lic["address"]
            row["licensed_professional_home_phone_number"] = lic["home"] or lic["phone"]
            row["licensed_professional_mobile_phone_number"] = lic["mobile"] or row["licensed_professional_home_phone_number"]

            # Owner
            row["owner_address"] = r.get("owner.address") if pd.notna(r.get("owner.address")) else None
            row["owner_company_name"] = r.get("owner.company_name") if pd.notna(r.get("owner.company_name")) else None
            row["owner_first_name"] = r.get("owner.first_name") if pd.notna(r.get("owner.first_name")) else None
            row["owner_last_name"] = r.get("owner.last_name") if pd.notna(r.get("owner.last_name")) else None

            # Third party
            tp = _parse_contact_block(r.get("third_party"))
            row["third_party_first_name"] = tp["first_name"]
            row["third_party_last_name"] = tp["last_name"]
            row["third_party_company_name"] = tp["company"]
            row["third_party_address"] = tp["address"]
            row["third_party_phone_number"] = tp["phone"] or tp["home"]
            row["third_party_mobile_phone_number"] = tp["mobile"] or row["third_party_phone_number"]
            row["third_party_email"] = tp["email"]

            # Record overview
            row["record_type"] = r.get("record_type")
            row["status"] = r.get("status")
            row["application_date"] = r.get("application_date")
            row["project_name"] = r.get("project_name")
            row["description"] = r.get("description")

            out_rows.append(row)

        result = pd.DataFrame(out_rows, columns=cols)

        # Replace None with NaN
        result = result.where(pd.notna(result), np.nan)

        result['status'] = result['status'].str.lower()
        result['record_type'] = result['record_type'].str.lower()

        include_record_types = ["residential new", "3rd party residential new"]
        result = result[result['record_type'].isin(include_record_types)]

        result['application_date'] = pd.to_datetime(result['application_date'])
        result.dropna(subset=['application_date'], inplace=True)

        # Replace all empty strings with NaN in order to perform the notna operation
        result = result.replace(to_replace="", value=np.nan)
        # Filter out rows where all the phone numbers are None
        result = result[result[
            ['applicant_phone', 'applicant_work_phone',
             'applicant_mobile_phone', 'licensed_professional_home_phone_number', 'licensed_professional_mobile_phone_number',
             'third_party_phone_number', 'third_party_mobile_phone_number'
             ]].notna().any(axis=1)]

        result["days_since_filling"] = (pd.to_datetime("now") - result["application_date"]).dt.days
        result['application_date'] = result['application_date'].dt.strftime('%d/%m/%Y')
        bins = [-1, 90, 180]
        labels = ["recently_issued", "primary_backlog"]
        result["bucket"] = pd.cut(result["days_since_filling"], bins=bins, labels=labels)
        if "bucket" in result.columns and "status" in result.columns:
            result.dropna(subset=["bucket", "status"], inplace=True)

        def pair_key(df, first_col, last_col):
            """Pair key for a given first and last name."""
            first_name = df[first_col].astype("string").str.strip()
            last_name = df[last_col].astype("string").str.strip()
            mask = first_name.notna() & last_name.notna()  # require both present
            key = pd.Series(pd.NA, index=df.index, dtype="string")
            key[mask] = (first_name[mask].str.lower() + "|" + last_name[mask].str.lower())  # normalized full name
            return key

        cand1 = pair_key(result, "applicant_first_name", "applicant_last_name")
        cand2 = pair_key(result, "licensed_professional_first_name", "licensed_professional_last_name")
        cand3 = pair_key(result, "third_party_first_name", "third_party_last_name")
        cand4 = pair_key(result, "owner_first_name", "owner_last_name")
        result["group_key"] = cand1.fillna(cand2).fillna(cand3).fillna(cand4)

        # Make a per-row fallback Series aligned to df.index
        idx_str = result.index.to_series().astype(str)
        fallback = "UNGROUPED_" + idx_str

        # Use where/combine_first so NaN rows get unique singleton keys
        result["group_key_final"] = result["group_key"].combine_first(fallback)

        # Optional numeric id for downstream joins
        result["group_id"] = pd.factorize(result["group_key_final"], sort=False)[0]

        # Use `group_key_final` for grouping/aggregation as needed:
        result["permit_count"] = 1
        group_cols = {col: BasePostProcessor.concatenate_values for col in result.columns if col not in ["permit_count", "group_key_final", "group_key"]}
        group_cols["permit_count"] = "count"
        result = result.groupby("group_key_final", sort=False).agg(group_cols).reset_index()
        result.drop(columns=["group_key_final", "group_id"], inplace=True)

        # Persist
        if isinstance(output_path, (str, Path)):
            op = str(output_path)
            if op.lower().endswith(".csv"):
                result.to_csv(op, index=False)
            elif op.lower().endswith((".parquet", ".pq")):
                result.to_parquet(op, index=False)
            else:
                result.to_csv(op, index=False)

        after_permits = self._infer_unique_permit_count(result)
        return PostProcessingResult(
            df=result,
            output_path=str(output_path) if output_path is not None else None,
            permits_number_before=before_permits,
            permits_number_after=after_permits,
        )
