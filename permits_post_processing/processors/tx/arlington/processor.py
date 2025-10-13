"""Arlington post-processor implementing logic for post processing arlington permits."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

from permits_post_processing.base import BasePostProcessor
from permits_post_processing.models import PostProcessingResult


class ArlingtonDefaultPostProcessor(BasePostProcessor):
    """Arlington post-processor implementing logic for post processing arlington permits."""

    name: str = "Arlington Default Processor"

    def process(self, df: pd.DataFrame, output_path: Optional[Path | str] = None) -> PostProcessingResult:  # type: ignore[override]
        """Process the Arlington permits dataset."""
        before_permits = self._infer_unique_permit_count(df)

        result = df.copy()

        # Column renames to align with downstream expectations
        rename_map = {
            "Description": "description",
            "work_type": "work_class",
            "address": "building_address",
            "sub": "sub_type",
        }
        existing_map = {k: v for k, v in rename_map.items() if k in result.columns}
        if existing_map:
            result.rename(columns=existing_map, inplace=True)

        # Keep only the relevant columns if present
        desired_cols: List[str] = [
            "permit_number",
            "sub_type",
            "work_class",
            "building_address",
            "building_area",
            "building_lot",
            "building_name",
            "building_type",
            "building_zip_code",
            "building_zoning",
            "description",
            "application_date",
            "issued_date",
            "expires_date",
            "status",
            "latitude",
            "longitude",
            "permit_valuation",
            "sub_contractors",
            "associated_people",
        ]
        existing_cols = [c for c in desired_cols if c in result.columns]
        result = result[existing_cols]

        result['building_zip_code'] = result['building_zip_code'].fillna(0)
        result['building_zip_code'] = result['building_zip_code'].astype(int)
        result['building_zip_code'] = result['building_zip_code'].astype(str)
        result['building_zip_code'] = result['building_zip_code'].replace("0", "")

        # Lowercasing some textual columns when present
        for col in ["status", "sub_type", "work_class", "building_type"]:
            if col in result.columns:
                result[col] = result[col].astype(str).str.lower()

        # Dates to DD/MM/YYYY if present and parseable
        for dc in ["application_date", "issued_date", "expires_date"]:
            if dc in result.columns:
                result[dc] = pd.to_datetime(result[dc], errors="coerce", format='%m/%d/%Y').dt.strftime("%d/%m/%Y")

        # Ensure JSON columns are parsed to Python objects if they are strings
        def _ensure_list_from_json(val):
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except Exception:
                    return []
            return val if isinstance(val, list) else []

        if "associated_people" in result.columns:
            result["associated_people"] = result["associated_people"].apply(_ensure_list_from_json)
        if "sub_contractors" in result.columns:
            result["sub_contractors"] = result["sub_contractors"].apply(_ensure_list_from_json)

        # Mapping helpers replicating notebook logic
        def map_associated_people(people: List[dict]) -> str:
            output = []
            for person in people or []:
                output.append(
                    "\n".join(
                        [
                            f"Type: {person.get('type')}",
                            f"Name: {person.get('name') or 'N/A'}",
                            f"Email: {person.get('email') or 'N/A'}",
                            f"Phone: {person.get('phone_number') or 'N/A'}",
                            f"Address: {person.get('address') or 'N/A'}",
                        ]
                    )
                )
                output.append("-----------------------------------")
            return "\n".join(output).strip()

        def map_sub_contractors(contracts: List[dict]) -> str:
            output = []
            for contractor in contracts or []:
                output.append(
                    "\n".join(
                        [
                            f"Type: {contractor.get('type')}",
                            f"Company Name: {contractor.get('company_name') or 'N/A'}",
                            f"Full Name: {contractor.get('point_of_contact') or 'N/A'}",
                            f"Email: {contractor.get('email') or 'N/A'}",
                            f"Phone: {contractor.get('phone_number') or 'N/A'}",
                            f"Effective from: {contractor.get('effective_from') or 'N/A'}",
                            f"Effective to: {contractor.get('effective_to') or 'N/A'}",
                            f"City registration number: {contractor.get('city_registration_number') or 'N/A'}",
                        ]
                    )
                )
                output.append("-----------------------------------")
            return "\n".join(output).strip()

        def calculate_phone_count(entries: List[dict]) -> int:
            phone_count = 0
            for info in entries or []:
                number = info.get("phone_number")
                if number:
                    if len(re.sub(r"\D", "", str(number))) == 10:
                        phone_count += 1
            return phone_count

        # Apply mapping columns
        if "associated_people" in result.columns:
            result["associated_people_info"] = result["associated_people"].apply(map_associated_people)
            result["associated_people_phone_count"] = result["associated_people"].apply(calculate_phone_count)
            result.drop(columns=["associated_people"], inplace=True)
        if "sub_contractors" in result.columns:
            result["sub_contractors_info"] = result["sub_contractors"].apply(map_sub_contractors)
            result["sub_contractors_phone_count"] = result["sub_contractors"].apply(calculate_phone_count)
            result.drop(columns=["sub_contractors"], inplace=True)

        if "associated_people_phone_count" in result.columns and "sub_contractors_phone_count" in result.columns:
            result["phone_count"] = result["associated_people_phone_count"].fillna(0).astype(int) + \
                result["sub_contractors_phone_count"].fillna(0).astype(int)

            # Example filter akin to notebook: keep rows with at least one phone
            result = result[result["phone_count"] > 0]

        result['application_date'] = pd.to_datetime(result['application_date'], format='%d/%m/%Y')
        result.dropna(subset=['application_date'], inplace=True)
        result['days_since_filling'] = (pd.to_datetime("now") - result['application_date']).dt.days
        result['application_date'] = result['application_date'].dt.strftime('%d/%m/%Y')
        result['days_since_filling'] = result['days_since_filling'].astype(int)

        buckets = [
            (-1, 90, "recently_issued"),
            (91, 180, "primary_backlog"),
        ]

        bins = []
        for i, el in enumerate(buckets):
            if i == 0:
                bins.append(el[0])
            bins.append(el[1])

        labels = []
        for i, el in enumerate(buckets):
            labels.append(el[2])

        result["bucket"] = pd.cut(result["days_since_filling"], bins=bins, labels=labels)
        result.dropna(subset=['bucket', 'status'], inplace=True)

        # Persist
        if output_path is not None:
            out_suffix = str(output_path).lower()
            if out_suffix.endswith(".csv"):
                result.to_csv(output_path, index=False)
            elif out_suffix.endswith(".parquet") or out_suffix.endswith(".pq"):
                result.to_parquet(output_path, index=False)
            else:
                result.to_csv(output_path, index=False)

        after_permits = self._infer_unique_permit_count(result)
        return PostProcessingResult(
            df=result,
            output_path=str(output_path) if output_path is not None else None,
            permits_number_before=before_permits,
            permits_number_after=after_permits,
        )
