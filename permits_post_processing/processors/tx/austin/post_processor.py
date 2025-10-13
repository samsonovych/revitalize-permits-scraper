"""Austin post-processor implementing the logic for post processing austin permits."""

from __future__ import annotations

import pandas as pd

import numpy as np

from permits_post_processing.base import BasePostProcessor, PostProcessingResult
from typing import Optional
from pathlib import Path


class AustinDefaultPostProcessor(BasePostProcessor):
    """Austin post-processor implementing the logic for post processing austin permits."""

    name: str = "Austin Default Processor"

    def process(self, df: pd.DataFrame, output_path: Optional[Path | str] = None) -> PostProcessingResult:  # type: ignore[override]
        """Process the Austin permits dataset."""
        # Track permits count before
        before_permits = self._infer_unique_permit_count(df)

        save_cols = [
            "permit_number",
            "permit_type_desc",
            "work_class",
            "permit_location",
            "original_zip",
            "description",
            "applieddate",
            "issue_date",
            "expiresdate",
            "completed_date",
            "status_current",
            "latitude",
            "longitude",
            "contractor_trade",
            "contractor_company_name",
            "contractor_full_name",
            "contractor_phone",
            "contractor_address1",
            "contractor_address2",
            "contractor_city",
            "contractor_zip",
            "applicant_full_name",
            "applicant_phone",
            "applicant_address1",
            "applicant_address2",
            "applicant_city",
            "applicantzip",
        ]
        existing_cols = [c for c in save_cols if c in df.columns]
        result: pd.DataFrame = df[existing_cols].copy()

        rename_map = {
            "permit_type_desc": "record_type",
            "permit_location": "building_address",
            "applieddate": "application_date",
            "expiresdate": "expires_date",
            "status_current": "status",
            "original_zip": "building_zip_code",
            "applicantzip": "applicant_zip",
        }
        result.rename(columns={k: v for k, v in rename_map.items() if k in result.columns}, inplace=True)

        # Lowercase selected text columns if present
        for col in ["record_type", "work_class", "description", "building_address", "status"]:
            if col in result.columns:
                result[col] = result[col].astype(str).str.lower()

        # Filter record types and work classes when present
        if "record_type" in result.columns:
            result = result[result["record_type"].isin(["building permit"])]
        if "work_class" in result.columns:
            result = result[result["work_class"].isin(["new"])]

        # Normalize zip columns
        if "building_zip_code" in result.columns:
            result["building_zip_code"] = result["building_zip_code"].fillna(0).astype(int).astype(str).replace("0", "")

        # Datetime conversions
        for dc in ["application_date", "issue_date", "expires_date", "completed_date"]:
            if dc in result.columns:
                result[dc] = pd.to_datetime(result[dc])

        # Phone cleanup and filtering
        if "contractor_phone" in result.columns:
            result["contractor_phone"] = result["contractor_phone"].fillna(0).astype(int).astype(str).replace("0", np.nan)
            result = result[result["contractor_phone"].str.len() == 10]
        if "applicant_phone" in result.columns:
            result["applicant_phone"] = result["applicant_phone"].fillna(0).astype(int).astype(str).replace("0", np.nan)

        # Zip cleanup for contractor/applicant
        if "contractor_zip" in result.columns:
            result["contractor_zip"] = result["contractor_zip"].fillna(0)
            result["contractor_zip"] = result["contractor_zip"].replace(r"[^0-9]", "", regex=True)
            result["contractor_zip"] = result["contractor_zip"].astype(str).replace("0", np.nan)
        if "applicant_zip" in result.columns:
            result["applicant_zip"] = result["applicant_zip"].fillna(0)
            result["applicant_zip"] = result["applicant_zip"].replace(r"[^0-9]", "", regex=True)
            result["applicant_zip"] = result["applicant_zip"].astype(str).replace("0", np.nan)

        # days_since_filling and buckets
        if "application_date" in result.columns:
            result["days_since_filling"] = (pd.to_datetime("now") - result["application_date"]).dt.days
            bins = [-1, 90, 180]
            labels = ["recently_issued", "primary_backlog"]
            result["bucket"] = pd.cut(result["days_since_filling"], bins=bins, labels=labels)
        if "bucket" in result.columns and "status" in result.columns:
            result.dropna(subset=["bucket", "status"], inplace=True)

        # Grouping by contractor_phone if present
        if "contractor_phone" in result.columns:
            result["permit_count"] = 1
            group_cols = {col: self.concatenate_values for col in result.columns if col not in ["contractor_phone", "permit_count"]}
            group_cols["permit_count"] = "count"
            result = result.groupby("contractor_phone").agg(group_cols).reset_index()

        # Persist
        if output_path is not None:
            suffix = str(output_path).lower()
            if suffix.endswith(".csv"):
                result.to_csv(output_path, index=False)
            elif suffix.endswith(".parquet") or suffix.endswith(".pq"):
                result.to_parquet(output_path, index=False)
            else:
                # default to CSV
                result.to_csv(output_path, index=False)

        after_permits = self._infer_unique_permit_count(result)
        return PostProcessingResult(
            df=result,
            output_path=str(output_path) if output_path is not None else None,
            permits_number_before=before_permits,
            permits_number_after=after_permits,
        )
