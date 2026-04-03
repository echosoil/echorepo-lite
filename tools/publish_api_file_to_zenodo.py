#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_simple_env_file(path: str) -> dict[str, str]:
    env: dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return env
    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip("'").strip('"')
    return env


def env_or_config(
    name: str,
    cli_value: str | None,
    file_env: dict[str, str],
    default: str | None = None,
) -> str | None:
    if cli_value not in (None, ""):
        return cli_value
    if os.getenv(name):
        return os.getenv(name)
    if name in file_env:
        return file_env[name]
    return default


def bool_from_envish(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y", "t"}


def build_echorepo_headers(api_key: str | None, bearer_token: str | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    if api_key:
        headers["X-API-Key"] = api_key
    elif bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    else:
        raise ValueError("Need either ECHOREPO API key or bearer token")
    return headers


def request_ok(resp: requests.Response, expected: set[int], context: str) -> None:
    if resp.status_code not in expected:
        raise RuntimeError(f"{context} failed with HTTP {resp.status_code}: {resp.text[:3000]}")


def build_filter_params(args: argparse.Namespace) -> dict[str, str]:
    params: dict[str, str] = {}
    if args.from_date:
        params["from"] = args.from_date
    if args.to_date:
        params["to"] = args.to_date
    if args.country:
        params["country"] = args.country
    if args.country_code:
        params["country_code"] = args.country_code
    if args.bbox:
        params["bbox"] = args.bbox
    if args.within:
        params["within"] = args.within
    if args.extra_param:
        for item in args.extra_param:
            if "=" not in item:
                raise ValueError(f"Invalid --extra-param value {item!r}, expected key=value")
            k, v = item.split("=", 1)
            k = k.strip()
            v = v.strip()
            if not k:
                raise ValueError(f"Invalid --extra-param value {item!r}, empty key")
            params[k] = v
    return params


def normalize_endpoint_path(api_path: str) -> str:
    api_path = api_path.strip()
    if not api_path:
        raise ValueError("API path must not be empty")
    if not api_path.startswith("/"):
        api_path = "/" + api_path
    return api_path

def normalize_grant_id(raw: str) -> str:
    raw = raw.strip()
    if "::" in raw:
        return raw
    return f"10.13039/501100000780::{raw}"

def parse_subject(raw: str) -> dict[str, str]:
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) < 2:
        raise ValueError(
            f"Invalid subject spec {raw!r}. Expected 'term|identifier|scheme'"
        )
    subject = {
        "term": parts[0],
        "identifier": parts[1],
    }
    if len(parts) >= 3 and parts[2]:
        subject["scheme"] = parts[2]
    return subject

def infer_download_name_from_path(api_path: str, fallback: str = "downloaded_file") -> str:
    name = api_path.rstrip("/").split("/")[-1]
    return name or fallback


def download_api_file(
    api_base: str,
    api_path: str,
    headers: dict[str, str],
    filters: dict[str, str],
    output_path: Path,
    timeout: int = 300,
) -> dict[str, Any]:
    url = f"{api_base.rstrip('/')}{normalize_endpoint_path(api_path)}"
    resp = requests.get(url, headers=headers, params=filters, timeout=timeout, stream=True)
    request_ok(resp, {200}, "API file download")
    with output_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return {
        "download_url": resp.url,
        "content_type": resp.headers.get("Content-Type", ""),
        "size_bytes": output_path.stat().st_size,
    }


def maybe_wrap_in_zip(input_path: Path, output_zip_path: Path, member_name: str | None = None) -> dict[str, Any]:
    member = member_name or input_path.name
    with zipfile.ZipFile(output_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(input_path, arcname=member)
    return {
        "zip_path": str(output_zip_path),
        "member_name": member,
        "size_bytes": output_zip_path.stat().st_size,
    }


def build_zenodo_base_url(use_sandbox: bool) -> str:
    return (
        "https://sandbox.zenodo.org/api/deposit/depositions"
        if use_sandbox
        else "https://zenodo.org/api/deposit/depositions"
    )


def zenodo_auth_headers(access_token: str, json_body: bool = False) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {access_token}"}
    if json_body:
        headers["Content-Type"] = "application/json"
    return headers


def create_new_deposition(zenodo_url: str, access_token: str) -> dict[str, Any]:
    resp = requests.post(
        zenodo_url,
        headers=zenodo_auth_headers(access_token, json_body=True),
        json={"metadata": {"prereserve_doi": True}},
        timeout=120,
    )
    request_ok(resp, {201}, "Zenodo create deposition")
    return resp.json()


def create_new_version_draft(
    zenodo_url: str, access_token: str, existing_deposition_id: str
) -> dict[str, Any]:
    resp = requests.post(
        f"{zenodo_url}/{existing_deposition_id}/actions/newversion",
        headers=zenodo_auth_headers(access_token),
        timeout=120,
    )
    request_ok(resp, {201}, "Zenodo new version action")
    body = resp.json()
    latest_draft_url = body.get("links", {}).get("latest_draft")
    if not latest_draft_url:
        raise RuntimeError("Zenodo did not return links.latest_draft")
    draft_resp = requests.get(
        latest_draft_url, headers=zenodo_auth_headers(access_token), timeout=120
    )
    request_ok(draft_resp, {200}, "Zenodo fetch latest draft")
    return draft_resp.json()


def update_metadata(
    zenodo_url: str, access_token: str, deposition_id: str, metadata: dict[str, Any]
) -> dict[str, Any]:
    resp = requests.put(
        f"{zenodo_url}/{deposition_id}",
        headers=zenodo_auth_headers(access_token, json_body=True),
        json={"metadata": metadata},
        timeout=120,
    )
    request_ok(resp, {200}, "Zenodo metadata update")
    return resp.json()


def upload_file_to_bucket(bucket_url: str, access_token: str, file_path: Path) -> None:
    with file_path.open("rb") as f:
        resp = requests.put(
            f"{bucket_url}/{file_path.name}",
            headers={"Authorization": f"Bearer {access_token}"},
            data=f,
            timeout=600,
        )
    request_ok(resp, {200, 201}, f"Zenodo upload file {file_path.name}")


def publish_deposition(zenodo_url: str, access_token: str, deposition_id: str) -> dict[str, Any]:
    resp = requests.post(
        f"{zenodo_url}/{deposition_id}/actions/publish",
        headers=zenodo_auth_headers(access_token),
        timeout=120,
    )
    request_ok(resp, {202}, "Zenodo publish")
    return resp.json()


def parse_creator(raw: str) -> dict[str, str]:
    parts = [p.strip() for p in raw.split("|")]
    if not parts or not parts[0]:
        raise ValueError(f"Invalid creator spec: {raw!r}")
    creator: dict[str, str] = {"name": parts[0]}
    if len(parts) >= 2 and parts[1]:
        creator["affiliation"] = parts[1]
    if len(parts) >= 3 and parts[2]:
        creator["orcid"] = parts[2]
    return creator

def parse_keywords(values: list[str] | None) -> list[str]:
    if not values:
        return []

    out: list[str] = []
    for raw in values:
        for part in raw.split(","):
            kw = part.strip()
            if kw and kw not in out:
                out.append(kw)
    return out

def build_metadata(args: argparse.Namespace) -> dict[str, Any]:
    keywords = parse_keywords(args.keyword)

    md: dict[str, Any] = {
        "title": args.title,
        "upload_type": "dataset",
        "description": args.description,
        "creators": [parse_creator(c) for c in args.creator],
        "access_right": args.access_right,
        "license": args.license,
        "prereserve_doi": True,
    }

    # Optional fields that we only include if specified, to avoid overwriting existing metadata on new version drafts
    if args.version:
        md["version"] = args.version

    if args.communities:
        md["communities"] = [{"identifier": x} for x in args.communities]

    if args.grant:
        md["grants"] = [{"id": normalize_grant_id(g)} for g in args.grant]

    if args.subject:
        md["subjects"] = [parse_subject(s) for s in args.subject]

    if args.copyright:
        md["notes"] = (
            f"{md.get('notes', '').strip()}\n\nCopyright: {args.copyright}".strip()
        )
    if keywords:
        md["keywords"] = keywords
    return md


def derive_concept_doi(published: dict[str, Any]) -> str | None:
    if published.get("conceptdoi"):
        return str(published["conceptdoi"])
    version_doi = published.get("doi")
    conceptrecid = published.get("conceptrecid")
    if version_doi and conceptrecid:
        parts = str(version_doi).rsplit(".", 1)
        if len(parts) == 2:
            return f"{parts[0]}.{conceptrecid}"
    return None


def append_log_row(log_file: Path, row: dict[str, Any]) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_exists = log_file.exists()
    fieldnames = [
        "run_at_utc",
        "status",
        "message",
        "api_base",
        "api_path",
        "download_url",
        "filters_json",
        "existing_deposition_id",
        "deposition_id",
        "record_id",
        "conceptrecid",
        "version_doi",
        "concept_doi",
        "prereserved_doi",
        "zenodo_html",
        "latest_draft_html",
        "bucket_url",
        "downloaded_filename",
        "downloaded_size_bytes",
        "upload_filename",
        "upload_size_bytes",
        "wrapped_in_zip",
        "zip_member_name",
        "sandbox",
        "title",
    ]
    with log_file.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download any authenticated API file and publish it to Zenodo"
    )
    parser.add_argument("--env-file", default=".env_zenodo")
    parser.add_argument("--sandbox", action="store_true")
    parser.add_argument("--api-base", required=True)
    parser.add_argument(
        "--api-path",
        default="/canonical/zenodo_bundle.zip",
        help="API path relative to api-base, e.g. /canonical/zenodo_bundle.zip",
    )
    parser.add_argument("--echorepo-api-key")
    parser.add_argument("--echorepo-bearer-token")
    parser.add_argument("--zenodo-access-token")
    parser.add_argument("--existing-deposition-id")
    parser.add_argument("--title", required=True)
    parser.add_argument("--description", required=True)
    parser.add_argument("--creator", action="append", required=True)
    parser.add_argument(
        "--keyword",
        action="append",
        help="Keyword(s), either repeatable or comma-separated, e.g. --keyword soil --keyword biodiversity or --keyword 'soil,biodiversity'",
    )
    parser.add_argument("--communities", nargs="*")
    parser.add_argument("--license", default="CC-BY-4.0")
    parser.add_argument("--access-right", default="open")
    parser.add_argument("--version")
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--country")
    parser.add_argument("--country-code")
    parser.add_argument("--bbox")
    parser.add_argument("--within")
    parser.add_argument(
        "--extra-param",
        action="append",
        help="Additional query parameter in key=value form; repeatable",
    )
    parser.add_argument(
        "--download-name",
        help="Local filename for the downloaded file; defaults to the basename from --api-path",
    )
    parser.add_argument(
        "--wrap-in-zip",
        action="store_true",
        help="Wrap the downloaded file inside a new ZIP before uploading to Zenodo",
    )
    parser.add_argument(
        "--zip-member-name",
        help="Filename to use inside the wrapper ZIP; defaults to the downloaded filename",
    )
    parser.add_argument(
        "--upload-name",
        help="Uploaded filename for Zenodo; defaults to downloaded filename or wrapper ZIP name",
    )
    parser.add_argument("--log-file", default="data/zenodo_sync_log.csv")

    parser.add_argument("--grant", action="append",
                        help="Repeatable Zenodo grant id. Example: 101112869 or 10.13039/501100000780::101112869")

    parser.add_argument("--subject", action="append",
                        help='Repeatable subject as "term|identifier|scheme", e.g. "Soil science|http://id.loc.gov/...|url"')

    parser.add_argument("--copyright",
                        help="Optional copyright statement; stored in Zenodo notes/description, not as a native Zenodo field")
    args = parser.parse_args()

    file_env = load_simple_env_file(args.env_file)

    sandbox = args.sandbox or bool_from_envish(
        env_or_config("ZENODO_SANDBOX", None, file_env, None), default=False
    )
    zenodo_access_token = env_or_config("ACCESS_TOKEN", args.zenodo_access_token, file_env, None)
    echorepo_api_key = env_or_config("ECHOREPO_API_KEY", args.echorepo_api_key, file_env, None)
    echorepo_bearer = env_or_config(
        "ECHOREPO_BEARER_TOKEN", args.echorepo_bearer_token, file_env, None
    )

    if not zenodo_access_token:
        print("ERROR: missing Zenodo access token", file=sys.stderr)
        return 2
    if not echorepo_api_key and not echorepo_bearer:
        print("ERROR: missing ECHOREPO API credentials", file=sys.stderr)
        return 2

    metadata = build_metadata(args)
    filters = build_filter_params(args)
    log_file = Path(args.log_file)

    log_row: dict[str, Any] = {
        "run_at_utc": utc_now_iso(),
        "status": "started",
        "message": "",
        "api_base": args.api_base,
        "api_path": normalize_endpoint_path(args.api_path),
        "download_url": "",
        "filters_json": json.dumps(filters, ensure_ascii=False, sort_keys=True),
        "existing_deposition_id": args.existing_deposition_id or "",
        "deposition_id": "",
        "record_id": "",
        "conceptrecid": "",
        "version_doi": "",
        "concept_doi": "",
        "prereserved_doi": "",
        "zenodo_html": "",
        "latest_draft_html": "",
        "bucket_url": "",
        "downloaded_filename": "",
        "downloaded_size_bytes": "",
        "upload_filename": "",
        "upload_size_bytes": "",
        "wrapped_in_zip": "1" if args.wrap_in_zip else "0",
        "zip_member_name": args.zip_member_name or "",
        "sandbox": "1" if sandbox else "0",
        "title": args.title,
    }

    try:
        echorepo_headers = build_echorepo_headers(echorepo_api_key, echorepo_bearer)
        zenodo_url = build_zenodo_base_url(sandbox)

        with tempfile.TemporaryDirectory(prefix="api_zenodo_sync_") as tmpdir:
            tmpdir_path = Path(tmpdir)

            default_download_name = infer_download_name_from_path(args.api_path)
            download_name = args.download_name or default_download_name
            downloaded_path = tmpdir_path / download_name

            dl_info = download_api_file(
                args.api_base,
                args.api_path,
                echorepo_headers,
                filters,
                downloaded_path,
            )
            log_row["download_url"] = dl_info["download_url"]
            log_row["downloaded_filename"] = downloaded_path.name
            log_row["downloaded_size_bytes"] = str(dl_info["size_bytes"])

            if downloaded_path.stat().st_size == 0:
                raise RuntimeError("Downloaded file is empty")

            upload_path = downloaded_path

            if args.wrap_in_zip:
                upload_name = args.upload_name or (
                    f"{downloaded_path.stem}.zip"
                    if downloaded_path.suffix
                    else f"{downloaded_path.name}.zip"
                )
                upload_path = tmpdir_path / upload_name
                zip_info = maybe_wrap_in_zip(
                    downloaded_path,
                    upload_path,
                    member_name=args.zip_member_name or downloaded_path.name,
                )
                log_row["zip_member_name"] = zip_info["member_name"]
            else:
                if args.upload_name and args.upload_name != downloaded_path.name:
                    renamed = tmpdir_path / args.upload_name
                    renamed.write_bytes(downloaded_path.read_bytes())
                    upload_path = renamed

            if args.existing_deposition_id:
                draft = create_new_version_draft(
                    zenodo_url, zenodo_access_token, args.existing_deposition_id
                )
            else:
                draft = create_new_deposition(zenodo_url, zenodo_access_token)

            deposition_id = str(draft["id"])
            bucket_url = draft["links"]["bucket"]

            log_row["deposition_id"] = deposition_id
            log_row["bucket_url"] = bucket_url
            log_row["latest_draft_html"] = draft.get("links", {}).get("latest_draft_html", "")
            log_row["prereserved_doi"] = (
                draft.get("metadata", {}).get("prereserve_doi", {}).get("doi", "")
            )
            log_row["upload_filename"] = upload_path.name
            log_row["upload_size_bytes"] = str(upload_path.stat().st_size)

            updated = update_metadata(zenodo_url, zenodo_access_token, deposition_id, metadata)
            upload_file_to_bucket(bucket_url, zenodo_access_token, upload_path)
            published = publish_deposition(zenodo_url, zenodo_access_token, deposition_id)

            log_row["status"] = "ok"
            log_row["message"] = "published"
            log_row["record_id"] = str(published.get("record_id", ""))
            log_row["conceptrecid"] = str(published.get("conceptrecid", ""))
            log_row["version_doi"] = str(published.get("doi", ""))
            log_row["concept_doi"] = derive_concept_doi(published) or ""
            log_row["zenodo_html"] = published.get("links", {}).get("html", "")
            log_row["latest_draft_html"] = updated.get("links", {}).get(
                "latest_draft_html", log_row["latest_draft_html"]
            )

            append_log_row(log_file, log_row)

            print(
                json.dumps(
                    {
                        "ok": True,
                        "sandbox": sandbox,
                        "api_download_url": log_row["download_url"],
                        "api_path": log_row["api_path"],
                        "deposition_id": deposition_id,
                        "record_id": log_row["record_id"],
                        "conceptrecid": log_row["conceptrecid"],
                        "version_doi": log_row["version_doi"],
                        "concept_doi": log_row["concept_doi"],
                        "prereserved_doi": log_row["prereserved_doi"],
                        "zenodo_html": log_row["zenodo_html"],
                        "log_file": str(log_file),
                        "filters": filters,
                        "downloaded_filename": log_row["downloaded_filename"],
                        "upload_filename": log_row["upload_filename"],
                        "wrapped_in_zip": args.wrap_in_zip,
                        "zip_member_name": log_row["zip_member_name"],
                        "downloaded_size_bytes": log_row["downloaded_size_bytes"],
                        "upload_size_bytes": log_row["upload_size_bytes"],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

    except Exception as e:
        log_row["status"] = "error"
        log_row["message"] = str(e)
        append_log_row(log_file, log_row)
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
