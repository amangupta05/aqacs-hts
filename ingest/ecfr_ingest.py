# Title '35' is an invalid url parameter - valid titles are numbered 1-34 and 36-50.


import argparse
import json
import time
from pathlib import Path

import requests
from lxml import etree

BASE_URL = "https://www.ecfr.gov"


def fetch_titles():
    """Fetch the list of titles from eCFR."""
    url = f"{BASE_URL}/api/versioner/v1/titles.json"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data["titles"]


def download_title_xml(date, title, out_dir):
    """
    Download full XML for a given title on a specific date.

    Args:
        date (str): snapshot date YYYY-MM-DD
        title (str): title number as string, e.g. "19" --- We might only need 19 for this
        out_dir (Path): directory to store XML

    Returns:
        Path: path to the saved XML file
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    url = f"{BASE_URL}/api/versioner/v1/full/{date}/title-{title}.xml"
    print(f"  GET {url}")
    resp = requests.get(url, timeout=120)
    if resp.status_code != 200:
        print(f"  Error {resp.status_code} for title {title} at date {date}: {resp.text}")
        resp.raise_for_status()
    out_path = out_dir / f"title-{title}.xml"
    out_path.write_bytes(resp.content)
    return out_path


def get_head_text(elem):
    head = elem.find("./HEAD")
    if head is not None:
        text = " ".join(head.itertext()).strip()
        return text or None
    return None


def get_paragraph_text(elem):
    """Collect all <P> descendants as plain text with blank lines between paragraphs."""
    parts = []
    for p in elem.findall(".//P"):
        t = " ".join(p.itertext()).strip()
        if t:
            parts.append(t)
    return "\n\n".join(parts).strip()


def find_ancestor_div(elem, div_type):
    """Walk up the tree to find the nearest ancestor DIVn with TYPE=div_type."""
    parent = elem.getparent()
    while parent is not None:
        if parent.tag.startswith("DIV") and parent.get("TYPE") == div_type:
            return parent
        parent = parent.getparent()
    return None


def extract_section(div8, snapshot_date, title_meta):
    """
    Given a <DIV8 TYPE="SECTION"> element, extract a section-level document.
    """
    head = get_head_text(div8) or ""
    text = get_paragraph_text(div8)
    if not text:
        return None

    section_num = div8.get("N")
    hierarchy_meta_raw = div8.get("hierarchy_metadata")
    citation = None
    path = None
    if hierarchy_meta_raw:
        try:
            hm = json.loads(hierarchy_meta_raw)
            citation = hm.get("citation")
            path = hm.get("path")
        except json.JSONDecodeError:
            pass

    part_div = find_ancestor_div(div8, "PART")
    subpart_div = find_ancestor_div(div8, "SUBPART")

    part_num = part_div.get("N") if part_div is not None else None
    subpart_id = subpart_div.get("N") if subpart_div is not None else None

    # title can be under "title" or "number"
    title_num = title_meta.get("title") or title_meta.get("number")

    doc = {
        "snapshot_date": snapshot_date,
        "source": "ecfr",
        "title": str(title_num),
        "title_name": title_meta.get("name"),
        "section": section_num,
        "part": part_num,
        "subpart": subpart_id,
        "heading": head,
        "citation": citation,
        "path": path,
        "node_type": "section",
        "text": text,
    }
    return doc


def parse_title_xml(xml_path, snapshot_date, title_meta):
    """
    Parse a title XML file into a list of section-level docs.
    """
    tree = etree.parse(str(xml_path))
    root = tree.getroot()

    sections = []
    for div8 in root.xpath(".//DIV8[@TYPE='SECTION']"):
        doc = extract_section(div8, snapshot_date, title_meta)
        if doc and doc.get("text"):
            sections.append(doc)
    return sections


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True, help="ECFR-YYYY-MM-DD (label for this run)")
    parser.add_argument("--root", required=True, help="Root snapshots directory")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD eCFR snapshot date to use in API calls")
    args = parser.parse_args()

    snapshot_label = args.snapshot      # "US-ECFR-YYYY-MM-DD"
    snapshot_date = args.date          # "YYYY-MM-DD"

    root_dir = Path(args.root)
    snapshot_dir = root_dir / snapshot_label
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    
    xml_dir = snapshot_dir / "xml"
    xml_dir.mkdir(parents=True, exist_ok=True)

    
    manifest_path = snapshot_dir / "manifest.jsonl"

    print(f"Using snapshot label: {snapshot_label}")
    print(f"Using eCFR API date:  {snapshot_date}")
    print(f"Snapshot base dir:    {snapshot_dir}")

    titles = fetch_titles()
    print(f"Found {len(titles)} titles in titles.json")

    with manifest_path.open("w", encoding="utf-8") as out_f:
        for t in titles:
            title_num = str(t.get("title") or t.get("number"))

            # Explicitly skip Title 35 (reserved / invalid)
            if title_num == "35":
                print("\n=== Skipping Title 35 (reserved / invalid for /full/ API) ===")
                continue

            print(f"\n=== Title {title_num} – {t.get('name')} ===")

            try:
                xml_path = download_title_xml(snapshot_date, title_num, xml_dir)
            except requests.HTTPError as e:
                print(f"  Failed to download title {title_num}: {e}")
                continue

            print(f"  Parsing {xml_path}")
            sections = parse_title_xml(xml_path, snapshot_date, t)
            print(f"  Extracted {len(sections)} sections")

            for doc in sections:
                out_f.write(json.dumps(doc, ensure_ascii=False) + "\n")

            time.sleep(1)

    print(f"\nDone. Manifest written to: {manifest_path}")


if __name__ == "__main__":
    main()
