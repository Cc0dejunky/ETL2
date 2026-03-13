"""
process.py
----------
Single-pass ETL: transforms products_export.csv -> products.csv
Updates store_config.json with discovered specs and tags.

Encoding: UTF-8-SIG throughout (Shopify compatibility).
"""

import csv
import json
import os
import re
import sys
import html
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV     = os.path.join(BASE_DIR, "products_export.csv")
OUTPUT_CSV    = os.path.join(BASE_DIR, "products.csv")
CONFIG_PATH   = os.path.join(BASE_DIR, "store_config.json")
TAXONOMY_PATH = os.path.join(BASE_DIR, "taxonomy.json")

# ---------------------------------------------------------------------------
# Module-level Pre-compiled Regex
# ---------------------------------------------------------------------------
# AliExpress / Shopify span-pair: <span>Key</span>: <span>Value</span>
HTML_KV_RE    = re.compile(r"<span[^>]*>(.*?)</span>\s*:?\s*<span[^>]*>(.*?)</span>",
                            re.IGNORECASE | re.DOTALL)
HTML_TAG_RE   = re.compile(r"<[^>]+>")
PIPE_RE       = re.compile(r"\|")
OPTION_COL_RE = re.compile(r"^option\d+\s+(name|value)$", re.IGNORECASE)
MULTI_SPC_RE  = re.compile(r"\s{2,}")

# Title-tag extractors
RYZEN_RE      = re.compile(r"\bryzen\s+\d\b", re.IGNORECASE)
CORE_I_RE     = re.compile(r"\bcore i\d\b", re.IGNORECASE)
MEM_RE        = re.compile(r"\b(\d{1,3})\s*(gb|tb)\b", re.IGNORECASE)

# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------

def load_json(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8-sig") as fh:
            return json.load(fh)
    return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8-sig") as fh:
        json.dump(data, fh, indent=4, ensure_ascii=False)

# ---------------------------------------------------------------------------
# HTML -> Plain-text Key:Value converter
# ---------------------------------------------------------------------------

def html_to_kv(html_text, spec_map):
    """Convert AliExpress / Shopify spec HTML into clean 'Key: Value' lines.
    Applies spec_map to normalize keys. Falls back to stripped plain text."""
    if not html_text or not isinstance(html_text, str):
        return ""
    matches = HTML_KV_RE.findall(html_text)
    pairs = []
    for k, v in matches:
        k_clean = html.unescape(HTML_TAG_RE.sub("", k).strip())
        v_clean = html.unescape(HTML_TAG_RE.sub("", v).strip())
        
        # Strip trailing question marks from some broken keys
        if k_clean.endswith("?"):
            k_clean = k_clean[:-1].strip()

        # Convert affirmative boolean traits to "Function: [Key]" and drop negatives entirely
        if v_clean.lower() in ("yes", "true", "1", "y"):
            v_clean = k_clean.title()
            k_clean = "Function"
        elif v_clean.lower() in ("no", "false", "0", "n"):
            continue

        # Apply normalization map (case-insensitive key match)
        for dirty_key, clean_key in spec_map.items():
            if k_clean.lower() == dirty_key.lower():
                k_clean = clean_key
                break

        # Standardize "Display Size" values to use clean numbers (e.g. 15.6)
        if k_clean.lower() in ("display size", "screen size", "size"):
            # Strip extra words: mm, inches, literal quotes
            v_clean = re.sub(r'(?i)(inches?|inch|mm|"|\')', '', v_clean)
            v_clean = v_clean.strip()
            # If it's a messy range like 2 - 4 or 7,8, just take the first number
            match = re.search(r'(\d+(?:\.\d+)?)', v_clean)
            if match:
                v_clean = f'{match.group(1)}"'
            elif v_clean.lower() == "no":
                 v_clean = "None"

        if k_clean and v_clean:
            for part in v_clean.split(","):
                part = part.strip()
                if part:
                    pairs.append(f"{k_clean}: {part}")
    if pairs:
        return "\n".join(pairs)
    return html.unescape(HTML_TAG_RE.sub("", html_text).strip())

# ---------------------------------------------------------------------------
# String helpers
# ---------------------------------------------------------------------------

def singularize(text):
    """Heuristic: convert plural category breadcrumb words to singular."""
    words = text.split()
    result = []
    for w in words:
        if w.isupper() or len(w) <= 3:
            result.append(w)
            continue
        if w.endswith("ies"):
            w = w[:-3] + "y"
        elif w.endswith("sses"):
            w = w[:-2]
        elif w.endswith("ches") or w.endswith("shes"):
            w = w[:-2]
        elif w.endswith("s") and not w.endswith("ss"):
            w = w[:-1]
        result.append(w)
    return " ".join(result)


def clean_tag_key(raw_key):
    """Normalize a spec key for tag formatting."""
    key = raw_key.strip()
    key = re.sub(r"\s+", " ", key)
    return key.title()

# ---------------------------------------------------------------------------
# Master library updater
# ---------------------------------------------------------------------------

def update_master_lib(kv_text, master):
    """Add new keys/values discovered from the body text into master_library."""
    for line in kv_text.strip().split("\n"):
        if ":" in line:
            parts = line.split(":", 1)
            if len(parts) < 2:
                continue
            k, v = parts[0].strip(), parts[1].strip()
            if not k or not v:
                continue
            if k not in master:
                master[k] = []
            if v not in master[k]:
                master[k].append(v)

# ---------------------------------------------------------------------------
# Vendor matching
# ---------------------------------------------------------------------------

def build_brand_patterns(brands):
    """Return (brand_str, compiled_re) tuples sorted longest-first."""
    sorted_brands = sorted(brands, key=len, reverse=True)
    return [(str(b), re.compile(re.escape(str(b)), re.IGNORECASE)) for b in sorted_brands]


def match_vendor(title_prefix, brand_patterns, fallback):
    """Match first 40 chars of title against brand patterns."""
    snippet = title_prefix[:40]
    for brand, pat in brand_patterns:
        if pat.search(snippet):
            return brand
    return fallback


# Spec fields that commonly hold the brand name
VENDOR_SPEC_KEYS = {"brand", "manufacturer", "make", "maker", "publisher", "brand name", "made by"}

def match_vendor_from_specs(kv_text, brand_patterns, fallback):
    """Secondary vendor lookup: scan 'Brand:' / 'Manufacturer:' spec lines."""
    for line in kv_text.split("\n"):
        if ":" not in line:
            continue
        parts = line.split(":", 1)
        key = parts[0].strip().lower()
        val = parts[1].strip()
        if key in VENDOR_SPEC_KEYS and val:
            for brand, pat in brand_patterns:
                if pat.search(val):
                    return brand
    return fallback

# ---------------------------------------------------------------------------
# Type matching — priority cascade
# ---------------------------------------------------------------------------

def build_type_mappings(taxonomy):
    """Pre-compile keyword patterns from taxonomy, sorted longest keyword first."""
    mappings = []
    for entry in taxonomy:
        ptype = entry.get("type_name")
        if not ptype:
            continue
        for kw in entry.get("identification_keywords", []):
            pat = re.compile(r"\b" + re.escape(str(kw).strip()) + r"\b", re.IGNORECASE)
            mappings.append((pat, ptype, entry))
    # Longer keyword = more specific = higher priority
    mappings.sort(key=lambda x: len(x[1]), reverse=True)
    return mappings


def match_type_and_category(title, cat_parts, type_mappings, taxonomy):
    """
    Priority cascade for product type and Shopify category.

    1. Audio (headphones/headsets) — highest priority to avoid 'phone' false-match
    2. Device keywords in title or breadcrumb (laptop, desktop, tablet, phone)
    3. Computer component keywords
    4. Taxonomy keyword scan
    5. Breadcrumb singularization fallback

    Returns: (type_name, shopify_category, forced_tags[])
    """
    title_lower = title.lower()
    last_path = cat_parts[-1].lower() if cat_parts else ""

    # 1. Audio — must come before 'phone' check
    style = ""
    if any(x in title_lower or x in last_path for x in ("over-ear", "over ear")):
        style = "Over Ear"
    elif any(x in title_lower or x in last_path for x in ("in-ear", "in ear")):
        style = "In Ear"
    elif any(x in title_lower or x in last_path for x in ("on-ear", "on ear")):
        style = "On Ear"
    elif any(x in title_lower or x in last_path for x in ("open-ear", "open ear")):
        style = "Open Ear"

    if style or any(x in title_lower for x in ("headphone", "headset", "earbud", "earphone")):
        ptype = "Headset" if ("mic" in title_lower or "headset" in title_lower) else "Headphones"
        cat = _category_from_taxonomy(ptype, taxonomy,
                                       default="Electronics > Audio > Headphones")
        return ptype, cat, ([style] if style else [])

    # 2. Device keywords
    if "laptop" in last_path or "notebook" in last_path or "laptop" in title_lower or "notebook" in title_lower:
        cat = _category_from_taxonomy("Laptop", taxonomy)
        return "Laptop", cat, []
    if any(x in title_lower or x in last_path for x in ("desktop", "mini pc", "all-in-one pc", "gaming pc")):
        cat = _category_from_taxonomy("Desktop Computer", taxonomy)
        return "Desktop Computer", cat, []
    if "tablet" in last_path or "tablet" in title_lower or "ipad" in title_lower:
        cat = _category_from_taxonomy("Tablet", taxonomy)
        return "Tablet", cat, []
    if "phone" in last_path or "smartphone" in title_lower or "iphone" in title_lower:
        cat = _category_from_taxonomy("Mobile Phone", taxonomy)
        return "Mobile Phone", cat, []

    # 3. Generic board-level components (GPU/CPU/PSU handled by taxonomy)
    comp_words = ("pci-e", "motherboard", "i/o card")
    if any(w in title_lower for w in comp_words):
        return "Computer Component", "Electronics > Computers > Computer Components & Parts", []

    # 4. Taxonomy keyword scan
    for pat, ptype, entry in type_mappings:
        if pat.search(title_lower):
            cat = entry.get("shopify_category", "")
            return ptype, cat, []

    # 5. Breadcrumb fallback
    if cat_parts:
        fallback_type = singularize(cat_parts[-2]) if len(cat_parts) >= 2 else singularize(cat_parts[0])
        return fallback_type, " > ".join(cat_parts[:-1]) if len(cat_parts) > 1 else "", []

    return "", "", []


def _category_from_taxonomy(type_name, taxonomy, default=""):
    for entry in taxonomy:
        if entry.get("type_name") == type_name:
            return entry.get("shopify_category", default)
    return default

# ---------------------------------------------------------------------------
# Tag extraction
# ---------------------------------------------------------------------------

BAD_TAG_VALUES = {"n/a", "none", "-", "", "other", "unknown", "null"}

# High-signal keys that users frequently search by, universally extracted
UNIVERSAL_TAG_KEYS = {
    "CPU", "Processor", "Processor Model", 
    "RAM", "Storage", "Display Size", "Resolution", "Display Resolution", 
    "Graphics Card Type", "GPU", "Graphics Card Model", 
    "Battery Capacity", "Operating System", 
    "Port", "Extend Port", "Interface", "Connectivity", 
    "Function", "Network Communiction"
}

def extract_tags_from_body(kv_text, target_keys, tags_discovered, product_type):
    """Extract tags for keys in target_keys from the cleaned body text."""
    tags = []
    for line in kv_text.split("\n"):
        if ":" not in line:
            continue
        parts = line.split(":", 1)
        k = parts[0].strip()
        v = parts[1].strip()
        if k not in target_keys or not v:
            continue
        for sub in v.split(","):
            sub = sub.strip()
            if sub and sub.lower() not in BAD_TAG_VALUES and sub not in tags:
                tags.append(sub)
                # Update tags_discovered
                type_dict = tags_discovered.setdefault(product_type, {})
                key_list = type_dict.setdefault(k, [])
                if sub not in key_list:
                    key_list.append(sub)
    return tags


def extract_tags_from_title(title):
    """Pull searchable tags from the product title (CPU, memory, OS)."""
    tags = []
    title_lower = title.lower()
    if "ryzen" in title_lower:
        m = RYZEN_RE.search(title_lower)
        tags.append(m.group(0).title() if m else "AMD Ryzen")
    if "core i" in title_lower:
        m = CORE_I_RE.search(title_lower)
        tags.append(m.group(0).title() if m else "Intel Core")
    if "windows 11" in title_lower:
        tags.append("Windows 11")
    elif "windows 10" in title_lower:
        tags.append("Windows 10")
    for m in MEM_RE.finditer(title_lower):
        val = m.group(1) + m.group(2).upper()
        if val not in tags:
            tags.append(val)
    return tags

# ---------------------------------------------------------------------------
# Smart title builder
# ---------------------------------------------------------------------------

def build_smart_title(vendor, product_type, existing_tags):
    """
    Rebuild title as: [Brand] [Type] ([Top spec tags])
    Uses first two meaningful tags as spec callouts.
    """
    top = [t for t in existing_tags if len(t) > 2][:2]
    title = f"{vendor} {product_type}".strip()
    if top:
        title += f" ({', '.join(top)})"
    return title

# ---------------------------------------------------------------------------
# Row helpers
# ---------------------------------------------------------------------------

def replace_pipes(row, header):
    """Replace | with ' / ' in all Option Name/Value columns."""
    for idx, col in enumerate(header):
        if OPTION_COL_RE.match(col) and idx < len(row) and row[idx]:
            replaced = PIPE_RE.sub(" / ", row[idx])
            row[idx] = MULTI_SPC_RE.sub(" ", replaced).strip()


def pad_row(row, target_length):
    if len(row) < target_length:
        row.extend([""] * (target_length - len(row)))
    return row

# ---------------------------------------------------------------------------
# Main ETL
# ---------------------------------------------------------------------------

def run():
    # Sanity checks
    for path, label in ((INPUT_CSV, "products_export.csv"),
                         (CONFIG_PATH, "store_config.json"),
                         (TAXONOMY_PATH, "taxonomy.json")):
        if not os.path.exists(path):
            print(f"[ERROR] Required file not found: {label} ({path})")
            sys.exit(1)

    # Load config & taxonomy
    config   = load_json(CONFIG_PATH, {})
    taxonomy = load_json(TAXONOMY_PATH, [])

    # Load global configs
    config = load_json(CONFIG_PATH, {})
    markup         = float(config.get("markup_multiplier", 3.0))
    fallback_vendor= config.get("fallback_vendor", "Generic")
    brands         = config.get("brands", [])
    static_cols    = config.get("static_columns", {})
    master_lib     = config.get("master_library", {})
    tags_discovered= config.get("tags_discovered", {})
    spec_map       = config.get("spec_normalization_map", {})
    target_tag_keys= config.get("target_tag_keys", [
        "Model Number", "RAM Type", "Color", "Material", "Style",
        "Memory Capacity", "Hard Drive Capacity", "Display Resolution",
        "CPU Model", "Brand Name", "Processor", "RAM", "Storage",
        "Display Size", "Operating System", "Graphics Card", "Cellular",
        "Battery", "Connectivity", "Refresh Rate", "Resolution"
    ])

    # Pre-compile
    brand_patterns = build_brand_patterns(brands)
    type_mappings  = build_type_mappings(taxonomy)

    print("[INFO] Starting ETL process...")
    start_time = datetime.now()

    # Read CSV
    with open(INPUT_CSV, encoding="utf-8-sig", newline="") as fh:
        reader  = csv.reader(fh)
        headers = [h.strip() for h in next(reader)]
        all_rows = list(reader)

    n_cols = len(headers)

    def cidx(name):
        try:
            return headers.index(name)
        except ValueError:
            return -1

    # Column index map
    I = {
        "title":       cidx("Title"),
        "body":        cidx("Body (HTML)"),
        "vendor":      cidx("Vendor"),
        "type":        cidx("Type"),
        "category":    cidx("Product Category") if cidx("Product Category") != -1 else cidx("Product category"),
        "tags":        cidx("Tags"),
        "price":       cidx("Variant Price"),
        "cost":        cidx("Cost per item"),
        "sku":         cidx("Variant SKU"),
        "mpn":         cidx("Google Shopping / MPN"),
        "tracker":     cidx("Variant Inventory Tracker"),
        "fulfillment": cidx("Fulfillment service"),
    }

    # Option column pairs
    option_cols = []
    for n in range(1, 4):
        ni = cidx(f"Option{n} Name")
        vi = cidx(f"Option{n} Value")
        if ni != -1 and vi != -1:
            option_cols.append((ni, vi))

    # Static column index map (resolved once)
    static_map = {cidx(k): str(v) for k, v in static_cols.items() if cidx(k) != -1}

    # Stats
    stats = {k: 0 for k in ("html", "type", "category", "vendor", "tags", "price", "pipes", "mpn")}
    last_tracker = ""

    def get_val(r, i):
        return r[i].strip() if i != -1 and i < len(r) else ""

    def put_val(r, i, val):
        if i != -1 and i < len(r):
            r[i] = val

    for row in all_rows:
        pad_row(row, n_cols)

        title    = get_val(row, I["title"])
        is_main  = bool(title)
        
        # A row is a variant or main row if it has a SKU, or option values, or is the main row.
        # Otherwise, it's likely an image-only row.
        is_variant = any(get_val(row, vi) for ni, vi in option_cols)
        has_sku    = bool(get_val(row, I["sku"]))
        is_data_row = is_main or is_variant or has_sku

        # ------------------------------------------------------------------
        # Tracker cascade (only for data rows)
        # ------------------------------------------------------------------
        curr_tracker = get_val(row, I["tracker"])
        if curr_tracker:
            last_tracker = curr_tracker
        elif last_tracker and is_data_row:
            put_val(row, I["tracker"], last_tracker)
            if I["fulfillment"] != -1:
                put_val(row, I["fulfillment"], last_tracker)

        # ------------------------------------------------------------------
        # 1. HTML -> Key:Value body rewrite (all rows with body content)
        # ------------------------------------------------------------------
        bi = I["body"]
        kv_text = ""
        if bi != -1 and get_val(row, bi):
            original = get_val(row, bi)
            kv_text  = html_to_kv(original, spec_map)
            if kv_text != original:
                put_val(row, bi, kv_text)
                stats["html"] += 1
            update_master_lib(kv_text, master_lib)

        # ------------------------------------------------------------------
        # 2. Pipe cleanup in option columns (all rows)
        # ------------------------------------------------------------------
        for ni, vi in option_cols:
            for oi in (ni, vi):
                if oi < len(row) and "|" in row[oi]:
                    row[oi] = PIPE_RE.sub(" / ", row[oi])
                    row[oi] = MULTI_SPC_RE.sub(" ", row[oi]).strip()
                    stats["pipes"] += 1

        # ------------------------------------------------------------------
        # 3. Main row enrichment
        # ------------------------------------------------------------------
        if is_main:
            title_lower = title.lower()

            # Vendor — title first, body specs as fallback
            vendor = match_vendor(title, brand_patterns, fallback_vendor)
            if vendor == fallback_vendor and kv_text:
                vendor = match_vendor_from_specs(kv_text, brand_patterns, fallback_vendor)
            put_val(row, I["vendor"], vendor)
            stats["vendor"] += 1

            # Type + Category
            existing_cat = get_val(row, I["category"])
            cat_parts = [p.strip() for p in str(existing_cat).split(">")] if existing_cat else []
            ptype, shopify_cat, forced_tags = match_type_and_category(
                title, cat_parts, type_mappings, taxonomy
            )

            if ptype:
                put_val(row, I["type"], ptype)
                stats["type"] += 1
            if shopify_cat:
                put_val(row, I["category"], shopify_cat)
                stats["category"] += 1

            # Tags — body + title sources merged
            existing_tags_str = get_val(row, I["tags"])
            existing = [t.strip() for t in str(existing_tags_str).split(",")
                        if t.strip() and t.strip().lower() not in BAD_TAG_VALUES]
            seen = {t.lower() for t in existing}

            # Use common_specs + high-signal universal keys as tag filters
            matched_entry = next((e for e in taxonomy if e.get("type_name") == ptype), None)
            base_specs = matched_entry.get("common_specs", []) if matched_entry else []
            type_spec_keys = set(base_specs) | UNIVERSAL_TAG_KEYS
            body_tags  = extract_tags_from_body(kv_text, type_spec_keys, tags_discovered, ptype) if kv_text else []
            title_tags = extract_tags_from_title(title)

            # Identification keywords from the matched taxonomy entry that appear in the title
            kw_tags = []
            if ptype:
                for entry in taxonomy:
                    if entry.get("type_name") == ptype:
                        for kw in entry.get("identification_keywords", []):
                            kw_str = str(kw)
                            if kw_str.lower() in title_lower and kw_str not in kw_tags:
                                kw_tags.append(kw_str.title())
                        break

            # Brand and type are always tagged explicitly
            meta_tags = [t for t in [vendor, ptype] if t and t != fallback_vendor]

            for t in (body_tags + title_tags + forced_tags + kw_tags + meta_tags):
                if t and str(t).lower() not in BAD_TAG_VALUES and str(t).lower() not in seen:
                    existing.append(t)
                    seen.add(str(t).lower())

            put_val(row, I["tags"], ", ".join(existing))
            stats["tags"] += 1

            # Smart title rebuild: [Brand] [Type] (Spec1, Spec2)
            if ptype and vendor:
                smart = build_smart_title(vendor, ptype, existing)
                put_val(row, I["title"], smart)

        else:
            # Variant rows: clear Type and Category to avoid Shopify duplication
            put_val(row, I["type"], "")
            put_val(row, I["category"], "")

        # ------------------------------------------------------------------
        # 4. Pricing — only overwrite if price is missing or equals cost
        # ------------------------------------------------------------------
        pi, ci = I["price"], I["cost"]
        if pi != -1 and ci != -1:
            cost_str  = get_val(row, ci)
            price_str = get_val(row, pi)
            if cost_str:
                try:
                    cost  = float(str(cost_str).replace(",", ""))
                    price = float(str(price_str).replace(",", "")) if price_str else 0.0
                    if price == 0.0 or price == cost:
                        put_val(row, pi, f"{cost * markup:.2f}")
                        stats["price"] += 1
                except ValueError:
                    pass

        # ------------------------------------------------------------------
        # 5. Static columns (only for data rows)
        # ------------------------------------------------------------------
        if is_data_row:
            for col_idx, val in static_map.items():
                put_val(row, col_idx, val)

        # ------------------------------------------------------------------
        # 6. MPN — always copy SKU
        # ------------------------------------------------------------------
        sku = get_val(row, I["sku"])
        if sku and I["mpn"] != -1:
            put_val(row, I["mpn"], sku)
            stats["mpn"] += 1

        # Final padding guarantee
        pad_row(row, n_cols)

    # Write output
    with open(OUTPUT_CSV, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        writer.writerows(all_rows)

    # Save updated config
    config["master_library"]  = master_lib
    config["tags_discovered"] = tags_discovered
    save_json(CONFIG_PATH, config)

    elapsed    = (datetime.now() - start_time).total_seconds()
    total_rows = len(all_rows)
    print(
        f"\n[DONE] {total_rows} rows in {elapsed:.3f}s | "
        f"HTML: {stats['html']} | Types: {stats['type']} | "
        f"Vendors: {stats['vendor']} | Tags: {stats['tags']} | "
        f"Price: {stats['price']} | MPN: {stats['mpn']}"
    )
    print(f"[OUT]  -> {OUTPUT_CSV}")
    print(f"[CFG]  -> {CONFIG_PATH} updated")


if __name__ == "__main__":
    run()
