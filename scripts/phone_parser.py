#!/usr/bin/env python3
"""US phone parser + signature-block extractor for the signature->phone enrichment.

All numbers assumed US. Output is E.164 `+1XXXXXXXXXX` or None.
Spec: handoffs/2026-06-12-signature-phone-warehouse-native.md (directive 5).
"""
from __future__ import annotations

import re

# Obvious fakes / placeholders (10-digit national form)
FAKE_NATIONAL = {
    "0000000000",
    "1111111111",
    "2222222222",
    "3333333333",
    "4444444444",
    "5555555555",
    "6666666666",
    "7777777777",
    "8888888888",
    "9999999999",
    "1234567890",
    "0123456789",
    "9876543210",
}

# 555-01XX is the reserved fictional block
_FICTIONAL_555 = re.compile(r"^\d{3}55501\d{2}$")


def normalize_us_phone(raw: str) -> str | None:
    """Normalize one candidate phone string to E.164 +1XXXXXXXXXX, else None.

    Accepts every common US format: (757) 685-7284, 757-685-7284, 757.685.7284,
    7576857284, +1 757 685 7284, 1-757-685-7284, 757 685 7284, tel:+17576857284,
    'Mobile: 757.685.7284', trailing extensions (x123 / ext. 5).
    """
    if not raw:
        return None
    s = raw.strip()
    # Fax-labeled values are not callable phones
    if re.match(r"(?i)^\s*f(?:ax)?\s*[#:.\-]", s):
        return None
    # Strip URI scheme and label prefixes ("tel:", "Mobile:", "Cell -", "Ph#")
    s = re.sub(r"(?i)^\s*(?:tel:|callto:|sms:)", "", s)
    s = re.sub(r"(?i)^\s*(?:mobile|cell|phone|direct|office|work|tel|ph|p|m|c|d|o|t)\s*[#:.\-]*\s*", "", s)
    # Cut anything after an extension marker
    s = re.split(r"(?i)[,;]?\s*(?:ext\.?|x|extension)\s*\d{1,6}\s*$", s)[0]
    digits = re.sub(r"\D", "", s)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    # NANP validity: area code and exchange can't start with 0/1
    if digits[0] in "01" or digits[3] in "01":
        return None
    if digits in FAKE_NATIONAL or _FICTIONAL_555.match(digits):
        return None
    # All-same-digit already covered; reject 7+ repeats of one digit too
    if re.match(r"^(\d)\1{6,}", digits):
        return None
    return "+1" + digits


# Candidate phone patterns inside free text. Deliberately broad; every hit is
# re-validated by normalize_us_phone, so false positives just normalize to None.
_CANDIDATE = re.compile(
    r"""
    (?:(?:tel|callto|sms):)?            # uri scheme
    (?:\+?1[\s.\-]?)?                   # country code
    \(?\d{3}\)?                         # area code, optional parens
    [\s.\-]?\d{3}                       # exchange
    [\s.\-]?\d{4}                       # subscriber
    (?:\s*(?:ext\.?|x|extension)\s*\d{1,6})?   # extension
    """,
    re.VERBOSE,
)

# Things that look like phones but aren't: order/tracking/zip+4 contexts.
_BAD_CONTEXT = re.compile(
    r"(?i)(?:order|invoice|account|tracking|ticket|case|ref(?:erence)?|conf(?:irmation)?|zip|fax)\s*(?:number|num|no\.?|#)?\s*[:#]?\s*$"
)
# A 10-digit run glued inside a longer digit/word run (e.g. id 982317576857284) is not a phone.
_EMBEDDED = re.compile(r"\d")


def extract_phones(text: str) -> list[str]:
    """Extract all valid US phones from free text, normalized E.164, in order, deduped."""
    if not text:
        return []
    found: list[str] = []
    seen: set[str] = set()
    for m in _CANDIDATE.finditer(text):
        start, end = m.start(), m.end()
        # Reject when glued to adjacent digits (longer number / id)
        if start > 0 and _EMBEDDED.match(text[start - 1]):
            continue
        if end < len(text) and _EMBEDDED.match(text[end]):
            continue
        # Reject explicit non-phone labels immediately before the match (incl. fax)
        if _BAD_CONTEXT.search(text[max(0, start - 24):start]):
            continue
        e164 = normalize_us_phone(m.group(0))
        if e164 and e164 not in seen:
            seen.add(e164)
            found.append(e164)
    return found


# ---------------- signature-block isolation ----------------

# Quoted-history markers: everything at/below the FIRST of these is not the
# inbound author's own text. ("Sent from my iPhone" is the author's own tail —
# deliberately NOT a marker.)
_QUOTE_MARKERS = [
    re.compile(r"(?im)^\s*>?\s*On .{4,120} wrote:\s*$"),
    re.compile(r"(?im)^\s*-{2,}\s*Original Message\s*-{2,}\s*$"),
    re.compile(r"(?im)^\s*-{2,}\s*Forwarded message\s*-{2,}\s*$"),
    re.compile(r"(?im)^\s*From:\s.+$"),    # Outlook-style reply header block
    re.compile(r"(?im)^\s*_{10,}\s*$"),    # Outlook divider
]


def author_portion(body: str) -> str:
    """Return only the inbound author's own text: strip quoted history below
    'On ... wrote:' / '-----Original Message-----' / 'From: ...' header blocks,
    and strip '>'-prefixed quote lines."""
    if not body:
        return ""
    cut = len(body)
    for rx in _QUOTE_MARKERS:
        m = rx.search(body)
        if m:
            cut = min(cut, m.start())
    own = body[:cut]
    lines = [ln for ln in own.splitlines() if not ln.lstrip().startswith(">")]
    return "\n".join(lines)


def signature_phones(body: str, tail_lines: int = 15) -> list[str]:
    """Phones from the signature block: last `tail_lines` non-empty lines of the
    author's own portion. Falls back to the whole author portion when short."""
    own = author_portion(body)
    if not own.strip():
        return []
    nonempty = [ln for ln in own.splitlines() if ln.strip()]
    tail = "\n".join(nonempty[-tail_lines:])
    return extract_phones(tail)


_DIRECT_LABEL = re.compile(r"(?i)\b(?:mobile|cell|direct|c|m)\s*[#:.\-]")


def best_signature_phone(body: str, tail_lines: int = 15) -> str | None:
    """Single best phone from the signature: prefer a mobile/cell/direct-labeled
    line, else the first phone found."""
    own = author_portion(body)
    if not own.strip():
        return None
    nonempty = [ln for ln in own.splitlines() if ln.strip()]
    tail_list = nonempty[-tail_lines:]
    phones = extract_phones("\n".join(tail_list))
    if not phones:
        return None
    if len(phones) == 1:
        return phones[0]
    for ln in tail_list:
        if _DIRECT_LABEL.search(ln):
            hit = extract_phones(ln)
            if hit:
                return hit[0]
    return phones[0]


_TAG_BREAK = re.compile(r"(?i)<\s*(?:br|/p|/div|/tr|/li|/h[1-6])\s*/?\s*>")
_TAG_ANY = re.compile(r"<[^>]+>")


def html_to_text(html: str) -> str:
    """Cheap HTML->text good enough for signature parsing: structural tags become
    newlines, all other tags drop, entities unescape."""
    import html as _html

    if not html:
        return ""
    s = re.sub(r"(?is)<(?:style|script|head)[^>]*>.*?</(?:style|script|head)>", " ", html)
    s = _TAG_BREAK.sub("\n", s)
    s = _TAG_ANY.sub(" ", s)
    return _html.unescape(s)
