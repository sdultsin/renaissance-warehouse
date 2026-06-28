"""Shared, deterministic Instantly email-body cleaner (FINALIZED-SPEC §7).

ONE importable module so the email-thread-sync entity AND the F1/F2 IM-outbound
backfill (`scripts/backfill_im_outbound_bodies.py`) share IDENTICAL cleaning
semantics. The cleaner:

  * HTML -> text: strip tags, unescape entities, collapse whitespace.
  * Cuts the quoted reply history at the FIRST quote marker
    (`reply-timestamp-box | reply-body-conatiner | gmail_quote | <blockquote |
     on … wrote: | -----original message`), keeping only the top (new) message.
  * NEVER lets spintax (`{a|b}`) or unresolved merge fields (`{{field}}`) survive
    for a REAL message — real Instantly sends store the already-rendered body, so
    the cleaner is a pass-through for those; the brace-stripper exists only to
    harden the §7 `source='template'` approximation path (G3 asserts 0 brace-pipe
    even on template rows).

Reused, not re-implemented: the HTML cut logic is the proven one from the
backfill_im_outbound_bodies cleaner (2026-06-14), lifted here verbatim and
extended with the two extra prose quote markers the spec §7 lists
(`on … wrote:`, `-----original message`).

Stdlib-only (re + html) so it imports on the droplet venv with no extra deps and
is unit-testable with NO network/db.
"""
from __future__ import annotations

import html as htmllib
import re

# Instantly composes outbound replies as: <top message> then the quoted history wrapped
# in a reply-timestamp-box / reply-body-conatiner block (Gmail forwards use gmail_quote /
# <blockquote>). Plain-text replies use the prose markers "On <date> ... wrote:" or
# "-----Original Message-----". Cut at the first such marker, then strip tags.
_QUOTE_MARKER = re.compile(
    r'reply-timestamp-box'
    r'|reply-body-conatiner'      # NB: Instantly's own (mis)spelling — keep verbatim
    r'|gmail_quote'
    r'|<blockquote'
    r'|on\s.{0,200}?\swrote:'     # "On Tue, ... <a@b> wrote:"
    r'|-{2,}\s*original message',
    re.I | re.S,
)
_BODY_TAG = re.compile(r'<body[^>]*>(.*)</body>', re.I | re.S)
_BR = re.compile(r'(?i)<br\s*/?>')
_BLOCK_CLOSE = re.compile(r'(?i)</(div|p|tr|li|h[1-6]|table)>')
_TAGS = re.compile(r'<[^>]+>')
_WS_EOL = re.compile(r'[ \t]+\n')
_MULTI_NL = re.compile(r'\n{3,}')

# Spintax  {a|b|c}  and unresolved merge  {{field}}  — must NEVER survive in any stored
# body_text / subject (FINALIZED-SPEC §2, §7; G3 gate). A real Instantly message has these
# already collapsed; this is belt-and-suspenders for the template-approximation path.
_SPINTAX = re.compile(r'\{[^{}]*\|[^{}]*\}')
_MERGE = re.compile(r'\{\{[^{}]*\}\}')


def _strip_spintax(s: str) -> str:
    """Collapse any residual spintax / merge brace so G3 can never trip.

    * `{{field}}`  -> ''   (an unresolved merge field is dropped, not kept).
    * `{a|b|c}`    -> first option `a` (the deterministic first-choice collapse,
      matching how a single rendered variant reads). Applied repeatedly so nested
      spintax fully collapses.
    """
    prev = None
    out = s
    # Drop unresolved merge fields first.
    out = _MERGE.sub("", out)
    # Collapse spintax to its first option, iterating until stable (nested spintax).
    while prev != out:
        prev = out
        out = _SPINTAX.sub(lambda m: m.group(0)[1:-1].split("|", 1)[0], out)
    return out


def html_to_text(raw_html: str | None) -> str:
    """Tag-strip + unescape an HTML fragment to plain text (no quote-cut)."""
    if not raw_html:
        return ""
    s = _BR.sub("\n", raw_html)
    s = _BLOCK_CLOSE.sub("\n", s)
    s = _TAGS.sub("", s)
    s = htmllib.unescape(s)
    s = _WS_EOL.sub("\n", s)
    s = _MULTI_NL.sub("\n\n", s)
    return s.strip()


def clean_html(raw_html: str | None, fallback_text: str | None = None) -> str:
    """Extract the clean top message (sans quoted history) from an email body.

    Order: prefer the HTML body (cut quoted history, strip tags); if there is no
    usable HTML, fall back to the plain-text body (still cut at a prose quote
    marker). The result is ALWAYS spintax/merge-stripped (`_strip_spintax`) so a
    `source='template'` approximation can never leak braces past G3.
    """
    if not raw_html or not raw_html.strip():
        return clean_text(fallback_text)
    m = _BODY_TAG.search(raw_html)
    s = m.group(1) if m else raw_html
    qm = _QUOTE_MARKER.search(s)
    if qm:
        # Cut at the START of the enclosing tag (e.g. <div class="reply-timestamp-box">),
        # not at the marker text, so no partial opening tag leaks past the tag-strip.
        cut = s.rfind("<", 0, qm.start())
        s = s[: cut if cut != -1 else qm.start()]
    cleaned = html_to_text(s)
    # Defensive: if cutting left nothing (rare layout), fall back to the full strip.
    if not cleaned:
        cleaned = html_to_text(m.group(1) if m else raw_html)
    cleaned = _strip_spintax(cleaned)
    return cleaned or clean_text(fallback_text)


def clean_text(raw_text: str | None) -> str:
    """Clean a plain-text body: cut quoted history at a prose marker, spintax-strip."""
    if not raw_text or not raw_text.strip():
        return ""
    s = raw_text
    qm = _QUOTE_MARKER.search(s)
    if qm:
        s = s[: qm.start()]
    s = _WS_EOL.sub("\n", s)
    s = _MULTI_NL.sub("\n\n", s)
    return _strip_spintax(s.strip())


def clean_body(item_body: dict | str | None) -> str:
    """Top-level entry for an Instantly `item['body']` ({html, text} | str | None).

    Prefers the HTML render (quote-cut) and falls back to the text field. This is
    the single call site the entity transform uses (FINALIZED-SPEC §4.D `body_text`).
    """
    if item_body is None:
        return ""
    if isinstance(item_body, str):
        return clean_text(item_body)
    if isinstance(item_body, dict):
        return clean_html(item_body.get("html"), item_body.get("text"))
    return ""


def clean_subject(raw_subject: str | None) -> str | None:
    """Spintax/merge-strip a SUBJECT line (FINALIZED-SPEC §7 / G3 — subject is scanned too).

    A real Instantly send stores the already-rendered subject, so this is a pass-through for
    those; but a `source='template'` approximation (or any future hand-built subject) could
    carry `{a|b}` / `{{field}}`, and G3 scans `subject` as well as `body_text`. We therefore
    run the SAME deterministic brace-stripper used on bodies so no spintax/merge can ever
    survive in the stored subject. We do NOT cut quoted history (subjects have none) and we do
    NOT html-strip (subjects are plain) — only collapse braces, preserving the rendered line.
    Returns None for a None input (so the column stays NULL, not ''), '' only for whitespace.
    """
    if raw_subject is None:
        return None
    return _strip_spintax(raw_subject)


def has_spintax_or_merge(s: str | None) -> bool:
    """True if a string still carries spintax `{a|b}` or merge `{{field}}` (G3 probe)."""
    if not s:
        return False
    return bool(_SPINTAX.search(s) or _MERGE.search(s))
