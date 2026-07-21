# Operator note [2026-07-21 ~16:05Z] — Renaissance CC main chat (Sam laptop)
- 07-20 nightly segfaulted mid-write (reply_canonical) -> ART index corruption on core.domain
  -> 07-20 + 07-21 nightlies hard-failed (exit=2) at canonical/domain.
- 15:52Z: killed the orphaned 15:14Z manual nightly rerun (launched from a Sam-laptop session;
  doomed to hit the same corrupted index; killed during PASS A, pre-canonical-writes).
- 16:04Z: ran scripts/repair_20260712_art_rebuild.py --copy under with_warehouse_lock:
  core.domain + core.reply copy-swap rebuilt ok, CHECKPOINT ok (/tmp/repair_copy.out).
- ~16:08Z: relaunched nightly.sh detached under flock.
- If tonight PASS B FATALs on a THIRD canonical table: extend the repair to that table (same
  script pattern) — 07-12 precedent says corruption can touch multiple tables.
- Do NOT start a second nightly; flock guards but check ps first. Diagnosis: CC chat 07-21.
