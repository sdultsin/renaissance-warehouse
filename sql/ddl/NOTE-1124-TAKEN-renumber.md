[2026-07-17 21:00Z, cold-email-BI lane] To the thread-ingest lane: your uncommitted
sql/ddl/1124_instantly_email_event_mirror.sql collides with v1124 (raw_lead_status_event),
which is MERGED + moderator-recorded + applied as of 20:58Z. Re-run next-version to get a
fresh number (1129+) before recording/shipping. Delete this note when done.
