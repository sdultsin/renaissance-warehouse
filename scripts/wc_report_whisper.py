import sys, json, os, tempfile
from pathlib import Path
sys.path.insert(0, "/root/renaissance-warehouse")
from core import db as db_module
from core.credentials import load_credentials
from entities.call_transcription import _download_recording
SNAP="/opt/duckdb/warehouse_current.duckdb"; OUT="/tmp/wc_jun1215.jsonl"
api_key=load_credentials().require("CLOSE_API_KEY")
ro=db_module.connect(Path(SNAP), read_only=True)
rows=ro.execute("""
  SELECT c.call_id, CAST(c.occurred_at AS DATE)::VARCHAR, c.duration_seconds, c.caller_name, c.lead_email, c.disposition, c.recording_url, o.outcome_class, o.note
  FROM core.call c LEFT JOIN core.call_outcome o ON o.call_id=c.call_id
  WHERE CAST(c.occurred_at AS DATE) IN (DATE '2026-06-12', DATE '2026-06-15')
    AND c.duration_seconds>=60 AND c.has_recording AND c.recording_url IS NOT NULL
  ORDER BY c.occurred_at
""").fetchall()
ro.close()
print(f"to whisper: {len(rows)} substantive calls", flush=True)
os.nice(10)
from faster_whisper import WhisperModel
model=WhisperModel("base", device="cpu", compute_type="int8")
done=0
with open(OUT,"w") as out:
  for call_id,d,dur,caller,email,disp,url,oc,note in rows:
    fd,tmp=tempfile.mkstemp(suffix=".mp3"); os.close(fd); tmp=Path(tmp)
    try:
      _download_recording(api_key, url, tmp)
      segs,info=model.transcribe(str(tmp), language="en")
      text=" ".join(s.text for s in segs).strip()
    except Exception as e:
      text=f"[FAILED:{str(e)[:100]}]"
    finally:
      try: tmp.unlink()
      except: pass
    out.write(json.dumps({"call_id":call_id,"date":d,"dur":dur,"caller":caller,"email":email,"disposition":disp,"outcome":oc,"note":note,"transcript":text})+"\n"); out.flush()
    done+=1
    if done%5==0: print(f"  {done}/{len(rows)}", flush=True)
print(f"WHISPER DONE {done}/{len(rows)}", flush=True)
