import duckdb, boto3, subprocess
from botocore.config import Config
def e(k):
    for l in open("/root/renaissance-warehouse/.env"):
        if l.startswith(k+"="): return l.split("=",1)[1].strip().strip('"').strip()
SNAP=subprocess.check_output(["readlink","-f","/opt/duckdb/warehouse_current.duckdb"]).decode().strip()
c=duckdb.connect(SNAP, read_only=True)
tbls=c.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_type='BASE TABLE'").fetchall()
a=e("R2_ACCOUNT_ID")
s=boto3.client("s3",endpoint_url="https://%s.r2.cloudflarestorage.com"%a,aws_access_key_id=e("R2_ACCESS_KEY_ID"),aws_secret_access_key=e("R2_SECRET_ACCESS_KEY"),config=Config(signature_version="s3v4"),region_name="auto")
present=set()
for pg in s.get_paginator("list_objects_v2").paginate(Bucket="renaissance-warehouse-escrow",Prefix="escrow/dt=2026-07-10/"):
    for o in pg.get("Contents",[]): present.add(o["Key"].split("/")[-1])
missing=[f"{sc}.{tn}" for sc,tn in tbls if f"{sc}.{tn}.parquet" not in present]
print("snapshot base tables:", len(tbls))
print("escrow objects today:", len(present))
print("tables WITH escrow copy: %d/%d" % (len(tbls)-len(missing), len(tbls)))
print("MISSING:", missing if missing else "NONE - full coverage OK")
c.close()
