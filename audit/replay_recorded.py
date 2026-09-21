"""Counterfactual repaired-code replay of recorded inputs; does not fill missing streams."""
import sqlite3,json,sys,time,hashlib
from pathlib import Path
from radar.engine import Engine
from radar.config import Config
from radar.model import Event,event_from_payload
source=Path(sys.argv[1]);dest=Path(sys.argv[2])
if dest.exists():raise SystemExit('Destination must be new')
c=sqlite3.connect(source.resolve().as_uri()+'?mode=ro',uri=True)
config=Config(**json.loads(c.execute("select value from manifest where key='config'").fetchone()[0]))
e=Engine(dest,config);start=time.perf_counter();batch=[]
for payload, in c.execute('select payload from events order by seq'):
 batch.append(event_from_payload(payload))
 if len(batch)==32:e.ingest_many(batch);batch=[]
if batch:e.ingest_many(batch)
r=e.status();r['seconds']=time.perf_counter()-start;r['source_sha256']=hashlib.sha256(source.read_bytes()).hexdigest();r['interpretation']='Changed-code replay of available inputs only. Missing subscriptions and network conditions cannot be reconstructed.'
r['reason_counts']=[dict(x) for x in e.db.execute('select action,reason,count(*) as count from decisions group by action,reason order by count desc')]
print(json.dumps(r,indent=2));e.close();c.close()
