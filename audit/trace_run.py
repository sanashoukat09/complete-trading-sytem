import sqlite3,json,collections,sys
import importlib.util
from pathlib import Path
spec=importlib.util.spec_from_file_location('radar.baseline_strategy',Path(__file__).with_name('baseline_strategy.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
initial,observe=module.initial,module.observe
from radar.model import Event
from radar.config import Config
c=sqlite3.connect(sys.argv[1]);cfg=Config(**json.loads(c.execute("select value from manifest where key='config'").fetchone()[0]))
states={};selected=set();episodes={};reasons=collections.Counter();candidates=[];changes=[]
for seq,p in c.execute('select seq,payload from events order by seq'):
 e=Event(**json.loads(p))
 if e.kind=='UNIVERSE':
  selected=set(e.data['selected']);changes.append({'at':e.received,'selected':sorted(selected)});continue
 s=states.setdefault(e.symbol,initial());a=observe(s,e,cfg)
 if e.kind=='TRADE':reasons[s['reason']]+=1
 ep=s['episode']
 if ep:
  key=e.symbol+':'+ep['id'];r=episodes.setdefault(key,dict(symbol=e.symbol,**ep,events=collections.Counter(),selected_events=0,attempt_stages=collections.Counter(),min_trade=None,max_trade=None))
  r['events'][e.kind]+=1;r['selected_events']+=e.symbol in selected
  if s['attempt']:r['attempt_stages'][s['attempt']['stage']]+=1
  if e.kind=='TRADE':
   p=e.data['price'];r['min_trade']=min(r['min_trade'] or p,p);r['max_trade']=max(r['max_trade'] or p,p)
 if a:candidates.append(dict(seq=seq,symbol=e.symbol,at=e.received,candidate=a))
print(json.dumps(dict(episodes=list(episodes.values()),trade_reasons=reasons,candidates=candidates,universe_changes=changes),indent=2))
