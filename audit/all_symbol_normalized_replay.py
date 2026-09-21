import sqlite3,json,sys,collections,datetime,bisect
sys.path.insert(0,'/mnt/data/Compression-Radar-6.1.2')
from radar.strategy import initial,observe
from radar.config import Config
from radar.model import event_from_payload, Event
src='/mnt/data/forensic_replay/live_history/data-paper-v61/radar.db'
con=sqlite3.connect(src)
cfg=Config(**json.loads(con.execute("select value from manifest where key='config'").fetchone()[0]))
# universe timeline
ut=[]
for seq,p in con.execute("select seq,payload from events where payload like '%\"kind\":\"UNIVERSE\"%' order by seq"):
    e=event_from_payload(p); ut.append((seq,set(e.data.get('selected',[]))))
useq=[x[0] for x in ut]
def selected_at(seq,sym):
    i=bisect.bisect_right(useq,seq)-1
    return i>=0 and sym in ut[i][1]
def norm(e):
    if e.kind=='GAP': return None
    if e.kind=='BAR': rec=max(e.at,e.data.get('close_ms',e.at))+2500
    elif e.kind in {'TRADE','OI','META'}: rec=e.at+100
    else: rec=e.at
    return Event(kind=e.kind,symbol=e.symbol,key=e.key,at=e.at,received=rec,data=e.data,version=e.version,raw=e.raw,processed_ms=rec)
states={}; attempts={}; candidates=[]; reason_counts=collections.Counter();
for idx,(seq,p) in enumerate(con.execute('select seq,payload from events order by seq')):
    oe=event_from_payload(p)
    if oe.kind in {'UNIVERSE','HEALTH'}: continue
    e=norm(oe)
    if e is None: continue
    sym=e.symbol
    if not sym: continue
    s=states.setdefault(sym,initial())
    # same closed-bar handling as prior focused replay
    if e.kind=='BAR' and e.data.get('close_ms',e.received)>=e.received-cfg.clock_uncertainty_ms: continue
    cand=observe(s,e,cfg)
    reason_counts[s.get('reason')]+=1
    a=s.get('attempt')
    if a:
        aid=a['id']
        rec=attempts.setdefault(aid,dict(symbol=sym,direction=a['direction'],started=a['started'],episode=(s.get('episode') or {}).get('id'),lower=(s.get('episode') or {}).get('lower'),upper=(s.get('episode') or {}).get('upper'),first_seq=seq,last_seq=seq,reasons=collections.Counter(),selected_any=False))
        rec['last_seq']=seq; rec['reasons'][s.get('reason')]+=1; rec['selected_any'] |= selected_at(seq,sym)
    if cand:
        candidates.append(dict(seq=seq,symbol=sym,time=e.decision_ms,price=e.data.get('price'),selected=selected_at(seq,sym),cand=cand))
print('ATTEMPTS',len(attempts),'CANDIDATES',len(candidates))
by_sym=collections.Counter(x['symbol'] for x in candidates)
print('CANDIDATE_SYMBOLS',json.dumps(by_sym,sort_keys=True))
uniq={}
for x in candidates:
    aid=x['cand']['id']
    u=uniq.setdefault(aid,dict(symbol=x['symbol'],kind=x['cand']['explanation']['kind'],direction=x['cand']['direction'],first=x,last=x,count=0,routes=collections.Counter(),selected_count=0))
    u['last']=x;u['count']+=1;u['routes'][x['cand']['explanation']['route']]+=1;u['selected_count']+=int(x['selected'])
print('UNIQUE_CANDIDATE_ATTEMPTS',len(uniq))
for aid,u in sorted(uniq.items(),key=lambda kv:kv[1]['first']['time']):
    f=u['first'];c=f['cand'];
    print('CAND_ATTEMPT',aid,u['symbol'],u['kind'],datetime.datetime.fromtimestamp(f['time']/1000,datetime.timezone.utc).isoformat(),'selected',f['selected'],'count',u['count'],'selected_count',u['selected_count'],'routes',dict(u['routes']),'trigger',c['trigger'],'stop',c['stop'],'mid',c['mid'],'far',c['far'],'excursion',c['explanation']['excursion'],'best_response',c['explanation']['best_response'])
print('ALL_ATTEMPTS')
for aid,a in sorted(attempts.items(),key=lambda kv:kv[1]['started']):
    print('ATTEMPT',aid,a['symbol'],a['direction'],datetime.datetime.fromtimestamp(a['started']/1000,datetime.timezone.utc).isoformat(),'bounds',a['lower'],a['upper'],'selected_any',a['selected_any'],'reasons',dict(a['reasons']))
