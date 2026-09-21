import sqlite3,json,sys,datetime,multiprocessing as mp
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from radar.strategy import initial,observe
from radar.config import Config
from radar.model import event_from_payload,Event
SRC='/mnt/data/forensic_replay/live_history/data-paper-v61/radar.db'

def norm(e,cfg):
    if e.kind=='GAP':return None
    if e.kind=='BAR':rec=max(e.at,e.data.get('close_ms',e.at))+2500
    elif e.kind in {'TRADE','OI','META'}:rec=e.at+100
    else:rec=e.at
    return Event(e.kind,e.symbol,e.key,e.at,rec,e.data,e.version,e.raw,rec)

def work(sym):
    con=sqlite3.connect(SRC)
    cfg=Config(**json.loads(con.execute("select value from manifest where key='config'").fetchone()[0]))
    prefix=f'binance:USDT-PERP:{sym}:';hi=prefix+'\uffff'
    rows=con.execute('select seq,payload from events where identity>=? and identity<? order by seq',(prefix,hi)).fetchall()
    s=initial();cands=[];attempt_ids=set();trades=[]
    for seq,p in rows:
        oe=event_from_payload(p)
        if oe.kind=='TRADE':trades.append((oe.at,oe.data['price'],int(oe.data['trade_id'])))
        e=norm(oe,cfg)
        if e is None:continue
        if e.kind=='BAR' and e.data.get('close_ms',e.received)>=e.received-cfg.clock_uncertainty_ms:continue
        cand=observe(s,e,cfg)
        if s.get('attempt'):attempt_ids.add(s['attempt']['id'])
        if cand:cands.append(dict(seq=seq,time=e.decision_ms,trade_time=e.at,price=e.data.get('price'),candidate=cand))
    uniq={}
    for x in cands:uniq.setdefault(x['candidate']['id'],x)
    results=[]
    for aid,x in uniq.items():
        c=x['candidate'];d=c['direction'];start=x['trade_time'];stop=c['stop'];mid=c['mid'];far=c['far'];ep=x['price']
        stop_hit=mid_hit=far_hit=None;maxfav=0.;maxadv=0.
        for at,p,tid in trades:
            if at<start:continue
            if at-start>cfg.max_hold_ms:break
            fav=d*(p-ep);maxfav=max(maxfav,fav);maxadv=min(maxadv,fav)
            if stop_hit is None and d*(p-stop)<=0:stop_hit=(at,p,tid)
            if mid_hit is None and d*(p-mid)>=0:mid_hit=(at,p,tid)
            if far_hit is None and d*(p-far)>=0:far_hit=(at,p,tid)
        hits=[(name,val[0]) for name,val in [('STOP',stop_hit),('MID',mid_hit),('FAR',far_hit)] if val]
        first=min(hits,key=lambda z:z[1])[0] if hits else None
        results.append(dict(symbol=sym,attempt=aid,kind=c['explanation']['kind'],route=c['explanation']['route'],direction=d,
            decision_ms=x['time'],trade_ms=start,trigger=ep,stop=stop,mid=mid,far=far,first_passage=first,
            stop_hit=stop_hit,mid_hit=mid_hit,far_hit=far_hit,max_favorable_from_trigger=maxfav,max_adverse_from_trigger=maxadv,
            explanation=c['explanation']))
    con.close()
    return dict(symbol=sym,events=len(rows),attempts=len(attempt_ids),unique_candidate_attempts=len(uniq),candidate_emissions=len(cands)),results

if __name__=='__main__':
    con=sqlite3.connect(SRC)
    syms=[r[0] for r in con.execute("select symbol from decisions where reason in ('BALANCE_MONITORING','WAIT_BOUNDARY_SWEEP','OUTSIDE_ATTEMPT','WAIT_RECLAIM') group by symbol order by symbol")];con.close()
    with mp.Pool(min(8,len(syms))) as pool: pairs=pool.map(work,syms)
    summary=[x[0] for x in pairs];all_candidates=[c for _,cs in pairs for c in cs]
    all_candidates.sort(key=lambda x:x['decision_ms'])
    out=dict(source=SRC,normalization='counterfactual healthy local processing: GAP records omitted, market/meta/OI receipt=event+100ms, bars available 2.5s after close; historical quote/depth were not present and are not reconstructed',symbols=summary,candidates=all_candidates)
    (ROOT/'audit/targeted_counterfactual_results.json').write_text(json.dumps(out,indent=2,ensure_ascii=False),encoding='utf-8')
    print('symbols',len(summary),'attempts',sum(x['attempts'] for x in summary),'unique candidates',len(all_candidates))
    for x in all_candidates:
        dt=datetime.datetime.fromtimestamp(x['decision_ms']/1000,datetime.timezone.utc).isoformat()
        print(x['symbol'],x['kind'],x['route'],dt,'trigger',x['trigger'],'stop',x['stop'],'mid',x['mid'],'far',x['far'],'first',x['first_passage'])
