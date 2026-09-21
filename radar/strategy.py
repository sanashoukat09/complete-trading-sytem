"""Causal experimental failed-auction strategy. No hidden-intent claims."""
import math,statistics
from .model import digest,positive

def initial():
    return dict(bars=[],oi=[],trades=[],episode=None,attempt=None,quote=None,depth=None,meta=None,
                last_trade_id=None,last_trade_ms=0,warm_trades=0,reason='WARMUP',rank=None)

def zscore(x,history):
    if len(history)<10:return None
    m=statistics.median(history);mad=statistics.median(abs(y-m) for y in history)
    if mad<=1e-12:return 0.0 if abs(x-m)<1e-12 else math.copysign(5.,x-m)
    return max(-10.,min(10.,(x-m)/(1.4826*mad)))

def features(s):
    bars=s['bars']; oi=s['oi'];vol=None;growth=None;gz=None
    if len(bars)>=31:
        vol=zscore(bars[-1]['volume'],[x['volume'] for x in bars[-31:-1]])
    if len(oi)>=11:
        gs=[math.log(b['value']/a['value']) for a,b in zip(oi,oi[1:]) if b['at']-a['at']==300000]
        if len(gs)>=10:growth=gs[-1];gz=zscore(gs[-1],gs[:-1])
    return dict(volume_z=vol,oi_growth=growth,oi_growth_z=gz)

def compression(s,now,c):
    bars=s['bars'];n=c.compression_bars;b=c.baseline_bars
    if len(bars)<n+b:return None
    recent=bars[-n:];prior=bars[-n-b:-n]
    if any(y['open_ms']-x['open_ms']!=60000 for x,y in zip(bars[-n-b:],bars[-n-b+1:])):return None
    lo=min(x['low'] for x in recent);hi=max(x['high'] for x in recent);w=hi-lo
    # Compare equal-duration windows: a 30-minute range against prior 30-minute ranges.
    widths=[max(x['high'] for x in prior[i:i+n])-min(x['low'] for x in prior[i:i+n]) for i in range(0,len(prior)-n+1,n)]
    pw=statistics.median(widths)
    if w<=0 or pw<=0 or w>pw*c.contraction_ratio:return None
    if abs(recent[-1]['close']-recent[0]['open'])>w*c.max_drift_fraction:return None
    # Alternating visits establish balance, rather than one flat directional bar.
    visits=[]
    for bar in recent:
        where=-1 if bar['close']<=lo+.3*w else 1 if bar['close']>=hi-.3*w else 0
        if where and (not visits or visits[-1]!=where):visits.append(where)
    if len(visits)<4:return None
    return dict(lower=lo,upper=hi,width=w,created=now,expires=now+c.episode_ms,
                source_start_ms=recent[0]['open_ms'],source_end_ms=recent[-1]['close_ms'],
                baseline_width=pw,contraction=w/pw,
                id='EP_'+digest([recent[0]['open_ms'],lo,hi])[:24],version=1,rotations=len(visits))

def observe(s,e,c):
    """Mutates only supplied JSON state. Returns an evidence-bearing candidate or None."""
    d=e.data;now=e.decision_ms
    ep=s['episode'];a=s['attempt']
    if ep and now>=ep['expires']:
        s['episode']=None;s['attempt']=None;s['reason']='EPISODE_EXPIRED'
    elif a and now-a['started']>c.attempt_ms:
        s['attempt']=None;s['reason']='ATTEMPT_EXPIRED'
    if e.kind=='TIMER':return
    if e.kind=='META':s['meta']=dict(d);return
    if e.kind=='GAP':
        s.update(quote=None,depth=None,trades=[],attempt=None,last_trade_id=None,warm_trades=0,reason='GAP_WARMUP');return
    if e.kind in ('QUOTE','DEPTH'):
        key='quote' if e.kind=='QUOTE' else 'depth'
        if s[key] and e.at<s[key]['at']:s['reason']='LATE_BOOK_OBSERVATION';return
        s[key]=dict(d,at=e.at,received=e.received);return
    if e.kind=='OI':
        if s['oi'] and e.at<=s['oi'][-1]['at']:return
        if not positive(d.get('value')):raise ValueError('Invalid OI')
        s['oi']=(s['oi']+[dict(value=d['value'],at=e.at)])[-100:];return
    ep=s['episode'];a=s['attempt']
    if ep and now>ep['expires']:s['episode']=None;s['attempt']=None;ep=a=None;s['reason']='EPISODE_EXPIRED'
    if a and now-a['started']>c.attempt_ms:s['attempt']=None;a=None;s['reason']='ATTEMPT_EXPIRED'
    if e.kind=='BAR':
        if d['close_ms']>=now-c.clock_uncertainty_ms:return
        if s['bars'] and d['open_ms']<=s['bars'][-1]['open_ms']:return
        if not (0<d['low']<=min(d['open'],d['close'])<=max(d['open'],d['close'])<=d['high']):raise ValueError('Invalid OHLC')
        s['bars']=(s['bars']+[dict(d)])[-(c.baseline_bars+c.compression_bars+60):]
        if ep and not a:
            # Three closed bars accepting beyond old value terminate it; a single sweep does not.
            closes=[x['close'] for x in s['bars'][-3:]]
            if all(x>ep['upper'] for x in closes) or all(x<ep['lower'] for x in closes):s['episode']=None;ep=None
        if ep is None and 0<=now-d['close_ms']<=120000:s['episode']=compression(s,now,c)
        s['reason']='BALANCE_MONITORING' if s['episode'] else 'NO_COMPRESSION'
        return
    if e.kind!='TRADE':return
    p=d['price'];qty=d['quantity'];tid=int(d['trade_id'])
    if not positive(p) or not positive(qty) or not isinstance(d.get('buyer_maker'),bool):raise ValueError('Invalid trade')
    if e.at<s['last_trade_ms'] or (s['last_trade_id'] is not None and tid<=s['last_trade_id']):s['reason']='LATE_TRADE';return
    last=s['last_trade_id']
    if last is not None and tid!=last+1:
        s['trades']=[];s['attempt']=None;s['warm_trades']=0;a=None;s['reason']='TRADE_GAP'
    s['last_trade_id']=tid;s['last_trade_ms']=e.at;s['warm_trades']+=1
    s['trades']=(s['trades']+[dict(price=p,qty=qty,buy=not d['buyer_maker'],at=e.at,id=e.identity())])[-300:]
    if now-e.at>c.max_exchange_lag_ms:s['reason']='DELAYED_TRADE';return
    meta=s['meta']
    if not ep:s['reason']='NO_COMPRESSION';return
    if not meta:s['reason']='METADATA_UNAVAILABLE';return
    if s['warm_trades']<20:s['reason']='FLOW_WARMUP';return
    if not s['bars'] or now-s['bars'][-1]['close_ms']>120000:s['reason']='BARS_STALE';return
    noise=max(meta['tick_size']*c.min_net_move_ticks,ep['width']*.01)
    if a is None:
        direction=1 if p<ep['lower']-noise else -1 if p>ep['upper']+noise else 0
        s['reason']='OUTSIDE_ATTEMPT' if direction else 'WAIT_BOUNDARY_SWEEP'
        if direction:
            anchor=statistics.median(x['price'] for x in s['trades'][-11:-1])
            s['attempt']=dict(id='AT_'+digest([ep['id'],e.identity()])[:24],direction=direction,
                started=now,extreme=p,stage='OUTSIDE',anchor=anchor,evidence=[e.identity()],response=[])
        return
    direction=a['direction'];boundary=ep['lower'] if direction==1 else ep['upper']
    if (direction==1 and p<ep['lower']-ep['width']) or (direction==-1 and p>ep['upper']+ep['width']):
        s['attempt']=None;s['reason']='EXCESSIVE_EXCURSION';return
    if a['stage']=='OUTSIDE':
        a['extreme']=min(a['extreme'],p) if direction==1 else max(a['extreme'],p)
        s['reason']='WAIT_RECLAIM'
        if direction*(p-boundary)>=noise:
            s['reason']='WAIT_RESPONSE'
            a.update(stage='RECLAIMED',reclaimed=now,reclaim_price=p,response=[]);a['evidence'].append(e.identity())
        return
    if direction*(p-a['extreme'])<=0:s['attempt']=None;s['reason']='STRUCTURAL_INVALIDATION';return
    if direction*(p-boundary)<0:s['reason']='RECLAIM_LOST';s['attempt']=None;return
    if a['stage']=='CONSUMED':s['reason']='ATTEMPT_CONSUMED';return
    a['response']=(a['response']+[s['trades'][-1]])[-100:]
    response=a['response'];total=sum(t['qty'] for t in response)
    fav=sum(t['qty'] for t in response if t['buy']==(direction==1))
    if len(response)<c.min_response_trades or now-a['reclaimed']<c.min_response_ms:s['reason']='WAIT_RESPONSE';return
    reference=max(a['anchor'],a['reclaim_price']) if direction==1 else min(a['anchor'],a['reclaim_price'])
    if direction*(p-reference)<noise:s['reason']='NO_PRICE_RESPONSE';return
    if fav/total<c.aggression_fraction:s['reason']='FLOW_CONTRADICTION';return
    stop=a['extreme']-direction*noise
    far=ep['upper'] if direction==1 else ep['lower'];mid=(ep['upper']+ep['lower'])/2
    return dict(id=a['id'],episode=ep['id'],episode_version=ep['version'],direction=direction,
        trigger=p,stop=stop,mid=mid,far=far,evidence=a['evidence']+[x['id'] for x in response],
        explanation=dict(kind='SPRING' if direction==1 else 'UPTHRUST',aggression=fav/total,
                         excursion=a['extreme'],response_reference=reference,rotations=ep['rotations']))
