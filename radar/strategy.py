"""Causal experimental failed-auction strategy. No hidden-intent claims."""
import math,statistics
from .model import digest,positive

def initial():
    return dict(bars=[],oi=[],trades=[],episode=None,attempt=None,quote=None,depth=None,meta=None,
                last_trade_id=None,last_trade_ms=0,warm_trades=0,reason='WARMUP',rank=None,attention=None)

def zscore(x,history):
    if len(history)<10:return None
    m=statistics.median(history);mad=statistics.median(abs(y-m) for y in history)
    if mad<=1e-12:return 0.0 if abs(x-m)<1e-12 else math.copysign(5.,x-m)
    return max(-10.,min(10.,(x-m)/(1.4826*mad)))

def _block_sums(values,width):
    if width<=0:return []
    usable=len(values)//width*width
    values=values[-usable:] if usable else []
    return [sum(values[i:i+width]) for i in range(0,len(values),width)]

def _series_z(values,index=-1):
    if len(values)<11:return None
    i=index if index>=0 else len(values)+index
    if i<10 or i>=len(values):return None
    return zscore(values[i],values[max(0,i-30):i])

def features(s,c=None):
    """Point-in-time participation features with explicit heat persistence.

    Discovery is intentionally different from trade confirmation. A one-window burst
    can earn observation, but it does not remain HOT if the participation immediately
    unwinds. Negative OI is kept signed: a high-volume deleveraging flush can remain
    interesting without being mislabeled sustained accumulation.
    """
    bars=s['bars'];oi=s['oi'];vol=None;growth=None;gz=None
    if len(bars)>=31:
        vol=zscore(bars[-1]['volume'],[x['volume'] for x in bars[-31:-1]])
    if len(oi)>=11:
        gs=[math.log(b['value']/a['value']) for a,b in zip(oi,oi[1:]) if b['at']-a['at']==300000]
        if len(gs)>=10:growth=gs[-1];gz=zscore(gs[-1],gs[:-1])

    width=getattr(c,'heat_volume_window_bars',5)
    event_z=getattr(c,'heat_event_z',2.0)
    retained_min=getattr(c,'heat_retention_min',.45)
    dead_max=getattr(c,'heat_dead_retention_max',.20)
    volume_alive=getattr(c,'heat_volume_alive_z',.50)

    block5=_block_sums([x['volume'] for x in bars],width)
    volume_5m=sum(x['volume'] for x in bars[-width:]) if len(bars)>=width else None
    volume_5m_z=_series_z(block5,-1)
    prior_volume_5m_z=_series_z(block5,-2)
    block10=_block_sums([x['volume'] for x in bars],width*2)
    volume_10m_z=_series_z(block10,-1)

    oi_growth_15m=None;oi_growth_15m_z=None;oi_retention=None;oi_reversal_fraction=None
    oi_impulse_direction=0;prior_oi_growth_z=None
    contiguous=[]
    if oi:
        contiguous=[oi[-1]]
        for x in reversed(oi[:-1]):
            if contiguous[0]['at']-x['at']!=300000:break
            contiguous.insert(0,x)
    if len(contiguous)>=2:
        all_g=[math.log(b['value']/a['value']) for a,b in zip(contiguous,contiguous[1:])]
        if len(all_g)>=2:
            prior_oi_growth_z=_series_z(all_g,-2)
        if len(contiguous)>=4:
            base=contiguous[-4]['value'];current=contiguous[-1]['value']
            if positive(base) and positive(current):
                oi_growth_15m=math.log(current/base)
                hist15=[]
                if len(contiguous)>=14:
                    for i in range(3,len(contiguous)):
                        if contiguous[i]['at']-contiguous[i-3]['at']==900000:
                            hist15.append(math.log(contiguous[i]['value']/contiguous[i-3]['value']))
                    if len(hist15)>=11:oi_growth_15m_z=zscore(hist15[-1],hist15[:-1])
                path=[math.log(x['value']/base) for x in contiguous[-3:]]
                peak=max(path,key=lambda x:abs(x)) if path else 0.
                if abs(peak)>1e-12:
                    oi_impulse_direction=1 if peak>0 else -1
                    retained=oi_impulse_direction*oi_growth_15m/abs(peak)
                    oi_retention=max(0.,min(1.,retained))
                    oi_reversal_fraction=max(0.,min(2.,1-retained))

    current_event=(volume_5m_z is not None and volume_5m_z>=event_z) or (gz is not None and abs(gz)>=event_z)
    prior_event=(prior_volume_5m_z is not None and prior_volume_5m_z>=event_z) or (prior_oi_growth_z is not None and abs(prior_oi_growth_z)>=event_z)
    current_volume_alive=volume_5m_z is not None and volume_5m_z>=volume_alive
    volume_alive_now=current_volume_alive or (volume_10m_z is not None and volume_10m_z>=volume_alive)
    high_volume_now=volume_5m_z is not None and volume_5m_z>=event_z
    oi_flush=(gz is not None and gz<=-event_z and high_volume_now and
              (oi_growth_15m is None or oi_growth_15m<0 or (oi_retention is not None and oi_retention<=dead_max)))
    current_reversing=bool(growth is not None and oi_impulse_direction and growth*oi_impulse_direction<0)

    if oi_flush:
        heat_state='FLUSH_EVENT'
    elif prior_event and current_reversing and oi_retention is not None and oi_retention<=dead_max and not current_volume_alive:
        # A statistically large reversal is not a new source of heat when it merely
        # removes the preceding impulse and participation volume has disappeared.
        heat_state='DEAD_BURST'
    elif current_event and not current_reversing and ((oi_retention is not None and oi_retention>=retained_min) or (volume_10m_z is not None and volume_10m_z>=volume_alive)):
        heat_state='SUSTAINED_HOT'
    elif oi_retention is not None and oi_retention>=retained_min and volume_alive_now:
        heat_state='HOT_RETAINED'
    elif current_event:
        heat_state='NEW_IMPULSE'
    elif prior_event and ((oi_retention is not None and oi_retention>dead_max) or volume_alive_now):
        heat_state='COOLING'
    else:
        heat_state='NORMAL'

    return dict(volume_z=vol,oi_growth=growth,oi_growth_z=gz,
                volume_5m=volume_5m,volume_5m_z=volume_5m_z,prior_volume_5m_z=prior_volume_5m_z,
                volume_10m_z=volume_10m_z,oi_growth_15m=oi_growth_15m,oi_growth_15m_z=oi_growth_15m_z,
                prior_oi_growth_z=prior_oi_growth_z,oi_retention=oi_retention,
                oi_reversal_fraction=oi_reversal_fraction,oi_impulse_direction=oi_impulse_direction,
                heat_state=heat_state)

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
    # Fix 6: Reject micro-compressions too narrow to overcome spread + slippage.
    # A range of 0.4% on a coin with 0.1% spread and 0.05% slippage gives almost no
    # structural edge -- the noise floor swallows the entire Spring move.
    if lo>0 and (w/lo*100)<getattr(c,'min_compression_width_pct',0.6):return None
    # Alternating visits establish balance, rather than one flat directional bar.
    visits=[]
    for bar in recent:
        where=-1 if bar['close']<=lo+.3*w else 1 if bar['close']>=hi-.3*w else 0
        if where and (not visits or visits[-1]!=where):visits.append(where)
    if len(visits)<4:return None
    # Fix 1b: Only recognise compressions on coins with developing interest.
    # A coin sitting flat with no OI change is low-volatility, not a coiled spring.
    # The heat gate is bypassed when the coin already has a live attempt -- re-evaluation
    # of an existing episode must not evict an in-progress Spring/Upthrust observation.
    min_heat=getattr(c,'min_compression_heat','COOLING')
    if min_heat!='ANY' and not s.get('attempt'):
        feat=features(s,c);heat=feat.get('heat_state','NORMAL')
        if heat in ('NORMAL','DEAD_BURST'):return None
    return dict(lower=lo,upper=hi,width=w,created=now,expires=now+c.episode_ms,
                source_start_ms=recent[0]['open_ms'],source_end_ms=recent[-1]['close_ms'],
                baseline_width=pw,contraction=w/pw,
                id='EP_'+digest([recent[0]['open_ms'],lo,hi])[:24],version=1,rotations=len(visits))

def _signed_progress(direction, price, reference):
    return direction*(price-reference)

def _trade_is_favorable(trade,direction):
    return trade['buy']==(direction==1)

def _flow_stats(trades,direction):
    total=sum(t['qty'] for t in trades)
    if total<=0:return dict(total=0.,favorable=0.,adverse=0.,favorable_share=.5,adverse_share=.5)
    favorable=sum(t['qty'] for t in trades if _trade_is_favorable(t,direction))
    return dict(total=total,favorable=favorable,adverse=total-favorable,
                favorable_share=favorable/total,adverse_share=(total-favorable)/total)

def _attempt_quality(a,ep,direction):
    """Describe effort/result without pretending it reveals hidden intent."""
    outside=a.get('outside',[]);flow=_flow_stats(outside,direction)
    if outside:
        span=max(1000,outside[-1]['at']-outside[0]['at'])
        outside_rate=flow['total']*1000/span
    else:outside_rate=0.
    baseline_rate=a.get('baseline_rate')
    effort_ratio=outside_rate/baseline_rate if baseline_rate and baseline_rate>0 else None
    boundary=ep['lower'] if direction==1 else ep['upper']
    excursion=abs(a['extreme']-boundary);excursion_fraction=excursion/ep['width'] if ep['width']>0 else None
    route='UNRESOLVED_EFFORT'
    if effort_ratio is not None:
        if effort_ratio>=1.25 and flow['adverse_share']>=.55 and excursion_fraction<=.35:
            route='HIGH_EFFORT_POOR_RESULT'
        elif effort_ratio<=.75 and excursion_fraction<=.15:
            route='LOW_EFFORT_EXHAUSTION'
        else:route='MIXED_EFFORT_RESULT'
    return dict(route=route,effort_ratio=effort_ratio,outside_rate=outside_rate,baseline_rate=baseline_rate,
                excursion_fraction=excursion_fraction,outside_adverse_share=flow['adverse_share'])

def _candidate(s,a,ep,p,noise,route,response,reference):
    direction=a['direction'];flow=_flow_stats(response,direction)
    stop=a['extreme']-direction*noise
    far=ep['upper'] if direction==1 else ep['lower'];mid=(ep['upper']+ep['lower'])/2
    outside=_flow_stats(a.get('outside',[]),direction)
    excursion=direction*((ep['lower'] if direction==1 else ep['upper'])-a['extreme'])
    return dict(id=a['id'],episode=ep['id'],episode_version=ep['version'],direction=direction,
        trigger=p,stop=stop,mid=mid,far=far,evidence=a['evidence']+[x['id'] for x in response],
        explanation=dict(kind='SPRING' if direction==1 else 'UPTHRUST',route=route,
                         aggression=flow['favorable_share'],outside_adverse_share=outside['adverse_share'],
                         effort_route=(a.get('quality') or {}).get('route'),
                         effort_ratio=(a.get('quality') or {}).get('effort_ratio'),
                         excursion_fraction=(a.get('quality') or {}).get('excursion_fraction'),
                         excursion=a['extreme'],excursion_distance=excursion,
                         response_reference=reference,rotations=ep['rotations'],
                         best_response=a.get('best_progress',0.),retest_extreme=a.get('retest_extreme')))

def observe(s,e,c):
    """Causal mirrored failed-auction observer for Spring/Upthrust.

    The state machine deliberately separates: outside attempt, reclaim, price response,
    defended retest, renewed progress, and competing outside acceptance. Flow supports
    the decision but a fixed buy/sell percentage is never allowed to overrule clear
    price-effort/result evidence by itself.
    """
    d=e.data;now=e.decision_ms
    ep=s['episode'];a=s['attempt']
    if ep and now>=ep['expires']:
        s['episode']=None;s['attempt']=None;s['reason']='EPISODE_EXPIRED'
    elif a and now-a['started']>c.attempt_ms:
        s['attempt']=None;s['reason']='ATTEMPT_EXPIRED'
    if e.kind=='TIMER':return
    if e.kind=='META':s['meta']=dict(d);return
    if e.kind=='ATTENTION':s['attention']=dict(d);return
    if e.kind=='GAP':
        group=d.get('group')
        if group=='public':
            # A book/quote reconnect removes executable-liquidity certainty but does
            # not erase an otherwise continuous aggTrade auction hypothesis.
            # Fix 9: Preserve last-known quote if it is still fresh enough.
            # The old unconditional quote=None caused the next confirmed signal to always
            # fail QUOTE_UNAVAILABLE_OR_STALE, since reconnect takes 1-3s and quotes
            # need time to re-arrive. We only wipe the quote when it is already stale
            # (i.e., older than max_quote_age_ms at the time of the gap event).
            # Preserve last-known quote and depth if still fresh enough.
            q=s.get('quote');depth=s.get('depth');age_limit=getattr(c,'max_quote_age_ms',8000)
            if q and now-q.get('received',0)>age_limit:s['quote']=None
            if depth and now-depth.get('received',0)>age_limit:s['depth']=None
            s['reason']='BOOK_GAP_WAIT_LIQUIDITY';return
        if group=='market':
            # Fix 2: Preserve attempts that already passed the OUTSIDE stage.
            # A 2-second aggTrade reconnect must not erase a reclaimed Spring/Upthrust
            # that took minutes to develop. We clear flow evidence (now untrustworthy)
            # but keep the structural fact that the sweep occurred and price returned
            # inside the balance. Only wipe the attempt if still in OUTSIDE phase
            # (no reclaim confirmed yet), since response evidence has no basis then.
            _existing=s.get('attempt')
            if _existing and _existing.get('stage') not in (None,'OUTSIDE'):
                _existing['response']=[];_existing.pop('retest_response',None)
                s.update(trades=[],last_trade_id=None,warm_trades=0,reason='TRADE_GAP_WARMUP');return
            s.update(trades=[],attempt=None,last_trade_id=None,warm_trades=0,reason='TRADE_GAP_WARMUP');return
        # Clock/unknown gaps can invalidate ordering across capabilities; reset all.
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
    trade=dict(price=p,qty=qty,buy=not d['buyer_maker'],at=e.at,id=e.identity())
    s['trades']=(s['trades']+[trade])[-300:]
    if now-e.at>c.max_exchange_lag_ms:s['reason']='DELAYED_TRADE';return
    meta=s['meta']
    if not ep:s['reason']='NO_COMPRESSION';return
    if not meta:s['reason']='METADATA_UNAVAILABLE';return
    if s['warm_trades']<20:s['reason']='FLOW_WARMUP';return
    if not s['bars'] or now-s['bars'][-1]['close_ms']>120000:s['reason']='BARS_STALE';return
    # Fix 1c: Gate new outside attempts on coins with developing OI/volume interest.
    # Once a sweep is already in progress (attempt exists), do not abort mid-sequence
    # due to a single quiet OI poll window -- the structural auction hypothesis stands.
    min_entry_heat=getattr(c,'min_entry_heat','COOLING')
    if min_entry_heat!='ANY' and a is None:
        _feat=features(s,c);_heat=_feat.get('heat_state','NORMAL')
        if _heat in ('NORMAL','DEAD_BURST'):s['reason']='COMPRESSION_HEAT_INSUFFICIENT';return
    noise=max(meta['tick_size']*c.min_net_move_ticks,ep['width']*.01)
    boundary=ep['lower'] if (a or {}).get('direction',1)==1 else ep['upper']
    if a is None:
        # A failed auction needs a genuine excursion, not a one-tick print just beyond
        # the edge. Five percent of the frozen balance width is still a small probe,
        # but it filters boundary micro-noise while remaining normalized by symbol.
        sweep_need=max(noise,ep['width']*.05)
        direction=1 if p<=ep['lower']-sweep_need else -1 if p>=ep['upper']+sweep_need else 0
        s['reason']='OUTSIDE_ATTEMPT' if direction else 'WAIT_BOUNDARY_SWEEP'
        if direction:
            anchor=statistics.median(x['price'] for x in s['trades'][-11:-1])
            pre=s['trades'][-41:-1]
            if len(pre)>=2:
                span=max(1000,pre[-1]['at']-pre[0]['at']);baseline_rate=sum(x['qty'] for x in pre)*1000/span
            else:baseline_rate=None
            s['attempt']=dict(id='AT_'+digest([ep['id'],e.identity()])[:24],direction=direction,
                started=now,extreme=p,stage='OUTSIDE',anchor=anchor,evidence=[e.identity()],outside=[trade],
                response=[],best_progress=0.,lost_since=None,lost_count=0,retest_extreme=None,baseline_rate=baseline_rate,quality=None)
        return
    direction=a['direction'];boundary=ep['lower'] if direction==1 else ep['upper']
    # Fix 4: Use configurable tolerance for excursion invalidation (default 1.5x width).
    # A Spring commonly wicks 1.0-1.5x below the balance before snapping back sharply.
    # The original 1x limit invalidated many legitimate Springs on the wick extreme.
    # At 1.5x we still correctly reject genuine breakouts (2x+ moves past the boundary).
    _exc_tol=getattr(c,'excursion_tolerance',1.5)*ep['width']
    if (direction==1 and p<ep['lower']-_exc_tol) or (direction==-1 and p>ep['upper']+_exc_tol):
        s['attempt']=None;s['reason']='EXCESSIVE_EXCURSION';return
    if a['stage']=='OUTSIDE':
        old=a['extreme'];a['extreme']=min(old,p) if direction==1 else max(old,p)
        s['reason']='WAIT_RECLAIM'
        if direction*(p-boundary)>=noise:
            a.update(stage='RECLAIMED',reclaimed=now,reclaim_price=p,response=[],best_progress=0.,
                     lost_since=None,lost_count=0,retest_extreme=None,retest_started=None)
            a['quality']=_attempt_quality(a,ep,direction)
            a['evidence'].append(e.identity());s['reason']='WAIT_RESPONSE'
        else:a['outside']=(a.get('outside',[])+[trade])[-120:]
        return
    if a['stage']=='CONSUMED':s['reason']='ATTEMPT_CONSUMED';return
    # The sweep extreme is a structural reference, not a one-tick hard stop while observing.
    # Only meaningful new adverse progress beyond it invalidates immediately.
    hard_fail=a['extreme']-direction*noise
    if direction*(p-hard_fail)<0:
        s['attempt']=None;s['reason']='STRUCTURAL_INVALIDATION';return
    inside_progress=direction*(p-boundary)
    if inside_progress < -noise:
        if a.get('lost_since') is None:a['lost_since']=now;a['lost_count']=1
        else:a['lost_count']+=1
        # Persistent return outside after reclaim is acceptance, not a patient retest.
        if a['lost_count']>=c.min_response_trades and now-a['lost_since']>=c.min_response_ms:
            s['attempt']=None;s['reason']='OUTSIDE_ACCEPTANCE_AFTER_RECLAIM';return
        s['reason']='RECLAIM_UNDER_PRESSURE';return
    a['lost_since']=None;a['lost_count']=0
    a['response']=(a.get('response',[])+[trade])[-160:]
    response=a['response'];flow=_flow_stats(response,direction)
    reference=max(a['anchor'],a['reclaim_price']) if direction==1 else min(a['anchor'],a['reclaim_price'])
    progress=_signed_progress(direction,p,reference)
    a['best_progress']=max(a.get('best_progress',0.),progress)
    quality=(a.get('quality') or {}).get('route','UNRESOLVED_EFFORT')
    # Clear high-effort failure or low-effort exhaustion earns a slightly earlier
    # response threshold. Mixed/unknown attempts must prove materially more progress.
    response_fraction=.025 if quality in ('HIGH_EFFORT_POOR_RESULT','LOW_EFFORT_EXHAUSTION') else .05
    small=max(noise,ep['width']*response_fraction)
    # Price-led confirmation is deliberately stronger than the aligned-flow route.
    # In the old mixed-effort branch, a fixed 4% threshold could paradoxically be
    # lower than the 5% aligned threshold. Never let the 'strong' route be easier.
    strong=max(noise*2,small*1.5,ep['width']*.04)

    # Once execution economics say the direct response has become expensive, do not
    # keep chasing the same confirmation. Preserve the valid auction hypothesis and
    # wait for a defended revisit that improves price without accepting outside.
    if a.get('entry_mode')=='RETEST_ONLY' and a['stage']!='RETEST':
        # Fix 5c: Time-pressure escape -- if >60% of attempt window has elapsed,
        # lift the RETEST_ONLY constraint. The original RR rejection may be stale
        # (price has improved since). Fall through to the direct-response check.
        if now-a['started']>c.attempt_ms*0.6:
            a.pop('entry_mode',None)  # allow direct-response below
        else:
            # Fix 5a: Config-driven retest zone (default 8% vs old hardcoded 5%).
            retest_zone=max(noise,ep['width']*getattr(c,'retest_zone_fraction',0.08))
            if a['best_progress']>=small and inside_progress<=retest_zone:
                a['retest_started']=now;a['retest_extreme']=p;a['retest_response']=[];a['stage']='RETEST'
                s['reason']='WAIT_RETEST_DEFENSE';return
            s['reason']='WAIT_BETTER_ENTRY_RETEST';return

    # A retest is a distinct entry route. Once it starts, renewed progress must be
    # measured from the retest extreme rather than falling back into direct-response
    # confirmation and accidentally chasing the old move.
    if a['stage']=='RETEST':
        a['retest_extreme']=min(a['retest_extreme'],p) if direction==1 else max(a['retest_extreme'],p)
        a.setdefault('retest_response',[]).append(trade);a['retest_response']=a['retest_response'][-100:]
        rext=a['retest_extreme'];renewed=_signed_progress(direction,p,rext)
        rflow=_flow_stats(a['retest_response'],direction)
        retest_need=max(noise,ep['width']*.03)
        # Fix 5b: Lower retest flow requirement -- retests are inherently quieter than
        # direct response. Slightly less buying pressure in a controlled pullback is
        # expected information, not a disqualifying signal.
        if len(a['retest_response'])>=c.min_response_trades and renewed>=retest_need and rflow['favorable_share']>=max(.40,c.aggression_fraction-.15):
            s['reason']='ENTRY_RETEST_CONFIRMED'
            return _candidate(s,a,ep,p,noise,'DEFENDED_RETEST',a['retest_response'],rext)
        s['reason']='WAIT_RETEST_DEFENSE';return

    if len(response)<c.min_response_trades or now-a['reclaimed']<c.min_response_ms:
        s['reason']='WAIT_RESPONSE';return
    # Two legitimate direct routes inside the SAME Spring/Upthrust family:
    # 1) flow-aligned response; 2) price-led poor-result/absorption response.
    aligned=progress>=small and flow['favorable_share']>=c.aggression_fraction
    price_led=progress>=strong and flow['favorable_share']>=max(.40,c.aggression_fraction-.15)
    if aligned or price_led:
        route='DIRECT_FLOW_RESPONSE' if aligned else 'DIRECT_PRICE_LED_RESPONSE'
        s['reason']='ENTRY_RESPONSE_CONFIRMED'
        return _candidate(s,a,ep,p,noise,route,response,reference)
    # Once there has been genuine response, a controlled revisit is information rather
    # than automatic failure. Keep it inside the original Spring/Upthrust hypothesis.
    # Fix 5a: Config-driven retest zone applied to both RETEST_ONLY and organic retest paths.
    retest_zone=max(noise,ep['width']*getattr(c,'retest_zone_fraction',0.08))
    if a['best_progress']>=small and inside_progress<=retest_zone:
        if a.get('retest_started') is None:
            a['retest_started']=now;a['retest_extreme']=p;a['retest_response']=[]
        else:
            a['retest_extreme']=min(a['retest_extreme'],p) if direction==1 else max(a['retest_extreme'],p)
        a['stage']='RETEST';s['reason']='WAIT_RETEST_DEFENSE';return
    s['reason']='NO_EFFECTIVE_RESPONSE'
