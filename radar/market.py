"""Read-only Binance adapters. No credentials and no private/order endpoints."""
import asyncio,time,math,logging,statistics
from dataclasses import replace
from copy import deepcopy
from .model import Event,positive
from .strategy import features
log=logging.getLogger(__name__)

def metadata(raw):
    if raw.get('quoteAsset')!='USDT' or raw.get('contractType')!='PERPETUAL':return None
    filters={x['filterType']:x for x in raw.get('filters',[])}
    try:
        lot=filters.get('MARKET_LOT_SIZE',filters['LOT_SIZE'])
        if float(lot['stepSize'])<=0:lot=filters['LOT_SIZE']
        d=dict(status=raw['status'],tick_size=float(filters['PRICE_FILTER']['tickSize']),
               step_size=float(lot['stepSize']),min_qty=float(lot['minQty']),max_qty=float(lot['maxQty']),
               min_notional=float(filters['MIN_NOTIONAL']['notional']))
    except (KeyError,ValueError):return None
    if not all(positive(d[k]) for k in ('tick_size','step_size','min_qty','max_qty','min_notional')):return None
    return d

def candle(symbol,row,received):
    d=dict(open_ms=int(row[0]),open=float(row[1]),high=float(row[2]),low=float(row[3]),close=float(row[4]),volume=float(row[5]),close_ms=int(row[6]))
    return Event('BAR',symbol,'1m:'+str(d['open_ms']),d['close_ms'],received,d,raw=row)

def normalize(raw,received):
    d=raw.get('data',raw);kind=d.get('e');symbol=d.get('s')
    if not symbol:return None
    at=int(d.get('E',received))
    if kind=='aggTrade':return Event('TRADE',symbol,str(d['a']),int(d['T']),received,dict(trade_id=int(d['a']),price=float(d['p']),quantity=float(d['q']),buyer_maker=d['m']),raw=raw)
    if kind=='bookTicker':return Event('QUOTE',symbol,str(d['u']),at,received,dict(bid=float(d['b']),ask=float(d['a']),bid_qty=float(d['B']),ask_qty=float(d['A'])),raw=raw)
    if kind=='depthUpdate':
        bids=sorted([[float(p),float(q)] for p,q in d['b'] if float(q)>0],reverse=True)
        asks=sorted([[float(p),float(q)] for p,q in d['a'] if float(q)>0])
        if not bids or not asks or bids[0][0]>asks[0][0]:raise ValueError('Invalid depth snapshot')
        # Only subscribed to depth20 snapshots, never diff-depth streams.
        return Event('DEPTH',symbol,str(d['u']),at,received,dict(bids=bids,asks=asks),raw=raw)
    return None

def _diversified_symbols(rows,limit):
    """Broad prefilter: do not let one 24h metric monopolize discovery."""
    if limit<=0:return []
    by_move=sorted(rows,key=lambda x:(-abs(x[3]),-x[0],x[1]))
    by_turn=sorted(rows,key=lambda x:(-x[0],-abs(x[3]),x[1]))
    out=[];seen=set();i=0
    while len(out)<min(limit,len(rows)) and i<max(len(by_move),len(by_turn)):
        for seq in (by_move,by_turn):
            if i<len(seq) and seq[i][1] not in seen:
                seen.add(seq[i][1]);out.append(seq[i])
                if len(out)>=limit:break
        i+=1
    return out

def _quick_activity(rows,now,clock_uncertainty_ms):
    """Cheap current-activity prefilter from closed 1m bars only."""
    closed=[r for r in rows if int(r[6])<now-clock_uncertainty_ms]
    if len(closed)<10:return dict(score=0.,volume_ratio=None,move_5m=None,range_5m=None)
    last=closed[-5:];prev=closed[-10:-5]
    v=sum(float(r[5]) for r in last);pv=sum(float(r[5]) for r in prev)
    ratio=v/pv if pv>0 else (10. if v>0 else 1.)
    p0=float(prev[-1][4]);p1=float(last[-1][4])
    move=abs(p1/p0-1) if p0>0 else 0.
    hi=max(float(r[2]) for r in last);lo=min(float(r[3]) for r in last)
    rng=(hi-lo)/p1 if p1>0 else 0.
    # Quick scan only chooses what receives deeper OI/history analysis. It does not
    # authorize monitoring/trading, so favor fresh participation without claiming edge.
    score=max(0.,math.log(max(ratio,1e-9)))*1.4+move*35+rng*20
    return dict(score=score,volume_ratio=ratio,move_5m=move,range_5m=rng)

def _heat_score(f):
    state=f.get('heat_state','NORMAL')
    v5=max(0.,f.get('volume_5m_z') or 0.);v10=max(0.,f.get('volume_10m_z') or 0.)
    oi5=abs(f.get('oi_growth_z') or 0.);oi15=abs(f.get('oi_growth_15m_z') or 0.)
    retention=f.get('oi_retention');retention=.5 if retention is None else retention
    base=v5+.6*v10+.75*oi5+.45*oi15+.75*retention
    mult={'SUSTAINED_HOT':1.25,'HOT_RETAINED':1.15,'NEW_IMPULSE':.90,
          'COOLING':.70,'FLUSH_EVENT':1.05,'NORMAL':.20,'DEAD_BURST':0.0}.get(state,.2)
    return base*mult

class PublicClient:
    def __init__(self):self.session=None;self._sem=asyncio.Semaphore(2);self._last=0.;self.offset=0
    def now(self):return int(time.time()*1000)+self.offset
    async def open(self):
        import aiohttp
        self.session=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        await self.sync_clock()
        return self
    async def sync_clock(self):
        before=int(time.time()*1000);server=await self.get('/fapi/v1/time');after=int(time.time()*1000)
        if after-before>4000:raise RuntimeError('Exchange clock synchronization uncertainty too high')
        self.offset=int(server['serverTime'])-(before+after)//2
        return dict(offset_ms=self.offset,uncertainty_ms=(after-before)/2,synced_ms=self.now())
    async def close(self):
        if self.session:await self.session.close()
    async def get(self,path,**params):
        if not path.startswith(('/fapi/v1/','/futures/data/')) or any(x in path.lower() for x in ('order','account','position')):raise ValueError('Not a public-data endpoint')
        async with self._sem:
            for attempt in range(4):
                await asyncio.sleep(max(0.,.15-(time.monotonic()-self._last)));self._last=time.monotonic()
                async with self.session.get('https://fapi.binance.com'+path,params=params) as r:
                    if r.status in (403,418,451):raise RuntimeError(f'Public API unavailable ({r.status}); do not bypass regional/access restrictions')
                    if r.status==429:
                        await asyncio.sleep(min(60,float(r.headers.get('Retry-After','30'))));continue
                    r.raise_for_status();data=await r.json()
                    if int(r.headers.get('X-MBX-USED-WEIGHT-1M','0'))>1800:await asyncio.sleep(30)
                    return data
            raise RuntimeError('Public API rate limit persisted')

class Feed:
    def __init__(self,engine,client=None):
        self.engine=engine;self.client=client or PublicClient();self.queue=asyncio.Queue(engine.cfg.queue_maxsize);self.stop=asyncio.Event()
        self.tasks=[];self.stream_tasks=[];self._subscriptions={};self._selection_lock=asyncio.Lock();self.rankings=[];self.selected=[];self.session_id=str(time.time_ns());self.health={};self.counter=0
        self._desired={'public':set(),'market':set()};self._sockets={'public':None,'market':None};self._subscribed={'public':set(),'market':set()};self._control_id=0
    async def emit(self,event):
        if event:await self.queue.put((event,None))
    async def flush(self):
        """Wait only for work already enqueued, never for a continuously arriving live stream."""
        loop=asyncio.get_running_loop();ack=loop.create_future()
        await self.queue.put((None,ack));await ack
    async def worker(self):
        while True:
            items=[await self.queue.get()]
            while len(items)<self.engine.cfg.worker_batch_size:
                try:items.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:break
            segment=[];segment_items=[]
            async def process_segment():
                nonlocal segment,segment_items
                if not segment:return
                now=self.client.now();qdelay=max(0,now-min(e.received for e in segment))
                self.health['queue_size']=self.queue.qsize();self.health['queue_delay_ms']=qdelay
                self.health['worker_received_ms']=now
                self.health['queue_overloaded']=qdelay>self.engine.cfg.overload_queue_delay_ms
                self.health['max_queue_delay_ms']=max(qdelay,self.health.get('max_queue_delay_ms',0))
                batch=[replace(e,processed_ms=now) for e in segment]
                processing=asyncio.create_task(asyncio.to_thread(self.engine.ingest_many,batch))
                try:await asyncio.shield(processing)
                except asyncio.CancelledError:
                    await processing;raise
                finally:
                    for _ in segment_items:self.queue.task_done()
                segment=[];segment_items=[]
            for item in items:
                event,ack=item
                if event is None:
                    await process_segment()
                    if not ack.done():ack.set_result(True)
                    self.queue.task_done()
                else:
                    segment.append(event);segment_items.append(item)
            await process_segment()
    async def refresh(self):
        c=self.engine.cfg;info=await self.client.get('/fapi/v1/exchangeInfo');tickers=await self.client.get('/fapi/v1/ticker/24hr')
        prices={x['symbol']:x for x in tickers};eligible=[];now=self.client.now();pins=self.engine.monitoring_pins(now)
        for raw in info['symbols']:
            meta=metadata(raw);sym=raw['symbol'];t=prices.get(sym,{})
            if not meta:continue
            pinned=sym in pins;turnover=float(t.get('quoteVolume',0));move=float(t.get('priceChangePercent',0) or 0)
            if (meta['status']=='TRADING' and turnover>=c.min_turnover and sym not in c.excluded_symbols) or pinned:
                eligible.append((turnover,sym,meta,move))

        # Phase 1: broad, cheap recent-activity scan. Diversify by turnover and 24h
        # movement so a fresh 5m ignition with a still-small daily move can be found.
        quick_pool=_diversified_symbols(eligible,c.quick_scan_count)
        quick=[]
        for turnover,sym,meta,move in quick_pool:
            try:
                rows=await self.client.get('/fapi/v1/klines',symbol=sym,interval='1m',limit=11)
                q=_quick_activity(rows,self.client.now(),c.clock_uncertainty_ms)
            except asyncio.CancelledError:raise
            except Exception as exc:
                self.health.setdefault('quick_scan_errors',{})[sym]=str(exc);q=dict(score=0.,volume_ratio=None,move_5m=None,range_5m=None)
            quick.append((q['score'],turnover,sym,meta,move,q))

        # Phase 2: deep scan a much larger cohort than v6.1.3. Most places are earned
        # by fresh 5m activity, but reserve slots for large movers and liquid contracts.
        qn=max(1,int(c.candidate_count*.70));mn=max(1,int(c.candidate_count*.15));tn=max(1,c.candidate_count-qn-mn)
        by_quick=sorted(quick,key=lambda x:(-x[0],-x[1],x[2]))
        by_move=sorted(quick,key=lambda x:(-abs(x[4]),-x[1],x[2]))
        by_turn=sorted(quick,key=lambda x:(-x[1],-x[0],x[2]))
        deep=[];seen=set()
        for seq,n in ((by_quick,qn),(by_move,mn),(by_turn,tn)):
            added=0
            for row in seq:
                if row[2] in seen:continue
                seen.add(row[2]);deep.append(row);added+=1
                if added>=n:break
        for row in by_quick:
            if len(deep)>=c.candidate_count:break
            if row[2] not in seen:seen.add(row[2]);deep.append(row)
        # Structural/position pins are never lost because they fell outside the scan cap.
        lookup={x[1]:x for x in eligible}
        for sym in sorted(pins):
            if sym in lookup and sym not in seen:
                turnover,_,meta,move=lookup[sym];deep.append((0.,turnover,sym,meta,move,dict(score=0.,volume_ratio=None,move_5m=None,range_5m=None)));seen.add(sym)

        chosen=[]
        for quick_score,turnover,sym,meta,move,q in deep:
            await self.emit(Event('META',sym,f'{self.session_id}:{now}',now,now,meta))
            bars=await self.client.get('/fapi/v1/klines',symbol=sym,interval='1m',limit=c.baseline_bars+c.compression_bars+5)
            receipt=self.client.now();state=self.engine.state(sym);last_ms=state['bars'][-1]['open_ms'] if state.get('bars') else 0
            for bar in bars:
                if int(bar[0])>last_ms and int(bar[6])<receipt-c.clock_uncertainty_ms:await self.emit(candle(sym,bar,receipt))
            oi=await self.client.get('/futures/data/openInterestHist',symbol=sym,period='5m',limit=30)
            for x in sorted(oi,key=lambda x:x['timestamp']):
                at=int(x['timestamp']);await self.emit(Event('OI',sym,'5m:'+str(at),at,self.client.now(),dict(value=float(x['sumOpenInterest'])),raw=x))
            chosen.append((turnover,sym,meta,move,q,quick_score))
        await self.flush()

        rankings=[]
        for turnover,sym,meta,move,q,quick_score in chosen:
            state=self.engine.state(sym);f=features(state,c);heat=f.get('heat_state','NORMAL');score=_heat_score(f)
            compressed=state['episode'] is not None
            monitor_eligible=heat not in ('NORMAL','DEAD_BURST') or compressed or sym in pins
            rankings.append(dict(symbol=sym,score=score,turnover=turnover,features=f,heat_state=heat,
                                 quick=q,quick_score=quick_score,compressed=compressed,
                                 monitor_eligible=monitor_eligible,tradable=meta['status']=='TRADING'))
            at=self.client.now();self.counter+=1
            await self.emit(Event('ATTENTION',sym,f'{self.session_id}:attention:{self.counter}',at,at,
                                  dict(heat_state=heat,score=score,features=f,quick=q,at=at)))
        await self.flush()
        rankings.sort(key=lambda x:(-x['score'],-x['turnover'],x['symbol']))
        self.rankings=rankings;self.ranked_ms=self.client.now()
        await self.select_monitoring(force=True)
        self.health.update(universe_refresh_ms=self.client.now(),quick_scan_count=len(quick_pool),deep_scan_count=len(chosen),eligible_contracts=len(eligible))

    async def select_monitoring(self,force=False):
        async with self._selection_lock:
            if not self.rankings:return
            pinned=self.engine.monitoring_pins(self.client.now())
            selected=[x['symbol'] for x in self.rankings if x['tradable'] and x.get('monitor_eligible',True)][:self.engine.cfg.universe_size]
            selected=sorted(set(selected)|pinned)
            if selected!=self.selected or force:
                now=self.client.now();self.counter+=1
                await self.emit(Event('UNIVERSE','*',f'{self.session_id}:{now}:{self.counter}',now,now,dict(selected=selected,ranking=self.rankings,ranked_ms=getattr(self,'ranked_ms',None),pinned=sorted(pinned),method='persistent-participation-v2')))
                await self.flush()
                await self.sync_streams(selected)

    async def sync_streams(self,selected):
        # Keep one venue socket per capability and update subscriptions in-place. This
        # avoids reconnecting every retained symbol whenever discovery rank changes.
        desired=set(selected)
        for group in ('public','market'):
            old=set(self._desired[group]);self._desired[group]=set(desired)
            task=self._subscriptions.get(group)
            if task is None or task.done():self._subscriptions[group]=asyncio.create_task(self.stream(group))
            ws=self._sockets.get(group)
            if ws is not None:
                await self._update_subscriptions(group,ws,old,desired)
        self.selected=sorted(desired);self.stream_tasks=list(self._subscriptions.values())

    def _stream_names(self,group,symbols):
        suffixes=('bookTicker','depth20@100ms') if group=='public' else ('aggTrade',)
        return [sym.lower()+'@'+suffix for sym in sorted(symbols) for suffix in suffixes]

    async def _control(self,ws,method,params):
        if not params or ws is None or getattr(ws, 'closed', False):return
        self._control_id=(self._control_id+1)%(2**31)
        try:
            await ws.send_json(dict(method=method,params=params,id=self._control_id))
        except Exception as exc:
            log.warning('WebSocket control command failed (%s): %s', method, exc)

    async def _update_subscriptions(self,group,ws,old,new):
        if ws is None or getattr(ws, 'closed', False):return
        add=set(new)-set(old);remove=set(old)-set(new)
        try:
            if add:await self._control(ws,'SUBSCRIBE',self._stream_names(group,add))
            if remove:await self._control(ws,'UNSUBSCRIBE',self._stream_names(group,remove))
            self._subscribed[group]=set(new)
        except Exception as exc:
            log.warning('Could not update %s subscriptions: %s', group, exc)

    async def stream(self,group,symbols=None):
        import aiohttp
        attempts=0;stale={}
        while not self.stop.is_set():
            try:
                desired=set(symbols or self._desired[group])
                for sym in desired:
                    self.counter+=1;now=self.client.now()
                    await self.emit(Event('GAP',sym,f'{self.session_id}:{group}:{self.counter}',now,now,dict(reason='STREAM_CONNECT_OR_RECONNECT',group=group)))
                # Use a persistent raw-stream socket and venue-supported live
                # SUBSCRIBE/UNSUBSCRIBE messages. normalize() accepts raw or combined.
                url='wss://fstream.binance.com/public/ws' if group=='public' else 'wss://fstream.binance.com/market/ws'
                async with self.client.session.ws_connect(url,heartbeat=20,receive_timeout=45) as ws:
                    attempts=0;self._sockets[group]=ws
                    desired=set(symbols or self._desired[group])
                    await self._control(ws,'SUBSCRIBE',self._stream_names(group,desired));self._subscribed[group]=set(desired)
                    async for msg in ws:
                        if msg.type==aiohttp.WSMsgType.TEXT:
                            raw=msg.json();received=self.client.now();event=normalize(raw,received)
                            if event:
                                lag=received-event.at
                                self.health.setdefault('symbols',{}).setdefault(event.symbol,{})[event.kind]=dict(received_ms=received,event_ms=event.at,lag_ms=lag)
                                await self.emit(event)
                                # A growing socket backlog is not live coverage. Reconnect, preserve gap evidence.
                                stale[event.symbol]=stale.get(event.symbol,0)+1 if lag>self.engine.cfg.max_exchange_lag_ms else 0
                                if stale[event.symbol]>=3:raise ConnectionError('STALE_STREAM_BACKLOG')
                            self.health[group+'_received_ms']=received
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):break
                raise ConnectionError('Stream ended')
            except asyncio.CancelledError:raise
            except Exception as exc:
                attempts+=1;log.warning('%s feed: %s',group,exc);self.health[group+'_error']=str(exc)
                await asyncio.sleep(min(30,2**min(attempts,5)))
            finally:
                self._sockets[group]=None;self._subscribed[group]=set()
    async def polls(self):
        while not self.stop.is_set():
            await self.refresh()
            await asyncio.sleep(self.engine.cfg.refresh_seconds)

    async def bar_polls(self):
        while not self.stop.is_set():
            for sym in list(self.selected):
                state=self.engine.state(sym);bars=state.get('bars',[])
                # Recover closed candles across slow REST/reconnect gaps, not only the last three.
                now=self.client.now();last=bars[-1]['open_ms'] if bars else now-120*60000
                limit=min(1500,max(3,(now-last)//60000+2))
                rows=await self.client.get('/fapi/v1/klines',symbol=sym,interval='1m',limit=limit)
                receipt=self.client.now()
                for row in rows:
                    if int(row[0])>last and int(row[6])<receipt-self.engine.cfg.clock_uncertainty_ms:await self.emit(candle(sym,row,receipt))
            self.health['bars_poll_ms']=self.client.now()
            await asyncio.sleep(15)

    async def clock_polls(self):
        while not self.stop.is_set():
            old=self.client.offset
            sample=await self.client.sync_clock();self.health['clock']=sample
            if abs(self.client.offset-old)>self.engine.cfg.clock_uncertainty_ms:
                for sym in list(self.selected):
                    now=self.client.now();self.counter+=1
                    await self.emit(Event('GAP',sym,f'{self.session_id}:clock:{self.counter}',now,now,dict(reason='CLOCK_OFFSET_CHANGED')))
            await asyncio.sleep(300)

    async def funding_polls(self):
        while not self.stop.is_set():
            now=self.client.now()
            for sym in {p['symbol'] for p in self.engine.positions(False) if p['status']=='OPEN' or (p['status']=='CLOSED' and now-p['closed_ms']<86400000)}:
                funding=await self.client.get('/fapi/v1/fundingRate',symbol=sym,limit=100)
                for x in funding:
                    at=int(x['fundingTime']);mark=float(x.get('markPrice') or 0)
                    if positive(mark):await self.emit(Event('FUNDING',sym,str(at),at,self.client.now(),dict(mark=mark,rate=float(x['fundingRate'])),raw=x))
                if funding and all(positive(float(x.get('markPrice') or 0)) for x in funding):
                    received=self.client.now();await self.emit(Event('FUNDING_SYNC',sym,f'{self.session_id}:{received}',received,received,dict(start=min(int(x['fundingTime']) for x in funding),end=received)))
            self.health['funding_poll_ms']=self.client.now()
            await asyncio.sleep(60)

    async def oi_polls(self):
        """Keep OI context fresh for actively monitored/pinned symbols independently of discovery refresh."""
        while not self.stop.is_set():
            now=self.client.now();symbols=sorted(set(self.selected)|self.engine.monitoring_pins(now))
            for sym in symbols:
                try:
                    rows=await self.client.get('/futures/data/openInterestHist',symbol=sym,period='5m',limit=3)
                    for x in sorted(rows,key=lambda x:x['timestamp']):
                        at=int(x['timestamp'])
                        await self.emit(Event('OI',sym,'5m:'+str(at),at,self.client.now(),dict(value=float(x['sumOpenInterest'])),raw=x))
                except asyncio.CancelledError:raise
                except Exception as exc:
                    self.health.setdefault('oi_errors',{})[sym]=str(exc)
            self.health['oi_poll_ms']=self.client.now()
            await asyncio.sleep(self.engine.cfg.oi_poll_seconds)

    async def timer(self):
        while not self.stop.is_set():
            try:
                now=self.client.now()
                for sym in sorted(set(self.selected)|self.engine.monitoring_pins(now,include_expired=True)):
                    await self.emit(Event('TIMER',sym,f'{self.session_id}:{now}',now,now,{}))
                if now//5000!=getattr(self,'_health_bucket',None):
                    self._health_bucket=now//5000
                    await self.select_monitoring()
                    await self.emit(Event('HEALTH','*',f'{self.session_id}:{now}',now,now,deepcopy(self.health)))
            except asyncio.CancelledError:raise
            except Exception as exc:
                log.warning('Timer cycle error (will continue): %s', exc)
            await asyncio.sleep(1)
    async def run(self):
        try:
            await self.client.open();await asyncio.to_thread(self.engine.recover)
            self.tasks=[asyncio.create_task(self.worker()),asyncio.create_task(self.polls()),asyncio.create_task(self.timer()),asyncio.create_task(self.bar_polls()),asyncio.create_task(self.funding_polls()),asyncio.create_task(self.clock_polls()),asyncio.create_task(self.oi_polls())]
            waiter=asyncio.create_task(self.stop.wait());self.tasks.append(waiter)
            done,_=await asyncio.wait(self.tasks,return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t is not waiter:t.result();raise RuntimeError('Required service stopped unexpectedly')
        finally:
            for t in self.tasks[1:]+self.stream_tasks:t.cancel()
            await asyncio.gather(*(self.tasks[1:]+self.stream_tasks),return_exceptions=True)
            if self.tasks and not self.tasks[0].done():
                try:await asyncio.wait_for(self.queue.join(),10)
                except asyncio.TimeoutError:log.error('Shutdown backlog remains; durable inputs recover on restart')
                self.tasks[0].cancel();await asyncio.gather(self.tasks[0],return_exceptions=True)
            await self.client.close()
