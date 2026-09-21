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
        self.engine=engine;self.client=client or PublicClient();self.queue=asyncio.Queue(5000);self.stop=asyncio.Event()
        self.tasks=[];self.stream_tasks=[];self._subscriptions={};self._selection_lock=asyncio.Lock();self.rankings=[];self.selected=[];self.session_id=str(time.time_ns());self.health={};self.counter=0
    async def emit(self,event):
        if event:await self.queue.put(event)
    async def worker(self):
        while True:
            batch=[await self.queue.get()]
            while len(batch)<32:
                try:batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:break
            now=self.client.now()
            self.health['queue_size']=self.queue.qsize()
            self.health['queue_delay_ms']=max(0,now-min(e.received for e in batch))
            self.health['worker_received_ms']=now
            batch=[replace(e,processed_ms=now) for e in batch]
            processing=asyncio.create_task(asyncio.to_thread(self.engine.ingest_many,batch))
            try:await asyncio.shield(processing)
            except asyncio.CancelledError:
                await processing
                raise
            finally:
                for _ in batch:self.queue.task_done()
    async def refresh(self):
        c=self.engine.cfg;info=await self.client.get('/fapi/v1/exchangeInfo');tickers=await self.client.get('/fapi/v1/ticker/24hr')
        prices={x['symbol']:x for x in tickers};candidates=[];now=self.client.now();pins=self.engine.monitoring_pins(now)
        for raw in info['symbols']:
            meta=metadata(raw);sym=raw['symbol'];t=prices.get(sym,{})
            if not meta:continue
            pinned=sym in pins
            turnover=float(t.get('quoteVolume',0))
            if (meta['status']=='TRADING' and turnover>=c.min_turnover and sym not in c.excluded_symbols) or pinned:
                candidates.append((turnover,sym,meta))
        candidates.sort(key=lambda x:(-abs(float(prices.get(x[1],{}).get('priceChangePercent',0))),-x[0],x[1]))
        chosen=candidates[:c.candidate_count]
        chosen += [x for x in candidates[c.candidate_count:] if x[1] in pins]
        for turnover,sym,meta in chosen:
            await self.emit(Event('META',sym,f'{self.session_id}:{now}',now,now,meta))
            bars=await self.client.get('/fapi/v1/klines',symbol=sym,interval='1m',limit=c.baseline_bars+c.compression_bars+5)
            receipt=self.client.now()
            s=self.engine.state(sym);last_ms=s['bars'][-1]['open_ms'] if s.get('bars') else 0
            for bar in bars:
                if int(bar[0])>last_ms and int(bar[6])<receipt-c.clock_uncertainty_ms:await self.emit(candle(sym,bar,receipt))
            oi=await self.client.get('/futures/data/openInterestHist',symbol=sym,period='5m',limit=30)
            for x in sorted(oi,key=lambda x:x['timestamp']):
                at=int(x['timestamp']);await self.emit(Event('OI',sym,'5m:'+str(at),at,self.client.now(),dict(value=float(x['sumOpenInterest'])),raw=x))
        await self.queue.join()
        rankings=[]
        for turnover,sym,meta in chosen:
            s=self.engine.state(sym);f=features(s)
            # Comparable standardized surprises; sign remains explicit in diagnostic features.
            score=max(0.,f['volume_z'] or 0.)+abs(f['oi_growth_z'] or 0.)
            rankings.append(dict(symbol=sym,score=score,turnover=turnover,features=f,compressed=s['episode'] is not None,tradable=meta['status']=='TRADING'))
        rankings.sort(key=lambda x:(-x['score'],-x['turnover'],x['symbol']))
        self.rankings=rankings;self.ranked_ms=self.client.now()
        await self.select_monitoring(force=True)
        self.health['universe_refresh_ms']=self.client.now()

    async def select_monitoring(self,force=False):
        async with self._selection_lock:
            if not self.rankings:return
            pinned=self.engine.monitoring_pins(self.client.now())
            selected=[x['symbol'] for x in self.rankings if x['tradable']][:self.engine.cfg.universe_size]
            selected=sorted(set(selected)|pinned)
            if selected!=self.selected or force:
                now=self.client.now();self.counter+=1
                await self.emit(Event('UNIVERSE','*',f'{self.session_id}:{now}:{self.counter}',now,now,dict(selected=selected,ranking=self.rankings,ranked_ms=getattr(self,'ranked_ms',None),pinned=sorted(pinned),method='relative-participation-v1')))
                await self.queue.join()
                await self.sync_streams(selected)

    async def sync_streams(self,selected):
        # Separate per-symbol subscriptions preserve existing flow across universe changes.
        desired={(group,sym) for sym in selected for group in ('public','market')}
        removed=[self._subscriptions.pop(k) for k in list(self._subscriptions) if k not in desired]
        for task in removed:task.cancel()
        if removed:await asyncio.gather(*removed,return_exceptions=True)
        for key in sorted(desired):
            if key not in self._subscriptions or self._subscriptions[key].done():
                self._subscriptions[key]=asyncio.create_task(self.stream(key[0],[key[1]]))
        self.selected=sorted(set(selected));self.stream_tasks=list(self._subscriptions.values())

    async def stream(self,group,symbols=None):
        import aiohttp
        attempts=0;symbols=tuple(symbols or self.selected);stale=0
        while not self.stop.is_set():
            try:
                for sym in symbols:
                    self.counter+=1;now=self.client.now()
                    await self.emit(Event('GAP',sym,f'{self.session_id}:{group}:{self.counter}',now,now,dict(reason='STREAM_CONNECT_OR_RECONNECT',group=group)))
                suffixes=['bookTicker','depth20@100ms'] if group=='public' else ['aggTrade']
                names='/'.join(sym.lower()+'@'+suffix for sym in symbols for suffix in suffixes)
                if not names:await asyncio.sleep(1);continue
                url=f'wss://fstream.binance.com/{group}/stream?streams={names}'
                async with self.client.session.ws_connect(url,heartbeat=20,receive_timeout=45) as ws:
                    attempts=0
                    async for msg in ws:
                        if msg.type==aiohttp.WSMsgType.TEXT:
                            raw=msg.json();received=self.client.now();event=normalize(raw,received)
                            if event:
                                lag=received-event.at
                                self.health.setdefault('symbols',{}).setdefault(event.symbol,{})[event.kind]=dict(received_ms=received,event_ms=event.at,lag_ms=lag)
                                await self.emit(event)
                                # A growing socket backlog is not live coverage. Reconnect, preserve gap evidence.
                                stale=stale+1 if lag>self.engine.cfg.max_exchange_lag_ms else 0
                                if stale>=3:raise ConnectionError('STALE_STREAM_BACKLOG')
                            self.health[group+'_received_ms']=received
                        elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):break
                raise ConnectionError('Stream ended')
            except asyncio.CancelledError:raise
            except Exception as exc:
                attempts+=1;log.warning('%s feed: %s',group,exc);self.health[group+'_error']=str(exc)
                await asyncio.sleep(min(30,2**min(attempts,5)))
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

    async def timer(self):
        while not self.stop.is_set():
            now=self.client.now()
            for sym in sorted(set(self.selected)|self.engine.monitoring_pins(now,include_expired=True)):
                await self.emit(Event('TIMER',sym,f'{self.session_id}:{now}',now,now,{}))
            if now//5000!=getattr(self,'_health_bucket',None):
                self._health_bucket=now//5000
                await self.select_monitoring()
                await self.emit(Event('HEALTH','*',f'{self.session_id}:{now}',now,now,deepcopy(self.health)))
            await asyncio.sleep(1)
    async def run(self):
        try:
            await self.client.open();await asyncio.to_thread(self.engine.recover)
            self.tasks=[asyncio.create_task(self.worker()),asyncio.create_task(self.polls()),asyncio.create_task(self.timer()),asyncio.create_task(self.bar_polls()),asyncio.create_task(self.funding_polls()),asyncio.create_task(self.clock_polls())]
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
