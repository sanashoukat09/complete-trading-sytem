import asyncio,json
from dataclasses import replace
from radar.strategy import initial,features
from radar.config import Config
from radar.market import Feed,_heat_score,_diversified_symbols
from radar.engine import Engine
from radar.model import Event,dumps
from .fixtures import BASE,SYM,bootstrap
from .test_feed import FakeClient


def _state(vol_blocks,oi_values):
    s=initial();t=BASE-len(vol_blocks)*5*60000
    for total in vol_blocks:
        for _ in range(5):
            s['bars'].append(dict(open_ms=t,close_ms=t+59999,open=100,high=101,low=99,close=100,volume=total/5));t+=60000
    t=BASE-len(oi_values)*300000
    for v in oi_values:
        s['oi'].append(dict(at=t,value=v));t+=300000
    return s

BASE_V=[500,510,490,505,495,515,485,500,520,480,505,495,500,510,490,505,495,500,510,490,500,505]
BASE_OI=[100+0.05*((-1)**i) for i in range(20)]


def test_single_burst_that_fully_unwinds_is_dead_not_hot():
    f=features(_state(BASE_V+[2500,500],BASE_OI+[100,105,100]),Config())
    assert f['heat_state']=='DEAD_BURST'
    assert f['oi_retention']<Config().heat_dead_retention_max
    assert _heat_score(f)==0


def test_partial_cooling_with_retained_oi_stays_interesting():
    f=features(_state(BASE_V+[2500,900],BASE_OI+[100,105,104]),Config())
    assert f['heat_state']=='HOT_RETAINED'
    assert f['oi_retention']>.75 and _heat_score(f)>0


def test_high_volume_oi_deleveraging_is_flush_not_dead():
    f=features(_state(BASE_V+[500,2500],BASE_OI+[100,101,95]),Config())
    assert f['heat_state']=='FLUSH_EVENT'
    assert f['oi_growth_15m']<0 and _heat_score(f)>0


def test_multiwindow_build_is_sustained_hot():
    f=features(_state(BASE_V+[1600,1800],BASE_OI+[100,103,106]),Config())
    assert f['heat_state']=='SUSTAINED_HOT'
    assert f['oi_retention']>.9 and _heat_score(f)>0


def test_default_discovery_is_much_broader_than_old_40():
    c=Config()
    assert c.candidate_count>=120 and c.quick_scan_count>c.candidate_count and c.universe_size>=20
    rows=[(1_000_000+i,f'S{i}USDT',{},(-1)**i*(i%30)) for i in range(200)]
    out=_diversified_symbols(rows,c.candidate_count)
    assert len(out)==c.candidate_count and len({x[1] for x in out})==c.candidate_count


def test_dead_burst_not_streamed_but_retained_and_flush_are(tmp_path):
    async def run():
        e=Engine(tmp_path/'heat-select.db',replace(Config(),universe_size=3,candidate_count=3,quick_scan_count=3))
        f=Feed(e,FakeClient())
        f.rankings=[
            dict(symbol='DEADUSDT',score=99,turnover=1e8,tradable=True,monitor_eligible=False,heat_state='DEAD_BURST'),
            dict(symbol='RETUSDT',score=6,turnover=1e8,tradable=True,monitor_eligible=True,heat_state='HOT_RETAINED'),
            dict(symbol='FLUSHUSDT',score=5,turnover=1e8,tradable=True,monitor_eligible=True,heat_state='FLUSH_EVENT'),
        ]
        emitted=[]
        async def emit(ev):emitted.append(ev)
        async def flush():pass
        async def sync(selected):f.selected=sorted(selected)
        f.emit=emit;f.flush=flush;f.sync_streams=sync
        await f.select_monitoring(force=True)
        assert set(f.selected)=={'RETUSDT','FLUSHUSDT'}
        assert emitted and emitted[-1].data['method']=='persistent-participation-v2'
        e.close()
    asyncio.run(run())


def test_dead_heat_releases_passive_balance_but_not_live_attempt(tmp_path):
    e=Engine(tmp_path/'pin-heat.db');bootstrap(e)
    now=BASE+3000
    e.ingest(Event('ATTENTION',SYM,'dead',now,now,dict(heat_state='DEAD_BURST',score=0,features={},quick={},at=now)))
    assert SYM not in e.monitoring_pins(now)
    s=e.state(SYM);ep=s['episode']
    # Persist a causal attempt to prove discovery cooling can no longer evict it.
    s['attempt']=dict(id='AT_TEST',direction=1,started=now,stage='OUTSIDE',extreme=ep['lower']-.1,evidence=[],outside=[],response=[])
    e.db.execute('INSERT OR REPLACE INTO states VALUES(?,?)',(SYM,dumps(s)))
    assert SYM in e.monitoring_pins(now)
    e.close()

def test_refresh_deep_scans_more_than_old_40_end_to_end(tmp_path):
    async def run():
        cfg=replace(Config(),candidate_count=50,quick_scan_count=60,universe_size=8,min_turnover=1)
        e=Engine(tmp_path/'broad-refresh.db',cfg)
        class BroadClient:
            def now(self):return BASE+3000
            async def get(self,path,**kw):
                if path.endswith('exchangeInfo'):
                    from .fixtures import raw_meta
                    return {'symbols':[dict(raw_meta(),symbol=f'C{i}USDT') for i in range(60)]}
                if path.endswith('24hr'):
                    return [dict(symbol=f'C{i}USDT',quoteVolume=str(50_000_000+i*1000),priceChangePercent=str((i%20)-10)) for i in range(60)]
                if path.endswith('klines'):
                    limit=kw.get('limit',125);sym=kw['symbol'];bias=int(sym[1:-4])%5
                    out=[]
                    for i in range(limit):
                        at=BASE-limit*60000+i*60000;vol=100+(i%7)
                        if i>=limit-10:vol=500+bias*20
                        out.append([at,100,101,99,100,vol,at+59999])
                    return out
                if path.endswith('openInterestHist'):
                    vals=[]
                    for i in range(30):
                        v=10000*(1.0002**i)
                        if i>=27:v*=1.03**(i-26)
                        vals.append(dict(timestamp=BASE-(29-i)*300000,sumOpenInterest=v))
                    return vals
                raise AssertionError(path)
        f=Feed(e,BroadClient())
        async def parked(*a):await asyncio.Event().wait()
        f.stream=parked;worker=asyncio.create_task(f.worker())
        try:
            await f.refresh();await f.queue.join()
            assert len(f.rankings)==50
            assert f.health['quick_scan_count']==60 and f.health['deep_scan_count']==50
            assert len(f.selected)<=8 and f.selected
            assert all(r['heat_state']!='DEAD_BURST' for r in f.rankings if r['symbol'] in f.selected)
        finally:
            worker.cancel()
            for t in f.stream_tasks:t.cancel()
            await asyncio.gather(worker,*f.stream_tasks,return_exceptions=True);e.close()
    asyncio.run(run())


def test_oi_heavy_outscores_volume_churn():
    f_vol = {'heat_state': 'NORMAL', 'volume_5m_z': 5.0, 'volume_10m_z': 2.0, 'oi_growth_z': 0.1, 'oi_growth_15m_z': 0.1, 'oi_retention': 0.1}
    f_oi = {'heat_state': 'NORMAL', 'volume_5m_z': 1.0, 'volume_10m_z': 0.5, 'oi_growth_z': 3.0, 'oi_growth_15m_z': 2.5, 'oi_retention': 0.95}
    assert _heat_score(f_oi) > _heat_score(f_vol)


def test_compressed_normal_requires_real_oi_backing(tmp_path):
    async def run():
        e = Engine(tmp_path / 'oi-select.db', replace(Config(), universe_size=5))
        f = Feed(e, FakeClient())
        f.rankings = [
            # Dormant normal compression with no OI backing -> rejected
            dict(symbol='DEADCOMPUSDT', score=5, turnover=1e8, tradable=True, heat_state='NORMAL', compressed=True,
                 monitor_eligible=False, features={'oi_retention': 0.1, 'oi_growth_z': -1.2, 'volume_5m_z': -0.5}),
            # Normal compression with strong OI build and retention -> approved
            dict(symbol='HOTCOMPUSDT', score=6, turnover=1e8, tradable=True, heat_state='NORMAL', compressed=True,
                 monitor_eligible=True, features={'oi_retention': 0.95, 'oi_growth_z': 1.8, 'volume_5m_z': 0.8}),
            # Active heat coin -> approved
            dict(symbol='ACTIVERETUSDT', score=7, turnover=1e8, tradable=True, heat_state='HOT_RETAINED', compressed=False,
                 monitor_eligible=True, features={'oi_retention': 0.85, 'oi_growth_z': 0.6, 'volume_5m_z': 0.6}),
        ]
        emitted = []
        async def emit(ev): emitted.append(ev)
        async def flush(): pass
        async def sync(selected): f.selected = sorted(selected)
        f.emit = emit; f.flush = flush; f.sync_streams = sync
        await f.select_monitoring(force=True)
        assert set(f.selected) == {'ACTIVERETUSDT', 'HOTCOMPUSDT'}
        assert 'DEADCOMPUSDT' not in f.selected
        e.close()
    asyncio.run(run())
