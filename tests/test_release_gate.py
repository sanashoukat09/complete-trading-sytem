import asyncio
import json
import pytest

from radar.engine import Engine
from radar.market import Feed, normalize
from radar.model import Event, event_from_payload
from .fixtures import setup, quote, SYM, BASE
from .test_feed import FakeClient


@pytest.mark.parametrize('direction',[1,-1])
def test_release_full_trade_tp1_then_breakeven_protect(tmp_path,direction):
    e=Engine(tmp_path/f'manager-{direction}.db')
    try:
        now,_=setup(e,direction)
        p=e.positions()[0]
        if p['status']=='PENDING':
            quote(e,now+100,p['entry_reference']/(1+direction*e.cfg.slippage_fraction))
        p=e.positions()[0]
        assert p['status']=='OPEN'
        # Reach first structural objective and bank the configured partial.
        quote(e,now+1000,p['tp1']+direction*.05)
        p=e.positions()[0]
        assert p['status']=='OPEN' and p['partial_done']
        assert 0 < p['remaining'] < p['original_qty']
        assert direction*(p['management_stop']-p['entry_price']) > 0
        # A reversal through the cost-adjusted BE protection must flatten the remainder.
        quote(e,now+2000,p['management_stop']-direction*.03)
        assert not e.positions()
        out=e.status()['outcomes'][-1]
        assert out['status']=='BREAKEVEN_PROTECT'
        assert out['remaining_qty']==0
        # TP1 was banked before protection, so this controlled fixture must remain net positive.
        assert out['net'] > 0 and out['net_r'] > 0
        executions=[json.loads(r[0]) for r in e.db.execute('select payload from executions order by seq')]
        assert [x['reason'] for x in executions]==['ENTRY','TP1_HIT','BREAKEVEN_PROTECT']
    finally:
        e.close()


@pytest.mark.parametrize('direction',[1,-1])
def test_release_full_trade_tp1_then_tp2(tmp_path,direction):
    e=Engine(tmp_path/f'tp2-{direction}.db')
    try:
        now,_=setup(e,direction)
        p=e.positions()[0]
        if p['status']=='PENDING':
            quote(e,now+100,p['entry_reference']/(1+direction*e.cfg.slippage_fraction))
        p=e.positions()[0]
        quote(e,now+1000,p['tp1']+direction*.05)
        p=e.positions()[0]
        assert p['partial_done'] and p['tp2'] is not None
        quote(e,now+2000,p['tp2']+direction*.10)
        assert not e.positions()
        out=e.status()['outcomes'][-1]
        assert out['status']=='TP2_HIT' and out['net_r'] > 0
    finally:
        e.close()


def test_release_live_subscription_delta_uses_persistent_socket(tmp_path):
    class WS:
        def __init__(self): self.messages=[]
        async def send_json(self,payload): self.messages.append(payload)
    async def run():
        e=Engine(tmp_path/'subs.db')
        try:
            f=Feed(e,FakeClient()); ws=WS()
            await f._update_subscriptions('market',ws,{'AAAUSDT'},{'AAAUSDT','BBBUSDT'})
            await f._update_subscriptions('market',ws,{'AAAUSDT','BBBUSDT'},{'BBBUSDT'})
            assert ws.messages[0]['method']=='SUBSCRIBE'
            assert ws.messages[0]['params']==['bbbusdt@aggTrade']
            assert ws.messages[1]['method']=='UNSUBSCRIBE'
            assert ws.messages[1]['params']==['aaausdt@aggTrade']
        finally:e.close()
    asyncio.run(run())


def test_release_raw_market_message_is_losslessly_replayable(tmp_path):
    e=Engine(tmp_path/'raw.db')
    try:
        raw=dict(e='aggTrade',s=SYM,E=BASE,T=BASE,a=7,p='100.25',q='3.5',m=False,
                 f=100,l=102,extra={'venue':'binance','x':[1,2,3]})
        event=normalize(raw,BASE+17)
        e.ingest(event)
        stored=e.db.execute('select payload from events').fetchone()[0]
        recovered=event_from_payload(stored)
        assert recovered.raw==raw
        assert recovered.data==event.data
        assert recovered.identity()==event.identity()
    finally:e.close()


def test_release_binance_2026_stream_lanes_are_current():
    # Binance retired the old root /ws path for general USD-M streams in April 2026.
    # The runtime must keep high-frequency book data on public and aggTrade on market.
    from pathlib import Path
    src=Path(__file__).parents[1].joinpath('radar','market.py').read_text(encoding='utf-8')
    assert "wss://fstream.binance.com/public/ws" in src
    assert "wss://fstream.binance.com/market/ws" in src
    assert "url='wss://fstream.binance.com/ws'" not in src
