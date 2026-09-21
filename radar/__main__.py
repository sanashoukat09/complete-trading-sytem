import argparse,asyncio,json,logging,signal,sys,sqlite3,threading,os,time
from pathlib import Path
from dataclasses import replace
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from .config import Config
from .engine import Engine
from .model import Event,event_from_payload,dumps,digest
from .market import Feed
from .strategy import features
from .research import evaluate

HTML='''<!doctype html><html><meta charset="utf-8"><title>Compression Radar</title><style>body{background:#101925;color:#e3eefb;font:16px system-ui;margin:36px}h1{color:#63d9ba}table{border-collapse:collapse;width:100%}td,th{padding:10px;text-align:left;border-bottom:1px solid #314154}pre{white-space:pre-wrap} .note{color:#ffc879}</style><h1>Compression Radar</h1><p class="note">Experimental live-data paper trading. No real orders. Profitability unverified.</p><h2 id="mode"></h2><p id="health"></p><h2>Selected coins</h2><pre id="universe"></pre><h2>Paper positions</h2><pre id="positions"></pre><h2>Recent decisions</h2><table><thead><tr><th>Symbol</th><th>Action</th><th>Reason</th></tr></thead><tbody id="decisions"></tbody></table><h2>Completed paper outcomes</h2><pre id="outcomes"></pre><script>async function refresh(){try{let s=await(await fetch('/status.json')).json();document.getElementById('mode').textContent=s.mode+' | '+s.operational;document.getElementById('health').textContent='Events processed: '+s.processed_events+' | Pending recovery: '+s.pending_events;for(let k of ['universe','positions','outcomes'])document.getElementById(k).textContent=JSON.stringify(s[k],null,2);let b=document.getElementById('decisions');b.replaceChildren();for(let x of s.decisions){let r=document.createElement('tr');for(let k of ['symbol','action','reason']){let t=document.createElement('td');t.textContent=x[k];r.append(t)}b.append(r)}}catch(e){document.getElementById('health').textContent='Status unavailable: '+e}}refresh();setInterval(refresh,3000)</script></html>'''

def get_dashboard_html():
    p = Path(__file__).parent / 'dashboard.html'
    if p.exists():
        try:return p.read_text(encoding='utf-8')
        except Exception:pass
    return HTML

def enrich_status(engine, feed):
    # One consistent snapshot shared with the reducer's connection.
    with engine.lock:return _enrich_status(engine,feed)

def _enrich_status(engine, feed):
    s = engine.status()
    now = feed.client.now()
    s['feed_health'] = dict(feed.health)
    connected = all(0 <= now - feed.health.get(k, 0) < 10000 for k in ('public_received_ms', 'market_received_ms'))
    s['operational'] = 'RECEIVING_DATA' if connected and not s['pending_events'] else 'WARMING_OR_DEGRADED'
    s['server_time_ms'] = now
    c = engine.cfg
    realized = sum(p['net'] for p in engine.positions(False) if p['status'] == 'CLOSED')
    equity = max(0., c.initial_equity + realized)
    active_pos = engine.positions()
    risk_in_play = sum(p['risk_remaining'] for p in active_pos)
    day = now // 86400000 * 86400000
    used_daily = engine.db.execute('SELECT COALESCE(SUM(-MIN(net,0)),0) FROM outcomes WHERE closed_ms>=? AND closed_ms<?', (day, day + 86400000)).fetchone()[0]
    s['portfolio'] = {
        'initial_equity': c.initial_equity,
        'equity': equity,
        'realized_pnl': realized,
        'risk_in_play': risk_in_play,
        'risk_in_play_pct': (risk_in_play / equity * 100) if equity > 0 else 0.,
        'daily_loss_used': used_daily,
        'daily_loss_cap': c.initial_equity * c.daily_loss_fraction,
        'max_positions': c.max_positions,
        'active_positions_count': len(active_pos),
        'total_risk_fraction': c.total_risk_fraction,
        'risk_fraction': c.risk_fraction
    }
    compressions = []
    approaching = []
    market_states = {}
    try:
        rows = engine.db.execute('SELECT symbol, payload FROM states').fetchall()
        for sym, p_json in rows:
            st = json.loads(p_json)
            q = st.get('quote')
            quote_live=engine.quote_valid(st,now)
            current_price=(q['bid']+q['ask'])/2 if quote_live else None
            last_close=st['bars'][-1]['close'] if st.get('bars') else None
            ep = st.get('episode')
            ep=ep if ep and ep['expires']>now else None
            att = st.get('attempt') if ep else None
            if att and now-att['started']>c.attempt_ms:att=None
            feat = features(st) if 'features' not in st else st['features']
            market_states[sym] = {
                'symbol': sym,
                'price': current_price,
                'quote_live':quote_live,
                'quote_age_ms':now-q['received'] if q else None,
                'exchange_lag_ms':now-q['at'] if q else None,
                'last_closed_bar_price':last_close,
                'subscribed':sym in feed.selected,
                'quote': q,
                'features': feat,
                'compressed': ep is not None,
                'has_attempt': att is not None,
                'reason': st.get('reason'),
                'warm_trades': st.get('warm_trades', 0),
                'bars_count': len(st.get('bars', []))
            }
            if ep:
                lo = ep['lower']; hi = ep['upper']; w = ep['width']
                w_pct = (w / lo * 100) if lo > 0 else 0
                time_left_ms = max(0, ep['expires'] - now)
                pos_pct = None; dist_lo_pct = None; dist_hi_pct = None
                if current_price and w > 0:
                    pos_pct = ((current_price - lo) / w) * 100.0
                    dist_lo_pct = ((current_price - lo) / current_price) * 100.0
                    dist_hi_pct = ((hi - current_price) / current_price) * 100.0
                comp_obj = {
                    'symbol': sym,
                    'lower': lo,
                    'upper': hi,
                    'width': w,
                    'width_pct': w_pct,
                    'rotations': ep.get('rotations', 4),
                    'id': ep.get('id'),
                    'created': ep.get('created'),
                    'expires': ep.get('expires'),
                    'time_left_ms': time_left_ms,
                    'current_price': current_price,
                    'quote_live':quote_live,
                    'subscribed':sym in feed.selected,
                    'contraction':ep.get('contraction'),
                    'source_start_ms':ep.get('source_start_ms'),
                    'source_end_ms':ep.get('source_end_ms'),
                    'pos_pct': pos_pct,
                    'dist_lo_pct': dist_lo_pct,
                    'dist_hi_pct': dist_hi_pct,
                    'attempt': att
                }
                compressions.append(comp_obj)
                if att and att.get('stage')!='CONSUMED' and quote_live:
                    stage = att.get('stage')
                    direction = att.get('direction')
                    kind = 'SPRING' if direction == 1 else 'UPTHRUST'
                    approaching.append({
                        'symbol': sym,
                        'kind': kind,
                        'stage': stage,
                        'extreme': att.get('extreme'),
                        'reclaim_price': att.get('reclaim_price'),
                        'started': att.get('started'),
                        'urgency': 'CRITICAL' if stage == 'RECLAIMED' else 'HIGH',
                        'detail': f"{kind} in progress: {stage}",
                        'comp': comp_obj
                    })
                elif current_price and dist_lo_pct is not None and dist_hi_pct is not None:
                    if 0 <= pos_pct <= 20:
                        approaching.append({
                            'symbol': sym,
                            'kind': 'SPRING_WATCH',
                            'stage': 'NEAR_SUPPORT',
                            'urgency': 'MEDIUM',
                            'detail': f"Approaching support ({dist_lo_pct:+.2f}%)",
                            'comp': comp_obj
                        })
                    elif 80 <= pos_pct <= 100:
                        approaching.append({
                            'symbol': sym,
                            'kind': 'UPTHRUST_WATCH',
                            'stage': 'NEAR_RESISTANCE',
                            'urgency': 'MEDIUM',
                            'detail': f"Approaching resistance ({dist_hi_pct:+.2f}%)",
                            'comp': comp_obj
                        })
    except Exception as exc:
        s['enrichment_error'] = str(exc)
    s['coverage'] = dict(selected=len(feed.selected),fresh_quotes=sum(m['quote_live'] for sym,m in market_states.items() if sym in feed.selected),queue_size=feed.queue.qsize())
    if not s['coverage']['fresh_quotes'] or feed.health.get('queue_delay_ms',0)>c.max_exchange_lag_ms:s['operational']='WARMING_OR_DEGRADED'
    for p in s['positions']:
        st=market_states.get(p['symbol'],{});q=st.get('quote')
        mark=(q['bid'] if p['direction']==1 else q['ask']) if st.get('quote_live') else None
        p['current_price']=mark
        p['unrealized_mark_pnl']=p['direction']*(mark-p['entry_price'])*p['remaining'] if mark is not None and p.get('entry_price') is not None else None
    for ranked in s.get('universe',{}).get('ranking',[]):
        ranked['compressed']=market_states.get(ranked['symbol'],{}).get('compressed',False)
    s['compressions'] = compressions
    s['approaching'] = approaching
    s['market_states'] = market_states
    s['config'] = c.to_dict()
    return s

def dashboard(engine,feed,port):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*a):pass
        def do_GET(self):
            if self.path not in ('/','/index.html','/status.json'):self.send_error(404);return
            try:
                if self.path in ('/','/index.html'):body=get_dashboard_html().encode();typ='text/html'
                else:
                    s=enrich_status(engine,feed)
                    body=dumps(s).encode();typ='application/json'
                self.send_response(200);self.send_header('Content-Type',typ+'; charset=utf-8');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(body)
            except Exception:self.send_error(503)
    server=ThreadingHTTPServer(('127.0.0.1',port),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start();return server

async def live_data(engine,port):
    feed=Feed(engine);server=dashboard(engine,feed,port) if port else None
    loop=asyncio.get_running_loop()
    for sig in (signal.SIGINT,signal.SIGTERM):
        try:loop.add_signal_handler(sig,feed.stop.set)
        except (NotImplementedError,RuntimeError):pass
    print(f'Mode: {engine.cfg.mode}. Real orders disabled. Dashboard http://127.0.0.1:{port}/',flush=True)
    try:await feed.run()
    finally:
        if server:server.shutdown();server.server_close()

def readonly(path):
    return sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)

def main(argv=None):
    p=argparse.ArgumentParser(description='Compression Radar — experimental public-data paper runtime, no real orders')
    sub=p.add_subparsers(dest='command',required=True)
    for name in ('paper','shadow','collection'):
        a=sub.add_parser(name);a.add_argument('--config',default='config.json');a.add_argument('--data-dir',default='data');a.add_argument('--port',type=int,default=8780)
    a=sub.add_parser('demo');a.add_argument('--data-dir',default='demo-data')
    sub.add_parser('doctor')
    a=sub.add_parser('export');a.add_argument('--data-dir',default='data');a.add_argument('--output',default='paper-positions.csv')
    a=sub.add_parser('status');a.add_argument('--data-dir',default='data')
    a=sub.add_parser('research');a.add_argument('--data-dir',default='data')
    a=sub.add_parser('replay');a.add_argument('--source',required=True);a.add_argument('--destination',required=True)
    args=p.parse_args(argv);logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    try:
        if args.command=='doctor':
            from .market import PublicClient
            async def diagnose():
                client=PublicClient()
                try:
                    await client.open();print('REST server time: OK; adjusted clock offset (ms):',client.offset)
                    info=await client.get('/fapi/v1/exchangeInfo');print('Metadata symbols:',len(info['symbols']))
                    for suffix in ('bookTicker','depth20@100ms','aggTrade'):
                        lane='public' if suffix in ('bookTicker','depth20@100ms') else 'market'
                        async with client.session.ws_connect(f'wss://fstream.binance.com/{lane}/stream?streams=btcusdt@{suffix}',receive_timeout=10) as ws:
                            raw=await asyncio.wait_for(ws.receive_json(),10);print(lane,suffix,raw.get('data',raw).get('e'),'OK')
                    print('Read-only connectivity passed. This does not verify profitability or an extended soak.')
                finally:await client.close()
            asyncio.run(diagnose());return 0
        if args.command=='demo':
            from .demo import setup,quote,BASE,SYM
            path=Path(args.data_dir)/'radar.db'
            if path.exists():raise ValueError('Demo directory must be new; choose --data-dir with a fresh name')
            e=Engine(path)
            try:
                now,_=setup(e);p=e.positions()[0]
                if p['status']=='PENDING':quote(e,now+100,p['entry_reference']/(1+e.cfg.slippage_fraction))
                p=e.positions()[0];quote(e,now+1000,p['tp1']+.05);quote(e,now+2000,p['tp2']+.1 if p['tp2'] else p['tp1']+.1)
                print(json.dumps(e.status(),indent=2));print('SYNTHETIC DEMO ONLY. These are not observed market profits.')
            finally:e.close()
            return 0
        if args.command=='export':
            import csv
            con=readonly(Path(args.data_dir)/'radar.db')
            try:
                rows=[json.loads(r[0]) for r in con.execute('SELECT payload FROM positions ORDER BY id')]
                columns=['id','symbol','status','direction','entry_ms','entry_price','original_qty','remaining','stop','tp1','tp2','gross','fees','funding','net','initial_risk','closed_ms']
                with open(args.output,'w',newline='',encoding='utf-8') as f:
                    w=csv.DictWriter(f,fieldnames=columns,extrasaction='ignore');w.writeheader();w.writerows(rows)
                print('Exported',len(rows),'paper positions to',args.output)
            finally:con.close()
            return 0
        if args.command in ('status','research'):
            con=readonly(Path(args.data_dir)/'radar.db')
            try:
                if args.command=='research':print(json.dumps(evaluate([json.loads(r[0]) for r in con.execute('SELECT payload FROM outcomes')]),indent=2))
                else:
                    print('Pending events:',con.execute('SELECT COUNT(*) FROM events WHERE applied=0').fetchone()[0]);print('Active paper positions:')
                    for r in con.execute("SELECT payload FROM positions WHERE status IN ('PENDING','OPEN')"):print(r[0])
                    print('Latest decisions:')
                    for r in con.execute('SELECT seq,symbol,action,reason FROM decisions ORDER BY seq DESC LIMIT 20'):print(r)
            finally:con.close()
            return 0
        if args.command=='replay':
            if Path(args.destination).exists():raise ValueError('Replay destination must not exist')
            source=readonly(args.source)
            try:
                cfg=Config(**json.loads(source.execute("SELECT value FROM manifest WHERE key='config'").fetchone()[0]));e=Engine(args.destination,cfg)
                try:
                    original=source.execute("SELECT value FROM manifest WHERE key='code_hash'").fetchone()[0]
                    current=e.db.execute("SELECT value FROM manifest WHERE key='code_hash'").fetchone()[0]
                    if original!=current:raise ValueError('Replay requires the same code revision')
                    for r in source.execute('SELECT payload FROM events ORDER BY seq'):e.ingest(event_from_payload(r[0]))
                    print(json.dumps(e.status(),indent=2));print('Replay completed offline.')
                finally:e.close()
            finally:source.close()
            return 0
        cfg=replace(Config.load(args.config),mode=args.command).validate();engine=Engine(Path(args.data_dir)/'radar.db',cfg)
        try:asyncio.run(live_data(engine,args.port))
        finally:engine.close()
        return 0
    except KeyboardInterrupt:return 0
    except Exception as exc:logging.exception('Stopped: %s',exc);return 1

if __name__=='__main__':sys.exit(main())
