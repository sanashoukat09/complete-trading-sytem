"""Single-writer durable paper engine. No exchange order API exists here."""
import sqlite3,json,threading,math,time,os
from dataclasses import asdict
from pathlib import Path
from contextlib import contextmanager
from .model import Event,dumps,digest,positive,round_step,pack_event_payload,event_from_payload
from .strategy import initial,observe,features
from .config import Config

SCHEMA='''
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA wal_autocheckpoint=256;
PRAGMA journal_size_limit=67108864;
CREATE TABLE IF NOT EXISTS manifest(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,identity TEXT UNIQUE NOT NULL,hash TEXT NOT NULL,payload BLOB NOT NULL,applied INTEGER NOT NULL DEFAULT 0,error TEXT);
CREATE INDEX IF NOT EXISTS pending_events ON events(applied,seq);
CREATE TABLE IF NOT EXISTS runtime_health(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS states(symbol TEXT PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,seq INTEGER NOT NULL,symbol TEXT NOT NULL,action TEXT NOT NULL,reason TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS positions(id TEXT PRIMARY KEY,symbol TEXT NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_symbol ON positions(symbol) WHERE status IN ('PENDING','OPEN');
CREATE TABLE IF NOT EXISTS executions(id TEXT PRIMARY KEY,position_id TEXT NOT NULL,seq INTEGER NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outcomes(position_id TEXT PRIMARY KEY,closed_ms INTEGER NOT NULL,net REAL NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS universe(id INTEGER PRIMARY KEY CHECK(id=1),payload TEXT NOT NULL);
'''

def locked_read(fn):
    def wrapped(self,*args,**kwargs):
        with self.lock:return fn(self,*args,**kwargs)
    return wrapped

class Engine:
    def __init__(self,path,config=None):
        self.path=str(path);self.cfg=(config or Config()).validate();Path(path).parent.mkdir(parents=True,exist_ok=True)
        self.lock=threading.RLock();self.db=sqlite3.connect(path,timeout=30,check_same_thread=False,isolation_level=None)
        self.db.row_factory=sqlite3.Row;self.db.executescript(SCHEMA)
        cfg=dumps(self.cfg.to_dict());row=self.db.execute("SELECT value FROM manifest WHERE key='config'").fetchone()
        if row and row[0]!=cfg:
            self.db.close();raise ValueError('Configuration differs from frozen database. Use a new data directory for a new experiment.')
        self.db.execute("INSERT OR IGNORE INTO manifest VALUES('config',?)",(cfg,))
        self.db.execute("INSERT OR IGNORE INTO manifest VALUES('schema','6.1.3-persistent-heat')")
        # Hash executable implementation, not only configuration.
        code=digest({p.name:p.read_text(encoding='utf-8') for p in sorted(Path(__file__).parent.glob('*.py'))})
        old=self.db.execute("SELECT value FROM manifest WHERE key='code_hash'").fetchone()
        if old and old[0]!=code:
            self.db.close();raise ValueError('Code changed: archive this experiment and start a new database')
        self.db.execute("INSERT OR IGNORE INTO manifest VALUES('code_hash',?)",(code,))
        self.fault=None # tests can inject a crash before commit; never alters validation

    @contextmanager
    def transaction(self):
        with self.lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:yield;self.db.execute('COMMIT')
            except BaseException:self.db.execute('ROLLBACK');raise

    @locked_read
    def state(self,symbol):
        row=self.db.execute('SELECT payload FROM states WHERE symbol=?',(symbol,)).fetchone()
        return json.loads(row[0]) if row else initial()

    def _state_unlocked(self,symbol):
        row=self.db.execute('SELECT payload FROM states WHERE symbol=?',(symbol,)).fetchone()
        return json.loads(row[0]) if row else initial()

    @locked_read
    def positions(self,active=True):
        q="SELECT payload FROM positions"+(" WHERE status IN ('PENDING','OPEN')" if active else '')+' ORDER BY id'
        return [json.loads(r[0]) for r in self.db.execute(q)]

    def _save_position(self,p):
        self.db.execute('INSERT INTO positions VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,payload=excluded.payload',
                        (p['id'],p['symbol'],p['status'],dumps(p)))

    def ingest(self,event):
        result=self.ingest_many([event])
        return result[0] if result else None

    @locked_read
    def ingest_many(self,events):
        prepared=[]
        for event in events:
            if event.kind=='BAR' and event.data.get('close_ms',event.received)>=event.received-self.cfg.clock_uncertainty_ms:continue
            event.validate();prepared.append((event,event.identity(),event.semantic_hash(),pack_event_payload(dumps(asdict(event)))))
        if not prepared:return []
        if Path(self.path).stat().st_size+sum(p.stat().st_size for p in Path(self.path).parent.glob(Path(self.path).name+'-wal'))>self.cfg.max_disk_mb*1024*1024:
            raise OSError('Configured journal disk budget reached; stop and archive the experiment before restarting')
        result=[]
        with self.transaction():
            for event,identity,h,payload in prepared:
                row=self.db.execute('SELECT seq,hash FROM events WHERE identity=?',(identity,)).fetchone()
                if row:
                    if row['hash']!=h and event.kind!='BAR':raise ValueError('Conflicting duplicate event: '+identity)
                    seq=row['seq']
                else:seq=self.db.execute('INSERT INTO events(identity,hash,payload) VALUES(?,?,?)',(identity,h,payload)).lastrowid
                result.append(seq)
        self.recover()
        return result

    def recover(self):
        while True:
            with self.lock:rows=self.db.execute('SELECT seq,payload FROM events WHERE applied=0 ORDER BY seq LIMIT ?',
                                                (self.cfg.reducer_batch_size,)).fetchall()
            if not rows:return
            try:
                # Bounded groups amortize disk sync. A failed group rolls back every state/offset effect.
                with self.transaction():
                    state_cache={};dirty=set();applied=[]
                    for row in rows:
                        if self.db.execute('SELECT applied FROM events WHERE seq=?',(row['seq'],)).fetchone()[0]:continue
                        e=event_from_payload(row['payload']);self._reduce(e,row['seq'],state_cache,dirty)
                        if self.fault:self.fault('before_commit',row['seq'])
                        applied.append((row['seq'],))
                    if dirty:
                        self.db.executemany('INSERT OR REPLACE INTO states VALUES(?,?)',
                                            [(sym,dumps(state_cache[sym])) for sym in sorted(dirty)])
                    if applied:self.db.executemany('UPDATE events SET applied=1,error=NULL WHERE seq=?',applied)
            except BaseException as exc:
                with self.lock:self.db.execute('UPDATE events SET error=? WHERE seq=?',(str(exc)[:1000],row['seq']))
                raise

    def _decision(self,e,seq,action,reason,payload=None):
        self.db.execute('INSERT OR REPLACE INTO decisions VALUES(?,?,?,?,?,?)',
            ('D_'+digest([e.identity(),action,reason])[:32],seq,e.symbol,action,reason,dumps(payload or {})))

    def _reduce(self,e,seq,state_cache=None,dirty=None):
        if e.kind=='HEALTH':
            self.db.execute('INSERT OR REPLACE INTO runtime_health VALUES(1,?)',(dumps(dict(e.data,at=e.decision_ms)),));return
        if e.kind=='UNIVERSE':
            self.db.execute('INSERT OR REPLACE INTO universe VALUES(1,?)',(dumps(e.data),));return
        if state_cache is None:
            s=self._state_unlocked(e.symbol)
        else:
            if e.symbol not in state_cache:state_cache[e.symbol]=self._state_unlocked(e.symbol)
            s=state_cache[e.symbol]
        before=(s.get('reason'),(s.get('attempt') or {}).get('stage'),(s.get('episode') or {}).get('id'))
        candidate=observe(s,e,self.cfg)
        if e.kind in ('QUOTE','DEPTH','TRADE','TIMER','FUNDING','FUNDING_SYNC','GAP','META'):
            self._manage(s,e,seq)
        if candidate and self.cfg.mode!='collection':self._propose(s,e,seq,candidate)
        elif e.kind in ('BAR','TIMER') or before!=(s.get('reason'),(s.get('attempt') or {}).get('stage'),(s.get('episode') or {}).get('id')):

            self._decision(e,seq,'WAIT',s['reason'],dict(features=features(s),episode=s['episode'],attempt=s['attempt']))
        if state_cache is None:self.db.execute('INSERT OR REPLACE INTO states VALUES(?,?)',(e.symbol,dumps(s)))
        else:dirty.add(e.symbol)

    @locked_read
    def monitoring_pins(self,now,include_expired=False):
        pins={p['symbol'] for p in self.positions()}
        for sym,payload in self.db.execute('SELECT symbol,payload FROM states'):
            s=json.loads(payload);ep=s.get('episode');attempt=s.get('attempt');attention=s.get('attention') or {}
            # Once a causal attempt exists, keep watching regardless of discovery cooling.
            # Before an attempt, a one-off burst that fully unwound may release an otherwise
            # passive balance so scarce live streams follow genuinely active auctions.
            f=attention.get('features') or {}
            g5=abs(f.get('oi_growth') or 0.0);g15=abs(f.get('oi_growth_15m') or 0.0)
            g_base=abs(f.get('oi_growth_base') or 0.0);v5_z=abs(f.get('volume_5m_z') or 0.0)
            is_active=(g5>=0.0025 or g15>=0.0025 or g_base>=0.0075 or v5_z>=1.0)
            active_heat=attention.get('heat_state')!='DEAD_BURST' and (not attention or is_active)
            structural=bool(attempt) or (ep and active_heat)
            if structural and (include_expired or not ep or ep['expires']>now) and (s.get('meta') or {}).get('status')=='TRADING':pins.add(sym)
        return pins

    def quote_valid(self,s,now):
        q=s.get('quote');c=self.cfg
        return bool(q and positive(q.get('bid')) and positive(q.get('ask')) and q['bid']<=q['ask']
                    and 0<=now-q['received']<=c.max_quote_age_ms and 0<=now-q['at']<=c.max_exchange_lag_ms
                    and (q['ask']-q['bid'])/q['bid']<=c.max_spread_fraction)

    def _price(self,s,direction,qty,now):
        if not self.quote_valid(s,now):raise ValueError('QUOTE_UNAVAILABLE_OR_STALE')
        q=s['quote'];price=q['ask'] if direction==1 else q['bid'];depth=s.get('depth')
        if self.cfg.require_depth:
            if not depth or not 0<=now-depth['received']<=self.cfg.max_quote_age_ms or not 0<=now-depth['at']<=self.cfg.max_exchange_lag_ms:
                raise ValueError('DEPTH_UNAVAILABLE_OR_STALE')
            levels=depth['asks'] if direction==1 else depth['bids'];left=qty;total=0.
            if not levels:raise ValueError('DEPTH_EMPTY')
            for p,v in levels:
                take=min(left,v*self.cfg.depth_participation);total+=take*p;left-=take
                if left<1e-10:break
            if left>1e-8:raise ValueError('INSUFFICIENT_EXECUTABLE_DEPTH')
            price=total/qty
            # A fresher best quote cannot be made more favorable by an older depth snapshot.
            price=max(price,q['ask']) if direction==1 else min(price,q['bid'])
        return price*(1+direction*self.cfg.slippage_fraction)

    def _selected(self,symbol):
        row=self.db.execute('SELECT payload FROM universe WHERE id=1').fetchone()
        return bool(row and symbol in json.loads(row[0])['selected'])

    def _propose(self,s,e,seq,a):
        c=self.cfg;now=e.decision_ms;meta=s['meta'];direction=a['direction'];reason=None
        # Discovery/ranking decides where to spend attention. Once a frozen episode and
        # causal attempt exist, transient rank churn or an OI polling hiccup must not
        # veto the market evidence that the system is already monitoring.
        monitored=self._selected(e.symbol) or bool(s.get('episode') and s['episode']['expires']>now)
        if not monitored:reason='NOT_MONITORED'
        elif not meta or meta.get('status')!='TRADING':reason='NOT_TRADABLE'
        elif s['warm_trades']<20:reason='FLOW_WARMUP'
        elif self.db.execute("SELECT 1 FROM positions WHERE symbol=? AND status IN ('PENDING','OPEN')",(e.symbol,)).fetchone():reason='EXPOSURE_ALREADY_EXISTS'
        if reason:s['reason']=reason;self._decision(e,seq,'WAIT',reason,a);return
        try:
            entry=self._price(s,direction,meta['step_size'],now)
            stop=round_step(a['stop'],meta['tick_size'],up=direction==-1)
            realized=sum(p['net'] for p in self.positions(False) if p['status']=='CLOSED')
            equity=max(0.,c.initial_equity+realized)
            per_unit=abs(entry-stop)+entry*(2*c.fee_fraction+c.slippage_fraction+c.funding_allowance_fraction)
            qty=round_step(min(equity*c.risk_fraction/per_unit,equity*c.max_notional_multiple/entry,meta['max_qty']),meta['step_size'])
            if qty<meta['min_qty'] or qty*entry<meta['min_notional']:raise ValueError('SIZE_BELOW_VENUE_MINIMUM')
            entry=self._price(s,direction,qty,now)
            cost=(entry+abs(stop))*c.fee_fraction+entry*(c.slippage_fraction+c.funding_allowance_fraction)
            unit_risk=direction*(entry-stop)+cost
            if unit_risk<=0 or direction*(entry-stop)<=0:raise ValueError('STOP_OBSOLETE')
            # Midpoint target only if it independently pays for risk; otherwise opposite balance edge.
            target=a['mid'];tp2=a['far']
            def rr(t):return (direction*(t-entry)-(entry+t)*c.fee_fraction-entry*(c.slippage_fraction+c.funding_allowance_fraction))/unit_risk
            if rr(target)<c.min_net_rr:target=a['far'];tp2=None
            target=round_step(target,meta['tick_size'],up=direction==-1)
            if rr(target)<c.min_net_rr:raise ValueError('INSUFFICIENT_NET_REWARD')
            # Re-size after depth/slippage recomputation, never silently exceed the budget.
            qty=min(qty,round_step(equity*c.risk_fraction/unit_risk,meta['step_size']))
            if qty<meta['min_qty'] or qty*entry<meta['min_notional']:raise ValueError('SIZE_BELOW_VENUE_MINIMUM')
            risk=qty*unit_risk;active=self.positions();existing=sum(p['risk_remaining'] for p in active)
            day=now//86400000*86400000
            used=self.db.execute('SELECT COALESCE(SUM(-MIN(net,0)),0) FROM outcomes WHERE closed_ms>=? AND closed_ms<?',(day,day+86400000)).fetchone()[0]
            # All coins share the conservative crypto cluster cap (no fictitious diversification).
            if len(active)>=c.max_positions or existing+risk>equity*c.total_risk_fraction or used+existing+risk>c.initial_equity*c.daily_loss_fraction:
                raise ValueError('ACCOUNT_RISK_BUDGET')
            if sum(p['entry_reference']*p['original_qty'] for p in active)+entry*qty>equity*c.max_notional_multiple:raise ValueError('ACCOUNT_NOTIONAL_CAP')
        except (ValueError,ZeroDivisionError) as exc:
            reason=str(exc)
            # Valid evidence can arrive after the direct-response price has become poor.
            # Do not loosen RR and do not chase: require a fresh defended retest instead.
            if reason in ('INSUFFICIENT_NET_REWARD','STOP_OBSOLETE','PRICE_MOVED_BEYOND_AUTHORIZATION') and s.get('attempt'):
                s['attempt']['entry_mode']='RETEST_ONLY';reason='WAIT_BETTER_ENTRY_RETEST'
            s['reason']=reason;self._decision(e,seq,'WAIT',reason,a);return
        f=features(s);oi_at=s['oi'][-1]['at'] if s['oi'] else None
        evidence=dict(a,entry=entry,stop=stop,tp1=target,tp2=tp2,quantity=qty,risk=risk,net_rr=rr(target),features=f,
                      oi_context=dict(available=bool(s['oi']),fresh=bool(oi_at is not None and now-oi_at<=c.max_oi_age_ms),last_at=oi_at))
        s['attempt']['stage']='CONSUMED'
        if c.mode=='shadow':self._decision(e,seq,'SIGNAL','SHADOW_NO_EXECUTION',evidence);return
        pid='P_'+digest([e.symbol,a['id']])[:32]
        if self.db.execute('SELECT 1 FROM positions WHERE id=?',(pid,)).fetchone():return
        p=dict(id=pid,symbol=e.symbol,status='PENDING',direction=direction,original_qty=qty,remaining=0.,
               entry_reference=entry,entry_price=None,stop=stop,management_stop=stop,tp1=target,tp2=tp2,partial_done=False,
               created_ms=now,entry_ms=None,closed_ms=None,initial_risk=risk,risk_remaining=risk,
               gross=0.,fees=0.,funding=0.,net=0.,attempt=a['id'],episode=a['episode'],evidence=evidence,
               entry_event=e.identity(),last_fill_seq=0,pending_reason=None,funding_ids=[])
        self._save_position(p);self._decision(e,seq,'INTENT','PAPER_AWAIT_NEXT_QUOTE',evidence)

    def _fill(self,p,e,seq,side,qty,price,reason):
        fid=digest([p['id'],e.identity(),reason]);fee=qty*price*self.cfg.fee_fraction
        self.db.execute('INSERT INTO executions VALUES(?,?,?,?)',(fid,p['id'],seq,dumps(dict(id=fid,side=side,qty=qty,price=price,fee=fee,reason=reason,at=e.decision_ms))))
        p['fees']+=fee;p['last_fill_seq']=seq
        if reason=='ENTRY':p.update(entry_price=price,remaining=qty,entry_ms=e.decision_ms,status='OPEN')
        else:
            p['gross']+=p['direction']*(price-p['entry_price'])*qty;p['remaining']=max(0.,p['remaining']-qty)
            p['risk_remaining']=p['initial_risk']*p['remaining']/p['original_qty']
        p['net']=p['gross']-p['fees']+p['funding']
        if reason!='ENTRY' and p['remaining']<1e-10:
            p.update(status='CLOSED',closed_ms=e.decision_ms,risk_remaining=0.,exit_reason=reason)
            outcome=dict(position_id=p['id'],status=reason,remaining_qty=0.,net=p['net'],net_r=p['net']/p['initial_risk'],initial_risk=p['initial_risk'],fees=p['fees'],funding=p['funding'],closed_ms=e.decision_ms,funding_complete=False)
            self.db.execute('INSERT INTO outcomes VALUES(?,?,?,?)',(p['id'],e.decision_ms,p['net'],dumps(outcome)))
            self._decision(e,seq,'EXIT',reason,outcome)

    def _manage(self,s,e,seq):
        for p in self.positions(active=e.kind not in ('FUNDING','FUNDING_SYNC')):
            if p['symbol']!=e.symbol:continue
            if e.kind=='FUNDING_SYNC':
                if p['status']=='CLOSED' and e.data['start']<=p['entry_ms'] and e.data['end']>=p['closed_ms']:
                    row=self.db.execute('SELECT payload FROM outcomes WHERE position_id=?',(p['id'],)).fetchone()
                    o=json.loads(row[0]);o['funding_complete']=True
                    self.db.execute('UPDATE outcomes SET payload=? WHERE position_id=?',(dumps(o),p['id']))
                continue
            if e.kind=='FUNDING':
                if p.get('entry_ms') is None:continue
                if p['entry_ms']<=e.at and (p['status']=='OPEN' or (p['closed_ms'] and e.at<=p['closed_ms'])) and e.key not in p['funding_ids']:
                    executions=[json.loads(r[0]) for r in self.db.execute('SELECT payload FROM executions WHERE position_id=?',(p['id'],))]
                    signed=sum(x['side']*x['qty'] for x in executions if x['at']<=e.at)
                    p['funding']-=signed*e.data['mark']*e.data['rate'];p['funding_ids'].append(e.key);p['net']=p['gross']-p['fees']+p['funding'];self._save_position(p)
                    if p['status']=='CLOSED':
                        row=self.db.execute('SELECT payload FROM outcomes WHERE position_id=?',(p['id'],)).fetchone();o=json.loads(row[0]);o.update(net=p['net'],net_r=p['net']/p['initial_risk'],funding=p['funding'])
                        self.db.execute('UPDATE outcomes SET net=?,payload=? WHERE position_id=?',(p['net'],dumps(o),p['id']))
                continue
            now=e.decision_ms;d=p['direction'];c=self.cfg
            if p['status']=='PENDING':
                if e.kind=='GAP' or now-p['created_ms']>5000 or (s['meta'] and s['meta']['status']!='TRADING'):
                    p.update(status='CANCELED',risk_remaining=0.);self._save_position(p);continue
                if e.kind!='QUOTE' or now-p['created_ms']<50:continue
                try:
                    price=self._price(s,d,p['original_qty'],now)
                    risk=d*(price-p['stop'])+price*(2*c.fee_fraction+c.slippage_fraction+c.funding_allowance_fraction)
                    reward=d*(p['tp1']-price)-(price+p['tp1'])*c.fee_fraction-price*(c.slippage_fraction+c.funding_allowance_fraction)
                    if risk<=0 or d*(price-p['stop'])<=0 or reward/risk<c.min_net_rr:raise ValueError('PRICE_MOVED_BEYOND_AUTHORIZATION')
                    qty=min(p['original_qty'],round_step(p['initial_risk']/risk,s['meta']['step_size']))
                    if qty<s['meta']['min_qty'] or qty*price<s['meta']['min_notional']:raise ValueError('PRICE_MOVED_BEYOND_AUTHORIZATION')
                    p['original_qty']=qty;p['initial_risk']=risk*qty;p['risk_remaining']=risk*qty
                except ValueError as exc:
                    p.update(status='CANCELED',risk_remaining=0.);self._decision(e,seq,'CANCEL',str(exc),dict(position_id=p['id']));self._save_position(p);continue
                self._fill(p,e,seq,d,p['original_qty'],price,'ENTRY');self._save_position(p);continue
            # Remember trigger during stale quotes; execute only on an observable valid book.
            price=None
            if e.kind=='TRADE' and p['entry_ms']<=e.at<=now and now-e.at<=c.max_exchange_lag_ms:
                price=e.data.get('price')
            elif self.quote_valid(s,now):
                price=s['quote']['bid'] if d==1 else s['quote']['ask']
            reason=p.get('pending_reason')
            if s['meta'] and s['meta']['status']!='TRADING':reason=reason or 'INVALIDATION_EXIT'
            active_stop=p.get('management_stop',p['stop'])
            if price and d*(price-active_stop)<=0:reason='BREAKEVEN_PROTECT' if p.get('partial_done') and active_stop!=p['stop'] else 'STOP_HIT'
            elif now-p['entry_ms']>=c.max_hold_ms:reason=reason or 'TIME_EXIT'
            elif price and p['tp2'] and d*(price-p['tp2'])>=0:reason=reason or 'TP2_HIT'
            elif price and not p['partial_done'] and d*(price-p['tp1'])>=0:reason=reason or 'TP1_HIT'
            if not reason:continue
            p['pending_reason']=reason
            qty=p['remaining']
            if reason=='TP1_HIT' and p['tp2']:
                qty=round_step(qty*c.partial_fraction,s['meta']['step_size'])
                if qty<s['meta']['min_qty'] or (p['remaining']-qty)<s['meta']['min_qty']:qty=p['remaining']
            try:fill=self._price(s,-d,qty,now)
            except ValueError:
                self._save_position(p);self._decision(e,seq,'WAIT','EXIT_AWAIT_VALID_LIQUIDITY',dict(position_id=p['id'],reason=reason));continue
            self._fill(p,e,seq,-d,qty,fill,reason)
            if reason=='TP1_HIT':
                p['partial_done']=True
                # After banking the structural first objective, protect the remainder
                # around cost-adjusted breakeven rather than leaving the full initial
                # structural risk on the table.
                cost_buffer=p['entry_price']*(2*c.fee_fraction+c.slippage_fraction)
                be=p['entry_price']+d*cost_buffer
                p['management_stop']=max(p['stop'],be) if d==1 else min(p['stop'],be)
            p['pending_reason']=None;self._save_position(p)

    def status(self):
        with self.lock:
            pending=self.db.execute('SELECT COUNT(*) FROM events WHERE applied=0').fetchone()[0]
            u=self.db.execute('SELECT payload FROM universe WHERE id=1').fetchone()
            return dict(mode=self.cfg.mode,release='6.1.3-live-data-paper',pending_events=pending,
                        processed_events=self.db.execute('SELECT COUNT(*) FROM events WHERE applied=1').fetchone()[0],
                        runtime_health=(lambda r:json.loads(r[0]) if r else {})(self.db.execute('SELECT payload FROM runtime_health WHERE id=1').fetchone()),
                        universe=json.loads(u[0]) if u else {},positions=self.positions(),
                        outcomes=[json.loads(r[0]) for r in self.db.execute('SELECT payload FROM outcomes ORDER BY closed_ms')],
                        decisions=[dict(r) for r in self.db.execute('SELECT seq,symbol,action,reason,payload FROM decisions ORDER BY seq DESC LIMIT 30')])
    def close(self):
        with self.lock:self.db.close()
