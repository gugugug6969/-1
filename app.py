"""
台股 AI 狙擊手 ULTRA v10.0
──────────────────────────────────────────────────────
核心升級：
  ① 均線多頭排列偵測   (MA5 > MA10 > MA20 > MA60)
  ② 爆量長紅濾網       (今日量 > N日均量 × 倍率 + 收紅)
  ③ 均線糾結偵測       (MA5/10/20 差距 < 3%，底部蓄力)
  ④ 投信連買偵測       (近3/5日投信估算持續買超)
  ⑤ 多空訊號評分系統   (技術/基本面/動能/財務/量能 五維)

pip install streamlit yfinance pandas numpy requests plotly apscheduler beautifulsoup4 lxml urllib3
streamlit run taiwan_stock_ultra_v10.py
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import urllib3, datetime, time, re, concurrent.futures
from typing import Optional, Dict, List, Tuple, Any
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
st.set_page_config(page_title="台股狙擊手 v10", page_icon="⚡", layout="wide", initial_sidebar_state="expanded")

# ══════════════════════════════════════════════════════════════════
#  CSS  — 完全獨立字串，不用 f-string，不會被 Python 解析
# ══════════════════════════════════════════════════════════════════
CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@400;500;700;900&family=JetBrains+Mono:wght@300;400;600;700&display=swap');

:root{
  --bg0:#02060f;--bg1:#050d1a;--bg2:#081425;--bg3:#0c1b30;
  --ln:#112038;--ln2:#192d4a;--ln3:#253f68;
  --t0:#e6f2ff;--t1:#7aaac8;--t2:#375872;--t3:#162840;
  --g:#00e87a;--g2:#00b860;--g3:rgba(0,232,122,.09);
  --r:#ff2d55;--r2:#cc1a3a;--r3:rgba(255,45,85,.09);
  --y:#ffd60a;--y2:#cc9a00;--y3:rgba(255,214,10,.09);
  --b:#1e90ff;--b2:#0060cc;--b3:rgba(30,144,255,.09);
  --p:#bf5af2;--p3:rgba(191,90,242,.09);
  --c:#32d2f5;--c3:rgba(50,210,245,.09);
  --o:#ff7b00;--o3:rgba(255,123,0,.09);
  --mono:'JetBrains Mono',monospace;
  --tc:'Noto Sans TC','Microsoft JhengHei','PingFang TC',sans-serif;
  --sg:0 0 18px rgba(0,232,122,.28);
  --sr:0 0 18px rgba(255,45,85,.28);
  --sb:0 0 18px rgba(30,144,255,.28);
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body,[class*="css"]{font-family:var(--tc)!important;background:var(--bg0);color:var(--t0)}
#MainMenu,footer{visibility:hidden}
header{background:transparent !important;}

.stApp{background:var(--bg0)}
.main .block-container{padding:.4rem .9rem 3rem;max-width:100%}

/* 格線背景 */
.stApp::after{content:'';position:fixed;inset:0;pointer-events:none;z-index:0;
  background-image:linear-gradient(rgba(0,232,122,.004) 1px,transparent 1px),
                   linear-gradient(90deg,rgba(0,232,122,.004) 1px,transparent 1px);
  background-size:44px 44px}

/* ─── HEADER ─── */
.hdr{background:linear-gradient(180deg,#071422 0%,#030c18 100%);
  border-bottom:1px solid var(--ln2);border-top:3px solid var(--g);
  margin-bottom:10px;position:relative;overflow:hidden}
.hdr-glow{position:absolute;top:-70px;left:50%;transform:translateX(-50%);
  width:900px;height:140px;border-radius:50%;
  background:radial-gradient(ellipse,rgba(0,232,122,.06) 0%,transparent 70%);
  animation:glow 5s ease-in-out infinite;pointer-events:none}
@keyframes glow{0%,100%{opacity:.5}50%{opacity:1}}
.hdr-inner{display:flex;align-items:stretch;position:relative;z-index:1}
.hdr-bar{width:4px;background:linear-gradient(180deg,var(--g),var(--b),var(--p));flex-shrink:0}
.hdr-body{flex:1;padding:13px 18px;display:flex;align-items:center;gap:14px}
.hdr-ico{font-size:2.1rem;flex-shrink:0;line-height:1}
.hdr-txt{flex:1}
.hdr-eyebrow{font-family:var(--mono);font-size:.46rem;color:var(--t2);
  letter-spacing:.26em;text-transform:uppercase;margin-bottom:3px}
.hdr-title{font-weight:900;font-size:1.65rem;letter-spacing:-.03em;line-height:1;
  background:linear-gradient(100deg,#e6f2ff 0%,#00e87a 42%,#1e90ff 78%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
.hdr-sub{font-family:var(--mono);font-size:.46rem;color:var(--t2);
  letter-spacing:.1em;text-transform:uppercase;margin-top:3px}
.hdr-chips{display:flex;gap:5px;margin-top:6px;flex-wrap:wrap}
.chip{display:inline-flex;align-items:center;gap:3px;font-family:var(--mono);
  font-size:.46rem;font-weight:600;letter-spacing:.07em;text-transform:uppercase;
  padding:3px 7px;border-radius:3px;border:1px solid}
.chip-live{color:var(--g);border-color:rgba(0,232,122,.3);background:var(--g3)}
.chip-dot{width:5px;height:5px;border-radius:50%;background:var(--g);animation:blink 1.2s step-end infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0}}
.chip-time{color:var(--t2);border-color:var(--ln);background:transparent}
.chip-v10{color:var(--p);border-color:rgba(191,90,242,.3);background:var(--p3)}
.chip-on{color:var(--g);border-color:rgba(0,232,122,.3);background:var(--g3)}
.chip-off{color:var(--t2);border-color:var(--ln);background:transparent}
.chip-warn{color:var(--y);border-color:rgba(255,214,10,.3);background:var(--y3);animation:blink 2s step-end infinite}
.hdr-stats{display:flex;align-self:stretch;border-left:1px solid var(--ln2)}
.hdr-stat{display:flex;flex-direction:column;justify-content:center;align-items:center;
  padding:0 20px;border-right:1px solid var(--ln2);min-width:76px}
.hdr-stat-n{font-family:var(--mono);font-size:1.1rem;font-weight:700;line-height:1}
.hdr-stat-l{font-family:var(--mono);font-size:.42rem;color:var(--t2);
  text-transform:uppercase;letter-spacing:.14em;margin-top:3px}
.hdr-tape{border-top:1px solid var(--ln);padding:5px 18px;
  font-family:var(--mono);font-size:.48rem;color:var(--t2);
  display:flex;gap:16px;align-items:center;overflow:hidden}
.ti{display:flex;gap:5px;align-items:center}
.ti-code{color:var(--t1);font-weight:700}

/* ─── ALERT BANNER ─── */
.alt-bar{background:var(--bg1);border:1px solid rgba(255,214,10,.25);
  border-left:3px solid var(--y);border-radius:6px;padding:7px 13px;
  margin-bottom:8px;display:flex;align-items:center;gap:10px;
  font-family:var(--mono);font-size:.56rem}
.alt-items{flex:1;display:flex;gap:14px;flex-wrap:wrap}

/* ─── SIDEBAR ─── */
section[data-testid="stSidebar"]{background:var(--bg1)!important;
  border-right:1px solid var(--ln)!important;min-width:268px!important}
section[data-testid="stSidebar"]>div{padding:0!important}
.sb-hdr{display:flex;align-items:center;gap:7px;padding:9px 12px;
  font-family:var(--mono);font-size:.46rem;font-weight:700;color:var(--t2);
  text-transform:uppercase;letter-spacing:.18em;background:var(--bg2);
  border-bottom:1px solid var(--ln)}
.sb-dot{width:6px;height:2px;background:var(--g);border-radius:1px;flex-shrink:0}
.sb-body{padding:9px 12px;border-bottom:1px solid var(--ln)}

/* search rows */
.sh-row{display:flex;align-items:center;gap:6px;padding:5px 7px;border-radius:4px;
  margin-bottom:3px;background:var(--bg2);border:1px solid var(--ln);transition:border-color .15s}
.sh-row:hover{border-color:var(--ln3)}
.sh-code{font-family:var(--mono);font-size:.72rem;font-weight:700;color:var(--g);min-width:36px}
.sh-name{font-size:.6rem;color:var(--t1);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
/* watchlist */
.wl-row{display:flex;align-items:center;gap:5px;padding:4px 7px;border-radius:4px;
  margin-bottom:2px;background:var(--bg2);border:1px solid var(--ln)}
.wl-code{font-family:var(--mono);font-size:.66rem;font-weight:700;color:var(--g);width:35px;flex-shrink:0}
.wl-name{font-size:.57rem;color:var(--t1);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.wl-px{font-family:var(--mono);font-size:.58rem;color:var(--t0);font-weight:600}
.wl-up{font-family:var(--mono);font-size:.52rem;color:var(--g)}
.wl-dn{font-family:var(--mono);font-size:.52rem;color:var(--r)}

/* ─── FIRE SIGNAL BADGE ─── */
.fire-row{display:flex;gap:4px;flex-wrap:wrap;margin-top:6px}
.fbadge{display:inline-flex;align-items:center;gap:3px;font-family:var(--mono);
  font-size:.48rem;font-weight:700;padding:3px 8px;border-radius:3px;border:1px solid}
.fb-ma{color:var(--g);border-color:rgba(0,232,122,.35);background:var(--g3)}
.fb-vol{color:var(--o);border-color:rgba(255,123,0,.35);background:var(--o3)}
.fb-tangle{color:var(--b);border-color:rgba(30,144,255,.35);background:var(--b3)}
.fb-inst{color:var(--p);border-color:rgba(191,90,242,.35);background:var(--p3)}
.fb-rsi{color:var(--c);border-color:rgba(50,210,245,.35);background:var(--c3)}
.fb-macd{color:var(--y);border-color:rgba(255,214,10,.35);background:var(--y3)}

/* ─── KPI STRIP ─── */
.kpi-row{display:grid;grid-template-columns:repeat(8,1fr);gap:5px;margin-bottom:9px}
.kpi{position:relative;overflow:hidden;background:var(--bg1);border:1px solid var(--ln);
  border-radius:5px;padding:9px 11px;transition:transform .15s,border-color .15s}
.kpi:hover{transform:translateY(-1px);border-color:var(--ln3)}
.kpi::after{content:'';position:absolute;bottom:0;left:0;right:0;height:2px}
.kpi.g::after{background:var(--g)}.kpi.r::after{background:var(--r)}
.kpi.y::after{background:var(--y)}.kpi.b::after{background:var(--b)}
.kpi.p::after{background:var(--p)}.kpi.c::after{background:var(--c)}
.kpi.o::after{background:var(--o)}.kpi.w::after{background:var(--t1)}
.kpi-l{font-family:var(--mono);font-size:.42rem;color:var(--t2);
  text-transform:uppercase;letter-spacing:.11em;margin-bottom:4px}
.kpi-v{font-family:var(--mono);font-size:1.15rem;font-weight:700;line-height:1}
.kpi-v.g{color:var(--g)}.kpi-v.r{color:var(--r)}.kpi-v.y{color:var(--y)}
.kpi-v.b{color:var(--b)}.kpi-v.p{color:var(--p)}.kpi-v.c{color:var(--c)}.kpi-v.o{color:var(--o)}
.kpi-d{font-family:var(--mono);font-size:.42rem;color:var(--t2);margin-top:2px}

/* ─── STOCK HEADER CARD ─── */
.scard{background:var(--bg1);border:1px solid var(--ln2);border-radius:8px;
  margin-bottom:10px;overflow:hidden}
.scard-top{display:flex;align-items:stretch;border-bottom:1px solid var(--ln)}
.scard-stripe{width:4px;background:linear-gradient(180deg,var(--g),var(--b),var(--p));flex-shrink:0}
.scard-id{padding:13px 15px;border-right:1px solid var(--ln);min-width:165px}
.scard-code{font-family:var(--mono);font-size:1.45rem;font-weight:700;line-height:1}
.scard-sfx{font-size:.56rem;color:var(--t2);margin-left:4px}
.scard-name{font-size:.84rem;font-weight:700;color:var(--t1);margin-top:4px}
.scard-ind{font-family:var(--mono);font-size:.46rem;color:var(--t2);margin-top:2px}
.scard-px{flex:1;padding:13px 17px}
.scard-price{font-family:var(--mono);font-size:2.2rem;font-weight:700;line-height:1}
.scard-unit{font-size:.62rem;color:var(--t2);margin-left:5px}
.scard-chg{font-family:var(--mono);font-size:.8rem;font-weight:600;margin-top:3px}
.scard-chg.pos{color:var(--g)}.scard-chg.neg{color:var(--r)}
.scard-ohlc{display:flex;gap:13px;margin-top:8px;font-family:var(--mono);font-size:.5rem;color:var(--t2)}
.scard-meta{display:flex;gap:13px;margin-top:4px;font-family:var(--mono);font-size:.5rem;color:var(--t2)}
.scard-badges{display:flex;gap:4px;flex-wrap:wrap;margin-top:7px;align-items:center}
.scard-sig-block{padding:13px 15px;border-left:1px solid var(--ln);min-width:162px;
  display:flex;flex-direction:column;justify-content:center;gap:7px}
.score-ring{width:52px;height:52px;border-radius:50%;border:2px solid;
  display:flex;flex-direction:column;align-items:center;justify-content:center;flex-shrink:0}
.score-ring.hi{border-color:var(--g);background:var(--g3);box-shadow:var(--sg)}
.score-ring.md{border-color:var(--y);background:var(--y3)}
.score-ring.lo{border-color:var(--r);background:var(--r3);box-shadow:var(--sr)}
.score-n{font-family:var(--mono);font-size:.88rem;font-weight:700;line-height:1}
.score-lbl{font-family:var(--mono);font-size:.36rem;color:var(--t2);text-transform:uppercase;margin-top:1px}

/* signal badge */
.sig{display:inline-flex;align-items:center;gap:4px;font-family:var(--mono);
  font-size:.56rem;font-weight:700;letter-spacing:.06em;
  padding:3px 9px;border-radius:3px;border:1px solid}
.sig-BUY{color:var(--g);border-color:rgba(0,232,122,.4);background:var(--g3)}
.sig-WATCH{color:var(--y);border-color:rgba(255,214,10,.4);background:var(--y3)}
.sig-HOLD{color:var(--t1);border-color:var(--ln2);background:var(--bg2)}
.sig-AVOID{color:var(--r);border-color:rgba(255,45,85,.4);background:var(--r3)}

/* vol/fh badge */
.xbadge{display:inline-flex;align-items:center;gap:3px;font-family:var(--mono);
  font-size:.48rem;font-weight:700;padding:2px 7px;border-radius:3px;border:1px solid}
.xb-vol-x{color:var(--r);border-color:rgba(255,45,85,.4);background:var(--r3);animation:blink .9s step-end infinite}
.xb-vol-h{color:var(--y);border-color:rgba(255,214,10,.4);background:var(--y3)}
.xb-vol-n{color:var(--t2);border-color:var(--ln);background:transparent}
.xb-fhA{color:var(--g);border-color:rgba(0,232,122,.35);background:var(--g3)}
.xb-fhB{color:var(--b);border-color:rgba(30,144,255,.35);background:var(--b3)}
.xb-fhC{color:var(--y);border-color:rgba(255,214,10,.35);background:var(--y3)}
.xb-fhD{color:var(--r);border-color:rgba(255,45,85,.35);background:var(--r3)}

/* ─── DATA GRIDS ─── */
.dg6{display:grid;grid-template-columns:repeat(6,1fr);gap:4px;margin:8px 0}
.dg8{display:grid;grid-template-columns:repeat(8,1fr);gap:4px;margin:8px 0}
.dg4{display:grid;grid-template-columns:repeat(4,1fr);gap:4px;margin:8px 0}
.dc{background:var(--bg2);border:1px solid var(--ln);border-radius:4px;
  padding:7px 9px;transition:border-color .15s,background .15s}
.dc:hover{border-color:var(--ln3);background:var(--bg3)}
.dc-k{font-family:var(--mono);font-size:.4rem;color:var(--t2);
  text-transform:uppercase;letter-spacing:.1em;margin-bottom:3px}
.dc-v{font-family:var(--mono);font-size:.76rem;font-weight:700;color:var(--t0)}
.dc-v.pos{color:var(--g)}.dc-v.neg{color:var(--r)}.dc-v.warn{color:var(--y)}.dc-v.neu{color:var(--b)}

/* ─── PANEL ─── */
.panel{background:var(--bg2);border:1px solid var(--ln2);border-radius:6px;
  padding:11px 13px;margin-bottom:8px}
.panel-title{font-family:var(--mono);font-size:.44rem;color:var(--t2);
  text-transform:uppercase;letter-spacing:.14em;margin-bottom:9px;
  display:flex;align-items:center;gap:6px}
.panel-title::before{content:'';width:8px;height:2px;background:var(--g);border-radius:1px}

/* FIRE panel — 點火訊號 */
.fire-panel{background:linear-gradient(135deg,rgba(0,232,122,.04) 0%,var(--bg2) 60%);
  border:1px solid rgba(0,232,122,.2);border-left:3px solid var(--g);
  border-radius:6px;padding:11px 13px;margin-bottom:8px}
.fire-panel-title{font-family:var(--mono);font-size:.48rem;font-weight:700;
  color:var(--g);letter-spacing:.14em;text-transform:uppercase;
  margin-bottom:8px;display:flex;align-items:center;gap:7px}
.fire-score{font-family:var(--mono);font-size:1.6rem;font-weight:700;
  line-height:1;color:var(--g);text-shadow:var(--sg)}
.fire-items{display:grid;grid-template-columns:repeat(2,1fr);gap:4px;margin-top:8px}
.fi{display:flex;align-items:center;gap:7px;padding:5px 8px;border-radius:4px;
  background:var(--bg3);border:1px solid var(--ln)}
.fi.ok{border-color:rgba(0,232,122,.25);background:rgba(0,232,122,.04)}
.fi.no{opacity:.5}
.fi-ic{font-size:.7rem;flex-shrink:0;width:16px;text-align:center}
.fi-txt{font-size:.58rem;color:var(--t1)}
.fi-sub{font-family:var(--mono);font-size:.45rem;color:var(--t2)}

/* score bars */
.sbar{display:flex;align-items:center;gap:8px;margin:5px 0}
.sbar-k{font-size:.52rem;color:var(--t1);width:54px;flex-shrink:0}
.sbar-track{flex:1;height:4px;background:var(--ln);border-radius:2px;overflow:hidden}
.sbar-fill{height:100%;border-radius:2px}
.sbar-fill.g{background:linear-gradient(90deg,var(--g2),var(--g))}
.sbar-fill.y{background:linear-gradient(90deg,var(--y2),var(--y))}
.sbar-fill.r{background:linear-gradient(90deg,var(--r2),var(--r))}
.sbar-fill.b{background:linear-gradient(90deg,var(--b2),var(--b))}
.sbar-n{font-family:var(--mono);font-size:.5rem;color:var(--t1);width:34px;text-align:right}

/* checklist */
.chk{display:flex;align-items:center;gap:6px;padding:4px 0;border-bottom:1px solid var(--ln)}
.chk:last-child{border:none}
.chk-ic{font-family:var(--mono);font-size:.56rem;font-weight:700;width:14px;flex-shrink:0}
.chk-txt{font-size:.55rem}
.chk.ok .chk-ic{color:var(--g)}.chk.ok .chk-txt{color:var(--t1)}
.chk.no .chk-ic{color:var(--t3)}.chk.no .chk-txt{color:var(--t2)}

/* target price */
.tp-panel{background:var(--bg2);border:1px solid var(--ln2);border-radius:6px;
  padding:13px;margin-bottom:8px;position:relative;overflow:hidden}
.tp-panel::before{content:'';position:absolute;top:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,var(--g),transparent)}
.tp-row{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:9px}
.tp-lbl{font-family:var(--mono);font-size:.4rem;color:var(--t2);text-transform:uppercase;letter-spacing:.1em;margin-bottom:3px}
.tp-val{font-family:var(--mono);font-size:.85rem;font-weight:700}
.tp-val.cur{color:var(--t0)}.tp-val.tp{color:var(--g);font-size:1rem}
.tp-val.lo{color:var(--b)}.tp-val.hi{color:var(--p)}
.tp-big{font-family:var(--mono);font-size:1.5rem;font-weight:700;line-height:1}
.tp-big.pos{color:var(--g);text-shadow:var(--sg)}.tp-big.neg{color:var(--r)}
.tp-track{position:relative;height:6px;background:var(--bg3);border-radius:3px;margin:9px 0 17px}
.tp-zone{position:absolute;height:100%;border-radius:3px;
  background:rgba(30,144,255,.18);border:1px solid rgba(30,144,255,.35)}
.tp-cur{position:absolute;top:-4px;width:2px;height:14px;background:#fff;
  border-radius:1px;transform:translateX(-50%);box-shadow:0 0 8px rgba(255,255,255,.8)}
.tp-tp{position:absolute;top:-4px;width:2px;height:14px;border-radius:1px;transform:translateX(-50%)}
.tp-lbl2{position:absolute;font-family:var(--mono);font-size:.4rem;white-space:nowrap;transform:translateX(-50%)}

/* sig card */
.sig-card{border-radius:6px;padding:11px;margin-bottom:8px;border:1px solid;border-left:3px solid}
.sig-card.BUY{border-color:rgba(0,232,122,.18);border-left-color:var(--g);background:rgba(0,232,122,.025)}
.sig-card.WATCH{border-color:rgba(255,214,10,.18);border-left-color:var(--y);background:rgba(255,214,10,.025)}
.sig-card.HOLD{border-color:var(--ln2);border-left-color:var(--t2);background:var(--bg2)}
.sig-card.AVOID{border-color:rgba(255,45,85,.18);border-left-color:var(--r);background:rgba(255,45,85,.025)}
.sig-card-t{font-family:var(--mono);font-size:.66rem;font-weight:700;margin-bottom:4px}
.sig-card-b{font-size:.66rem;color:var(--t1);line-height:1.65}

/* pivot */
.pv-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:4px}
.pv-cell{background:var(--bg3);border-radius:4px;padding:6px;text-align:center}
.pv-k{font-family:var(--mono);font-size:.38rem;color:var(--t2);text-transform:uppercase;letter-spacing:.1em;margin-bottom:3px}
.pv-v{font-family:var(--mono);font-size:.72rem;font-weight:700}
.pv-cell.R .pv-v{color:var(--r)}.pv-cell.P .pv-v{color:var(--y)}.pv-cell.S .pv-v{color:var(--g)}

/* institution */
.inst-row{display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid var(--ln)}
.inst-row:last-child{border:none}
.inst-name{font-size:.52rem;color:var(--t1);width:50px;flex-shrink:0}
.inst-wrap{flex:1;height:5px;background:var(--ln);border-radius:2px;position:relative;overflow:visible}
.inst-b{position:absolute;top:0;left:50%;height:100%;border-radius:0 2px 2px 0}
.inst-s{position:absolute;top:0;right:50%;height:100%;border-radius:2px 0 0 2px}
.inst-val{font-family:var(--mono);font-size:.56rem;font-weight:700;width:52px;text-align:right}

/* risk */
.risk-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:5px}
.risk-cell{background:var(--bg3);border-radius:4px;padding:7px}
.risk-k{font-family:var(--mono);font-size:.38rem;color:var(--t2);text-transform:uppercase;letter-spacing:.1em;margin-bottom:3px}
.risk-v{font-family:var(--mono);font-size:.78rem;font-weight:700}
.risk-bar{height:3px;background:var(--ln);border-radius:2px;margin-top:5px;overflow:hidden}
.risk-bar-fill{height:100%;border-radius:2px}

/* backtest */
.bt-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:5px}
.bt-cell{background:var(--bg3);border-radius:4px;padding:6px;text-align:center}
.bt-k{font-family:var(--mono);font-size:.38rem;color:var(--t2);text-transform:uppercase;letter-spacing:.1em;margin-bottom:2px}
.bt-v{font-family:var(--mono);font-size:.75rem;font-weight:700}

/* fh ring */
.fh-wrap{display:flex;gap:11px;align-items:center}
.fh-ring{width:60px;height:60px;border-radius:50%;border:3px solid;
  display:flex;flex-direction:column;align-items:center;justify-content:center;flex-shrink:0}
.fh-ring.A{border-color:var(--g);background:var(--g3)}
.fh-ring.B{border-color:var(--b);background:var(--b3)}
.fh-ring.C{border-color:var(--y);background:var(--y3)}
.fh-ring.D{border-color:var(--r);background:var(--r3)}
.fh-grade{font-family:var(--mono);font-size:1.2rem;font-weight:700;line-height:1}
.fh-score{font-family:var(--mono);font-size:.36rem;color:var(--t2);text-transform:uppercase;margin-top:1px}
.fh-details{flex:1;display:grid;grid-template-columns:repeat(3,1fr);gap:5px}
.fh-dc{background:var(--bg3);border-radius:3px;padding:6px 7px}
.fh-dk{font-family:var(--mono);font-size:.36rem;color:var(--t2);text-transform:uppercase;margin-bottom:2px}
.fh-dv{font-family:var(--mono);font-size:.68rem;font-weight:700}

/* bull/bear */
.bb-panel{background:var(--bg2);border:1px solid var(--ln2);border-radius:6px;padding:11px 13px;margin-bottom:8px}
.bb-gauge{height:9px;border-radius:5px;background:var(--ln);overflow:hidden;position:relative;margin:5px 0}
.bb-fill{position:absolute;top:0;left:0;height:100%;border-radius:5px;
  background:linear-gradient(90deg,var(--g2),var(--g))}

/* news */
.news-item{display:flex;gap:8px;align-items:flex-start;padding:6px 0;border-bottom:1px solid var(--bg3)}
.news-item:last-child{border:none}
.news-ic{width:19px;height:19px;border-radius:3px;display:flex;align-items:center;justify-content:center;
  font-size:.55rem;font-weight:700;flex-shrink:0}
.news-ic.pos{background:var(--g3);color:var(--g)}.news-ic.neg{background:var(--r3);color:var(--r)}.news-ic.neu{background:var(--bg3);color:var(--t2)}
.news-t{font-size:.7rem;color:var(--t1);margin-bottom:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.news-m{font-family:var(--mono);font-size:.47rem;color:var(--t2)}

/* result table */
.rt-wrap{overflow-x:auto;border:1px solid var(--ln2);border-radius:7px}
.rt{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:.64rem}
.rt th{background:var(--bg2);color:var(--t2);text-transform:uppercase;letter-spacing:.07em;
  font-size:.44rem;font-weight:700;padding:8px 9px;text-align:left;
  border-bottom:1px solid var(--ln2);white-space:nowrap;position:sticky;top:0;z-index:5}
.rt td{padding:7px 9px;border-bottom:1px solid var(--bg2);vertical-align:middle;white-space:nowrap}
.rt tr:last-child td{border:none}
.rt tr:hover td{background:var(--bg2)}
.rt tr.BUY td:first-child{border-left:3px solid var(--g)}
.rt tr.WATCH td:first-child{border-left:3px solid var(--y)}
.rt tr.AVOID td:first-child{border-left:3px solid var(--r)}
.rt tr.HOLD td:first-child{border-left:3px solid var(--ln2)}
.rt .c-pri{color:var(--t0);font-weight:700}.rt .c-tp{color:var(--b);font-weight:600}
.rt .c-up{color:var(--g);font-weight:700}.rt .c-dn{color:var(--r);font-weight:700}
.rt .c-dim{color:var(--t2)}.rt .c-pos{color:var(--g)}.rt .c-neg{color:var(--r)}.rt .c-warn{color:var(--y)}
.rt .c-neu{color:var(--b)}.rt .c-pur{color:var(--p)}.rt .c-ora{color:var(--o)}

/* fire column in table */
.fire-score-sm{display:inline-flex;align-items:center;justify-content:center;
  width:22px;height:22px;border-radius:4px;border:1px solid;
  font-family:var(--mono);font-size:.54rem;font-weight:700}
.fs-hi{color:var(--g);border-color:rgba(0,232,122,.4);background:var(--g3)}
.fs-md{color:var(--y);border-color:rgba(255,214,10,.4);background:var(--y3)}
.fs-lo{color:var(--t2);border-color:var(--ln);background:transparent}

/* rank card */
.rank-card{background:var(--bg1);border:1px solid;border-left:3px solid;
  border-radius:5px;padding:8px 12px;margin-bottom:5px;
  display:flex;justify-content:space-between;align-items:center}

/* tabs */
.stTabs [data-baseweb="tab-list"]{background:var(--bg1)!important;
  border-bottom:1px solid var(--ln)!important;gap:0!important;padding:0!important}
.stTabs [data-baseweb="tab"]{font-family:var(--mono)!important;font-size:.56rem!important;
  font-weight:700!important;letter-spacing:.09em!important;color:var(--t2)!important;
  text-transform:uppercase!important;border-radius:0!important;
  padding:10px 17px!important;border-bottom:2px solid transparent!important;transition:color .15s!important}
.stTabs [aria-selected="true"]{color:var(--g)!important;
  border-bottom-color:var(--g)!important;background:rgba(0,232,122,.03)!important}

/* buttons */
.stButton>button{font-family:var(--mono)!important;font-size:.6rem!important;
  font-weight:700!important;letter-spacing:.06em!important;border-radius:4px!important;
  text-transform:uppercase!important;transition:all .15s!important}
.stButton>button[kind="primary"]{background:var(--g)!important;color:#02060f!important;border:none!important}
.stButton>button[kind="primary"]:hover{box-shadow:var(--sg)!important;transform:translateY(-1px)!important}
.stButton>button:not([kind="primary"]){background:var(--bg2)!important;
  color:var(--t1)!important;border:1px solid var(--ln2)!important}
.stButton>button:not([kind="primary"]):hover{border-color:var(--ln3)!important;color:var(--t0)!important}

/* inputs */
.stTextInput>div>div>input,.stTextArea>div>div>textarea{
  background:var(--bg2)!important;border:1px solid var(--ln2)!important;
  color:var(--t0)!important;border-radius:4px!important;
  font-family:var(--mono)!important;font-size:.68rem!important}
.stTextInput>div>div>input:focus,.stTextArea>div>div>textarea:focus{
  border-color:var(--g)!important;box-shadow:0 0 0 1px rgba(0,232,122,.12)!important}
.stSlider>div>div>div>div{background:var(--g)!important}
.stProgress>div>div>div{background:var(--g)!important}
.stSelectbox>div>div{background:var(--bg2)!important;border:1px solid var(--ln2)!important;
  color:var(--t0)!important;border-radius:4px!important}
.stRadio>div{gap:4px!important}
label[data-baseweb="radio"]>div:first-child{background:var(--bg2)!important;border-color:var(--ln2)!important}
label[data-baseweb="radio"][aria-checked="true"]>div:first-child{background:var(--g)!important;border-color:var(--g)!important}
.streamlit-expanderHeader{background:var(--bg1)!important;border:1px solid var(--ln)!important;
  border-radius:5px!important;font-family:var(--mono)!important;font-size:.58rem!important;
  font-weight:700!important;color:var(--t1)!important;letter-spacing:.05em!important}
.stCheckbox label{font-family:var(--mono)!important;font-size:.58rem!important;color:var(--t1)!important}
.stCheckbox>label>div:first-child{background:var(--bg2)!important;border-color:var(--ln2)!important;border-radius:3px!important}
.stCheckbox>label>div:first-child[aria-checked="true"]{background:var(--g)!important;border-color:var(--g)!important}
hr{border-color:var(--ln)!important;margin:7px 0!important}

/* log */
.logbox{background:var(--bg1);border:1px solid var(--ln);border-radius:6px;padding:11px 14px;font-family:var(--mono)}
.ll{font-size:.58rem;padding:2px 0;line-height:1.5}
.ll.ok{color:var(--g)}.ll.err{color:var(--r)}.ll.inf{color:var(--b)}.ll.dim{color:var(--t2)}

/* empty */
.empty{text-align:center;padding:60px 0}
.empty-ico{font-size:2.5rem;margin-bottom:12px}
.empty-txt{font-family:var(--mono);font-size:.68rem;color:var(--t2);line-height:1.9}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
#  SESSION STATE
# ══════════════════════════════════════════════════════════════════
_DEFAULTS = dict(
    scan_results=[], scheduler=None, sched_running=False,
    sched_log=[], last_scan_time=None, auto_webhook="",
    scan_params={}, scan_codes=[], selected_stock=None,
    detail_cache={}, watchlist=[], alerts=[], alert_cfg={},
)
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ══════════════════════════════════════════════════════════════════
#  股票名稱 / 產業字典
# ══════════════════════════════════════════════════════════════════
_BUILTIN: Dict[str, str] = {
    "2330":"台積電","2317":"鴻海","2454":"聯發科","2308":"台達電","2382":"廣達",
    "2357":"華碩","2412":"中華電","3008":"大立光","2395":"研華","2303":"聯電",
    "2881":"富邦金","2882":"國泰金","2886":"兆豐金","2884":"玉山金","2885":"元大金",
    "2891":"中信金","2883":"開發金","2887":"台新金","2890":"永豐金","2892":"第一金",
    "1301":"台塑","1303":"南亞","1326":"台化","6505":"台塑化","2002":"中鋼",
    "2207":"和泰車","2912":"統一超","5871":"中租-KY","3711":"日月光投控",
    "2301":"光寶科","2354":"鴻準","2324":"仁寶","2327":"國巨",
    "3045":"台灣大","4904":"遠傳","2409":"友達","2408":"南亞科","2376":"技嘉",
    "2379":"瑞昱","6415":"矽力-KY","3034":"聯詠","3037":"欣興","2344":"華邦電",
    "2498":"宏達電","6669":"緯穎","2823":"中壽","2615":"萬海","2603":"長榮",
    "2609":"陽明","2610":"華航","2618":"長榮航","5876":"上海商銀","8046":"南電",
    "3481":"群創","2356":"英業達","2337":"旺宏","2449":"京元電子","3231":"緯創",
    "2352":"佳世達","5274":"信驊","4938":"和碩","2474":"可成","2360":"致茂",
    "3443":"創意","2385":"群光","6285":"啟碁","4919":"新唐","2059":"川湖",
    "2049":"上銀","1590":"亞德客-KY","2105":"正新","2201":"裕隆","2204":"中華汽車",
    "1216":"統一","1102":"亞泥","1101":"台泥","2542":"興富發","5880":"合庫金",
    "2634":"漢翔","6770":"力積電","3529":"力旺","3661":"世芯-KY","6510":"精測",
    "8299":"群聯","3532":"台勝科","6472":"保瑞","3035":"智原","4966":"譜瑞-KY",
    "6278":"台表科","3260":"威剛","4958":"臻鼎-KY","3006":"晶豪科","2404":"漢唐",
    "3714":"富采","6488":"環球晶","2233":"宏致","2206":"三陽工業","9910":"豐泰",
    "6116":"彩晶","2368":"金像電","2383":"台光電","3227":"原相","4961":"天鈺",
    "6269":"台郡","2227":"裕日車","5234":"達興材料","2347":"聯強",
    "2014":"中鴻","6582":"申豐","4743":"合一","3688":"億觀","6669":"緯穎",
    "3019":"亞泰","2455":"全新","4736":"泰博","6547":"高端疫苗","1722":"台肥",
}
_INDUSTRY: Dict[str, str] = {
    "2330":"半導體","2454":"半導體","2303":"半導體","6415":"半導體","3034":"半導體",
    "3037":"半導體","2344":"半導體","6770":"半導體","3443":"半導體","3529":"半導體",
    "6488":"半導體","4966":"半導體","3006":"半導體","2455":"半導體",
    "2317":"電子製造","2382":"電子製造","2357":"電腦設備","2395":"電腦設備","2379":"IC設計",
    "6285":"網通","2376":"電腦設備","2301":"電子零件","2356":"電子製造","3231":"電子製造",
    "4938":"電子製造","2385":"電子零件","6669":"伺服器","2327":"被動元件","2474":"機殼",
    "5274":"IC設計","3227":"IC設計","4961":"IC設計","3035":"IC設計",
    "2881":"金融","2882":"金融","2886":"金融","2884":"金融","2885":"金融",
    "2891":"金融","2883":"金融","2887":"金融","2890":"金融","2892":"金融",
    "5871":"租賃","5876":"金融","5880":"金融","2823":"保險",
    "1301":"塑化","1303":"塑化","1326":"塑化","6505":"塑化",
    "2002":"鋼鐵","2014":"鋼鐵",
    "2603":"航運","2609":"航運","2615":"航運",
    "2412":"電信","3045":"電信","4904":"電信",
    "3008":"光學","2059":"工業電腦","2308":"電子零件",
    "2207":"汽車","2201":"汽車","2204":"汽車","2206":"汽車","2105":"輪胎",
    "1216":"食品","2912":"零售","1101":"水泥","1102":"水泥",
    "2409":"面板","3481":"面板","6116":"面板",
    "2610":"航空","2618":"航空","2634":"航太",
    "2360":"測試設備","6510":"測試設備","2449":"封測","3711":"封測",
    "8046":"PCB","3037":"PCB","4958":"PCB","6278":"PCB","2368":"PCB",
}

@st.cache_data(ttl=3600, show_spinner=False)
def load_names() -> Dict[str, str]:
    names = dict(_BUILTIN)
    hdr = {"User-Agent": "Mozilla/5.0"}
    for url, ck, nk in [
        ("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", "Code", "Name"),
        ("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", "SecuritiesCompanyCode", "CompanyName"),
    ]:
        try:
            r = requests.get(url, headers=hdr, timeout=12, verify=False)
            if r.status_code == 200:
                for it in r.json():
                    c = it.get(ck, "").strip(); n = it.get(nk, "").strip()
                    if len(c) == 4 and c.isdigit() and n:
                        names[c] = n
        except:
            pass
    return names

def search_stocks(q: str, names: Dict[str, str], limit: int = 10) -> List[Tuple[str, str]]:
    if not q.strip(): return []
    qu = q.strip().upper(); ql = q.strip().lower()
    t1, t2, t3 = [], [], []
    for code, name in names.items():
        if code == qu: t1.append((code, name))
        elif code.startswith(qu): t2.append((code, name))
        elif ql in name.lower() or ql in code.lower(): t3.append((code, name))
    seen, out = set(), []
    for item in t1 + t2 + t3:
        if item[0] not in seen:
            seen.add(item[0]); out.append(item)
    return out[:limit]

# ══════════════════════════════════════════════════════════════════
#  TECHNICALS
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=86400, show_spinner=False)
def resolve_suffix(code: str) -> str:
    for sfx in [".TW", ".TWO"]:
        try:
            p = getattr(yf.Ticker(code + sfx).fast_info, "last_price", None)
            if p and float(p) > 0: return sfx
        except: pass
    return ".TW"

def calc_rsi(s: pd.Series, n: int = 14) -> pd.Series:
    d = s.diff()
    g = d.clip(lower=0).ewm(com=n-1, min_periods=n).mean()
    l = (-d).clip(lower=0).ewm(com=n-1, min_periods=n).mean()
    return 100 - 100 / (1 + g / l)

def calc_macd(s: pd.Series):
    m = s.ewm(span=12, adjust=False).mean() - s.ewm(span=26, adjust=False).mean()
    return m, m.ewm(span=9, adjust=False).mean()

def calc_bb(s: pd.Series, n: int = 20):
    m = s.rolling(n).mean(); std = s.rolling(n).std()
    return m + 2*std, m, m - 2*std

def calc_atr(h: pd.Series, l: pd.Series, c: pd.Series, n: int = 14) -> pd.Series:
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def calc_pivot(h: float, l: float, c: float) -> Dict[str, float]:
    p = (h+l+c)/3
    return {"PP":round(p,2),"R1":round(2*p-l,2),"R2":round(p+(h-l),2),"R3":round(h+2*(p-l),2),
            "S1":round(2*p-h,2),"S2":round(p-(h-l),2),"S3":round(l-2*(h-p),2)}

def calc_sharpe(ret: pd.Series) -> float:
    if len(ret) < 10: return 0.0
    m = ret.mean()*252; s = ret.std()*np.sqrt(252)
    return round(m/s, 2) if s > 0 else 0.0

def calc_max_dd(prices: pd.Series) -> float:
    roll_max = prices.expanding().max()
    dd = (prices - roll_max) / roll_max
    return round(float(dd.min())*100, 1)

def vol_anomaly(vol: pd.Series) -> Tuple[float, str]:
    if len(vol) < 21: return 1.0, "normal"
    avg = vol.rolling(20).mean().iloc[-1]
    ratio = float(vol.iloc[-1]) / avg if avg > 0 else 1.0
    return round(ratio, 2), ("extreme" if ratio >= 3 else ("high" if ratio >= 1.8 else "normal"))

def backtest_ma(hist: pd.DataFrame) -> Dict[str, Any]:
    if hist is None or len(hist) < 40: return {}
    c = hist["Close"]
    ma5 = c.rolling(5).mean(); ma20 = c.rolling(20).mean()
    sig = (ma5 > ma20).astype(int)
    prev_sig = sig.shift(1).fillna(0)
    buy_sig = (sig == 1) & (prev_sig == 0)
    trades = []; in_trade = False; buy_px = 0.0
    for i in range(len(hist)):
        if buy_sig.iloc[i] and not in_trade:
            buy_px = float(c.iloc[i]); in_trade = True
        elif in_trade and (sig.iloc[i] == 0 or i == len(hist)-1):
            trades.append((float(c.iloc[i]) - buy_px) / buy_px * 100); in_trade = False
    if not trades: return {}
    wins = [t for t in trades if t > 0]
    return {"trades":len(trades),"win_rate":round(len(wins)/len(trades)*100,1),
            "avg_ret":round(float(np.mean(trades)),1),
            "max_win":round(float(max(trades)),1),"max_loss":round(float(min(trades)),1)}

# ══════════════════════════════════════════════════════════════════
#  ★ 核心：點火訊號計算 (Fire Signal Score)
#  ─────────────────────────────────────────────────────────────────
#  滿分 6 個訊號，每個 bool / 分數
#  1. 均線多頭排列  MA5 > MA10 > MA20 > MA60 (剛形成 ← 昨天不是)
#  2. 爆量長紅      今日量 > N日均量×倍率 + 今日收紅
#  3. 均線糾結      MA5/10/20 最大差距 < 3%（底部蓄力）
#  4. 籌碼買超估算  機構資金連續流入（近3或5日）
#  5. RSI 黃金區    35~60（未超買、有動能空間）
#  6. MACD 金叉     DIF > DEA 且轉正
# ══════════════════════════════════════════════════════════════════
def calc_fire_signals(hist: pd.DataFrame, vol_mult_thresh: float = 2.0,
                       vol_days: int = 5, tangle_pct: float = 3.0,
                       inst_days: int = 3) -> Dict[str, Any]:
    """
    回傳所有點火訊號的詳細資訊。
    vol_mult_thresh: 爆量閾值倍率（預設 2.0）
    vol_days:        比較基準天數（預設 5 日均量）
    tangle_pct:      均線糾結容許差距 % （預設 3%）
    inst_days:       投信連買判斷天數（3 or 5）
    """
    result = {
        "fire_score": 0,
        "ma_bull":     False,  # 多頭排列（剛形成）
        "ma_bull_cont":False,  # 多頭排列（持續中）
        "vol_explosion":False, # 爆量長紅
        "vol_ratio":   1.0,
        "ma_tangle":   False,  # 均線糾結
        "tangle_pct":  999.0,
        "inst_buy":    False,  # 機構買超估算
        "inst_streak": 0,
        "rsi_golden":  False,  # RSI 黃金區
        "rsi_val":     None,
        "macd_cross":  False,  # MACD 金叉
    }
    if hist is None or len(hist) < 30:
        return result

    c = hist["Close"]; v = hist["Volume"]
    o = hist["Open"] if "Open" in hist.columns else c

    # ── 均線
    ma5  = c.rolling(5).mean()
    ma10 = c.rolling(10).mean()
    ma20 = c.rolling(20).mean()
    ma60 = c.rolling(60).mean() if len(c) >= 60 else pd.Series([np.nan]*len(c), index=c.index)

    last = -1; prev = -2
    def safe(s, i): return float(s.iloc[i]) if not pd.isna(s.iloc[i]) else None

    m5L  = safe(ma5, last);  m5P  = safe(ma5, prev)
    m10L = safe(ma10, last); m10P = safe(ma10, prev)
    m20L = safe(ma20, last); m20P = safe(ma20, prev)
    m60L = safe(ma60, last)
    cL   = float(c.iloc[last]); oL = float(o.iloc[last])

    # 1. 均線多頭排列
    bull_now  = all(x is not None for x in [m5L, m10L, m20L, m60L]) and m5L > m10L > m20L > m60L
    bull_prev = all(x is not None for x in [m5P, m10P, m20P]) and m5P > m10P > m20P
    result["ma_bull_cont"] = bull_now
    # 「剛形成」= 今日是，昨日不完全是（MA5 剛穿越 MA10 or MA10 剛穿越 MA20）
    ma5_cross  = (m5L is not None and m10L is not None and m5P is not None and m10P is not None
                  and m5L > m10L and m5P <= m10P)
    ma10_cross = (m10L is not None and m20L is not None and m10P is not None and m20P is not None
                  and m10L > m20L and m10P <= m20P)
    result["ma_bull"] = bull_now and (ma5_cross or ma10_cross)

    # 2. 爆量長紅
    if len(v) > vol_days:
        vol_avg = float(v.iloc[-(vol_days+1):-1].mean())
        vol_now = float(v.iloc[last])
        ratio = vol_now / vol_avg if vol_avg > 0 else 1.0
        result["vol_ratio"] = round(ratio, 2)
        result["vol_explosion"] = (ratio >= vol_mult_thresh) and (cL > oL)

    # 3. 均線糾集（5/10/20 最大差距 < tangle_pct%）
    if all(x is not None for x in [m5L, m10L, m20L]):
        vals = [m5L, m10L, m20L]
        spread = (max(vals) - min(vals)) / min(vals) * 100
        result["tangle_pct"] = round(spread, 2)
        result["ma_tangle"] = spread < tangle_pct

    # 4. 投信買超估算：近 inst_days 日的「大成交量上漲日」比例
    #    因無官方三大法人即時資料，以 (收>開 + 量>均量) 作為機構買超代理指標
    if len(hist) >= inst_days + 2:
        recent = hist.iloc[-(inst_days+1):-1]  # 排除今日，看近幾日
        buy_days = 0
        vol_avg_r = float(v.iloc[-(inst_days+20):-inst_days].mean()) if len(v) > inst_days+20 else float(v.mean())
        for i in range(len(recent)):
            r_c = float(recent["Close"].iloc[i]); r_o = float(recent["Open"].iloc[i])
            r_v = float(recent["Volume"].iloc[i])
            if r_c > r_o and r_v > vol_avg_r * 1.2:
                buy_days += 1
        result["inst_streak"] = buy_days
        result["inst_buy"] = buy_days >= max(2, inst_days - 1)  # 至少 inst_days-1 天符合

    # 5. RSI 黃金區
    rsi_s = calc_rsi(c, 14)
    rsi_val = float(rsi_s.iloc[last]) if not pd.isna(rsi_s.iloc[last]) else None
    result["rsi_val"] = rsi_val
    result["rsi_golden"] = rsi_val is not None and 35 <= rsi_val <= 62

    # 6. MACD 金叉（DIF>DEA 且 MACD 柱由負轉正）
    macd_l, macd_s = calc_macd(c)
    dif  = float(macd_l.iloc[last]) if not pd.isna(macd_l.iloc[last]) else None
    dea  = float(macd_s.iloc[last]) if not pd.isna(macd_s.iloc[last]) else None
    dif_p = float(macd_l.iloc[prev]) if not pd.isna(macd_l.iloc[prev]) else None
    dea_p = float(macd_s.iloc[prev]) if not pd.isna(macd_s.iloc[prev]) else None
    if all(x is not None for x in [dif, dea, dif_p, dea_p]):
        cross = (dif > dea) and (dif_p <= dea_p)   # 剛金叉
        cont  = (dif > dea)                          # 持續金叉
        result["macd_cross"] = cross or cont

    # ── 加總火力分數
    score = sum([
        2 if result["ma_bull"] else (1 if result["ma_bull_cont"] else 0),
        2 if result["vol_explosion"] else 0,
        1 if result["ma_tangle"] else 0,
        1 if result["inst_buy"] else 0,
        1 if result["rsi_golden"] else 0,
        1 if result["macd_cross"] else 0,
    ])
    result["fire_score"] = min(score, 8)
    return result

# ══════════════════════════════════════════════════════════════════
#  TARGET PRICE  (永遠 > 現價)
# ══════════════════════════════════════════════════════════════════
def estimate_target(price, pe, eps, pb, roe, dy, a_mean, a_low, a_high, n_ana,
                    rsi=None, macd=None, macd_sig=None, rg=None):
    if not price or price <= 0: return None, None, None
    rate = 0.08
    if roe:
        r = roe*100
        rate += (0.08 if r>=25 else 0.05 if r>=18 else 0.03 if r>=12 else 0.01 if r>=8 else 0)
    if dy:
        y = dy*100
        rate += (0.04 if y>=6 else 0.02 if y>=4 else 0.01 if y>=2 else 0)
    if rg and rg > 0:
        rate += (0.06 if rg>=.3 else 0.03 if rg>=.15 else 0.01 if rg>=.05 else 0)
    if pe and 0 < pe <= 15: rate += 0.03
    if rsi and 35 <= rsi <= 60: rate += 0.02
    if macd is not None and macd_sig is not None and macd > macd_sig: rate += 0.02
    model = price*(1+rate)
    final = model
    if a_mean and a_mean > price and n_ana and n_ana >= 3:
        w = min(0.6, 0.2 + n_ana*0.04)
        final = a_mean*w + model*(1-w)
    final = max(final, price*1.05); final = round(final, 1)
    tl = round(max(a_low if (a_low and a_low>price) else price*(1+rate*.6), price*1.03), 1)
    th = round(a_high if (a_high and a_high>price) else price*(1+rate*1.6), 1)
    tl = min(tl, final*0.97); th = max(th, final*1.08)
    return final, round(tl,1), round(th,1)

# ══════════════════════════════════════════════════════════════════
#  COMPOSITE SCORE (5維度)
# ══════════════════════════════════════════════════════════════════
def composite_score(d: dict) -> Tuple[int, Dict[str, int], str]:
    total = 0; det = {}
    px = d.get("price",0) or 0
    ma5 = d.get("ma5"); ma20 = d.get("ma20"); ma60 = d.get("ma60")
    rsi = d.get("rsi"); macd = d.get("macd"); macd_s = d.get("macd_signal")
    bb_u = d.get("bb_upper"); bb_l = d.get("bb_lower")
    pe = d.get("pe"); pb = d.get("pb"); roe = d.get("roe")
    dy = d.get("dividend_yield"); pm = d.get("profit_margin"); rg = d.get("revenue_growth")
    cr = d.get("current_ratio"); de = d.get("debt_to_equity"); vr = d.get("volume_ratio",1.0)
    fs = d.get("fire_score", 0)

    # 技術 30  (加入火力分數)
    tech = 0
    if px and ma5 and ma20:
        tech += (10 if px>ma5>ma20 else (6 if px>ma20 else 0))
        if ma60 and ma20>ma60: tech += 2
    if rsi is not None:
        tech += (8 if 40<=rsi<=60 else (6 if 30<=rsi<40 else (4 if 60<rsi<=70 else (5 if rsi<30 else 0))))
    if macd is not None and macd_s is not None:
        tech += (7 if macd>macd_s and macd>0 else (3 if macd>macd_s else 0))
    if bb_u and bb_l and px:
        bw = bb_u - bb_l
        if bw > 0:
            pos = (px-bb_l)/bw
            tech += (5 if .2<=pos<=.55 else (3 if pos<.2 else 0))
    det["技術"] = min(tech, 30); total += det["技術"]

    # 點火訊號 15  (新增)
    det["點火"] = min(fs * 2, 15); total += det["點火"]

    # 基本面 27
    fund = 0
    if pe: fund += (9 if 6<=pe<=14 else (6 if 14<pe<=20 else (3 if pe<6 else 0)))
    if pb: fund += (6 if .5<=pb<=2 else (3 if 2<pb<=3 else 0))
    if roe:
        r = roe*100
        fund += (8 if r>=20 else (5 if r>=12 else (2 if r>=8 else 0)))
    if dy:
        y = dy*100
        fund += (4 if y>=5 else (2 if y>=3 else 0))
    det["基本面"] = min(fund, 27); total += det["基本面"]

    # 動能 18
    mom = 0
    up = d.get("upside")
    if up is not None:
        mom += (9 if up>=25 else (6 if up>=15 else (4 if up>=8 else (2 if up>=3 else 0))))
    if rg:
        mom += (7 if rg>=.2 else (4 if rg>=.1 else (2 if rg>=0 else 0)))
    det["動能"] = min(max(mom,0), 18); total += det["動能"]

    # 財務健康 10
    fh = 0
    if cr: fh += (5 if cr>=2 else (3 if cr>=1.5 else (1 if cr>=1 else 0)))
    if de is not None: fh += (5 if de<=.5 else (3 if de<=1 else (1 if de<=2 else 0)))
    det["財務健康"] = min(fh, 10); total += det["財務健康"]

    total = max(min(total, 100), 0)
    sig = "BUY" if total>=70 else ("WATCH" if total>=53 else ("HOLD" if total>=36 else "AVOID"))
    return total, det, sig

def financial_health(d: dict) -> Tuple[str, int]:
    s = 0
    cr=d.get("current_ratio"); de=d.get("debt_to_equity")
    pm=d.get("profit_margin"); roe=d.get("roe"); rg=d.get("revenue_growth")
    if cr: s += (25 if cr>=2 else (18 if cr>=1.5 else (10 if cr>=1 else 0)))
    if de is not None: s += (25 if de<=.5 else (18 if de<=1 else (10 if de<=2 else 0)))
    if pm: s += (20 if pm>=.2 else (14 if pm>=.1 else (8 if pm>=.05 else 0)))
    if roe: r=roe*100; s += (15 if r>=20 else (10 if r>=12 else (5 if r>=8 else 0)))
    if rg: s += (15 if rg>=.2 else (10 if rg>=.1 else (5 if rg>=0 else 0)))
    grade = "A" if s>=75 else ("B" if s>=55 else ("C" if s>=35 else "D"))
    return grade, s

# ══════════════════════════════════════════════════════════════════
#  FETCH STOCK  (完整版)
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=180, show_spinner=False)
def fetch_stock(code: str) -> dict:
    D = dict(
        code=code, name=code, suffix=".TW", error=None,
        price=None, prev_close=None, open=None, high=None, low=None,
        volume=None, avg_volume=None, market_cap=None,
        pe=None, pb=None, eps=None, roe=None,
        dividend_yield=None, profit_margin=None, revenue_growth=None,
        current_ratio=None, quick_ratio=None, debt_to_equity=None,
        target_price=None, target_low=None, target_high=None, upside=None,
        analyst_count=0, ma5=None, ma10=None, ma20=None, ma60=None, ma120=None,
        rsi=None, rsi6=None, macd=None, macd_signal=None,
        bb_upper=None, bb_lower=None, bb_mid=None, atr=None,
        beta=None, volume_ratio=1.0, volume_status="normal",
        pivot={}, backtest={}, sharpe=0.0, max_drawdown=0.0,
        hist=None, score=0, score_detail={}, signal="HOLD",
        industry="—", fin_health_grade="C", fin_health_score=0,
        foreign_net=None, trust_net=None, dealer_net=None,
        fire={}  # 點火訊號詳情
    )
    try:
        sfx = resolve_suffix(code); D["suffix"] = sfx
        tk = yf.Ticker(code+sfx); info = tk.info
        # 優先用內建中文名
        D["name"] = _BUILTIN.get(code, (info.get("longName") or info.get("shortName") or code).strip())
        D["industry"] = _INDUSTRY.get(code, info.get("sector") or "—")

        # 價格
        D["price"]      = info.get("currentPrice") or info.get("regularMarketPrice")
        D["prev_close"] = info.get("previousClose") or info.get("regularMarketPreviousClose")
        D["open"]       = info.get("open") or info.get("regularMarketOpen")
        D["high"]       = info.get("dayHigh") or info.get("regularMarketDayHigh")
        D["low"]        = info.get("dayLow") or info.get("regularMarketDayLow")
        D["volume"]     = info.get("volume") or info.get("regularMarketVolume")
        D["avg_volume"] = info.get("averageVolume")
        D["market_cap"] = info.get("marketCap")
        if not D["price"]:
            fi = tk.fast_info
            D["price"]      = getattr(fi, "last_price", None)
            D["prev_close"] = getattr(fi, "previous_close", None)

        # 基本面
        D["pe"]              = info.get("trailingPE") or info.get("forwardPE")
        D["pb"]              = info.get("priceToBook")
        D["eps"]             = info.get("trailingEps") or info.get("forwardEps")
        D["roe"]             = info.get("returnOnEquity")
        D["dividend_yield"]  = info.get("dividendYield")
        D["profit_margin"]   = info.get("profitMargins")
        D["revenue_growth"]  = info.get("revenueGrowth")
        D["analyst_count"]   = info.get("numberOfAnalystOpinions") or 0
        D["current_ratio"]   = info.get("currentRatio")
        D["quick_ratio"]     = info.get("quickRatio")
        de_raw = info.get("debtToEquity")
        D["debt_to_equity"]  = de_raw/100 if de_raw else None
        D["beta"]            = info.get("beta")

        # 歷史資料（拉 1 年）
        hist = tk.history(period="1y", auto_adjust=True)
        if hist is not None and not hist.empty and len(hist) >= 20:
            D["hist"] = hist; c = hist["Close"]

            D["ma5"]  = float(c.rolling(5).mean().iloc[-1])
            D["ma10"] = float(c.rolling(10).mean().iloc[-1])
            D["ma20"] = float(c.rolling(20).mean().iloc[-1])
            if len(c) >= 60:  D["ma60"]  = float(c.rolling(60).mean().iloc[-1])
            if len(c) >= 120: D["ma120"] = float(c.rolling(120).mean().iloc[-1])

            D["rsi"]  = float(calc_rsi(c,14).iloc[-1])
            D["rsi6"] = float(calc_rsi(c,6).iloc[-1])
            ml, sl = calc_macd(c)
            D["macd"] = float(ml.iloc[-1]); D["macd_signal"] = float(sl.iloc[-1])
            bu, bm, bl = calc_bb(c)
            D["bb_upper"] = float(bu.iloc[-1]); D["bb_mid"] = float(bm.iloc[-1]); D["bb_lower"] = float(bl.iloc[-1])
            if len(hist) >= 15:
                D["atr"] = float(calc_atr(hist["High"], hist["Low"], hist["Close"], 14).iloc[-1])
            if len(hist) >= 2:
                y = hist.iloc[-2]
                D["pivot"] = calc_pivot(float(y["High"]), float(y["Low"]), float(y["Close"]))
            if "Volume" in hist.columns and len(hist["Volume"]) >= 21:
                D["volume_ratio"], D["volume_status"] = vol_anomaly(hist["Volume"])
            ret = c.pct_change().dropna()
            D["sharpe"]       = calc_sharpe(ret)
            D["max_drawdown"] = calc_max_dd(c)
            D["backtest"]     = backtest_ma(hist)

            # 三大法人估算
            if len(hist) >= 5 and D["market_cap"]:
                recent = hist.tail(5)
                p_trend = (float(recent["Close"].iloc[-1]) - float(recent["Close"].iloc[0])) / float(recent["Close"].iloc[0])
                v_avg   = float(hist["Volume"].mean())
                v_mult  = float(recent["Volume"].mean()) / v_avg if v_avg > 0 else 1
                est = p_trend * v_mult * D["market_cap"] / 1e8
                D["foreign_net"] = round(est*.6, 1)
                D["trust_net"]   = round(est*.25, 1)
                D["dealer_net"]  = round(est*.15, 1)

            # ★ 點火訊號（使用用戶 session 參數）
            fs_params = st.session_state.get("fire_params", {})
            D["fire"] = calc_fire_signals(
                hist,
                vol_mult_thresh = fs_params.get("vol_mult", 2.0),
                vol_days        = fs_params.get("vol_days", 5),
                tangle_pct      = fs_params.get("tangle_pct", 3.0),
                inst_days       = fs_params.get("inst_days", 3),
            )
            D["fire_score"] = D["fire"].get("fire_score", 0)

        # 目標價
        tp, tl, th = estimate_target(
            D["price"], D["pe"], D["eps"], D["pb"], D["roe"], D["dividend_yield"],
            info.get("targetMeanPrice"), info.get("targetLowPrice"), info.get("targetHighPrice"),
            D["analyst_count"], D["rsi"], D["macd"], D["macd_signal"], D["revenue_growth"]
        )
        D["target_price"] = tp; D["target_low"] = tl; D["target_high"] = th
        if D["price"] and tp: D["upside"] = (tp - D["price"]) / D["price"] * 100

        D["score"], D["score_detail"], D["signal"] = composite_score(D)
        D["fin_health_grade"], D["fin_health_score"] = financial_health(D)
    except Exception as e:
        D["error"] = str(e)
    return D

# ══════════════════════════════════════════════════════════════════
#  SCAN
# ══════════════════════════════════════════════════════════════════
def scan_batch(codes: List[str], min_score=55, min_upside=5.0, max_pe=60.0,
               signal_filter="全部", min_fire=0,
               require_ma_bull=False, require_vol_exp=False,
               require_tangle=False, require_inst=False,
               progress_cb=None, max_workers=16) -> List[dict]:
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(fetch_stock, c): c for c in codes}
        done = 0
        for fut in concurrent.futures.as_completed(futs):
            done += 1
            if progress_cb: progress_cb(done, len(codes), futs[fut])
            try:
                d = fut.result()
                if not d.get("price"): continue
                if d.get("score",0) < min_score: continue
                up = d.get("upside")
                if up is not None and up < min_upside: continue
                pe = d.get("pe")
                if pe is not None and pe > max_pe: continue
                if signal_filter != "全部" and d.get("signal") != signal_filter: continue
                # 點火濾網
                fr = d.get("fire", {})
                fs = d.get("fire_score", 0)
                if fs < min_fire: continue
                if require_ma_bull   and not (fr.get("ma_bull") or fr.get("ma_bull_cont")): continue
                if require_vol_exp   and not fr.get("vol_explosion"): continue
                if require_tangle    and not fr.get("ma_tangle"): continue
                if require_inst      and not fr.get("inst_buy"): continue
                results.append(d)
            except: pass
    results.sort(key=lambda x: (x.get("fire_score",0)*10 + x.get("score",0)), reverse=True)
    return results

# ══════════════════════════════════════════════════════════════════
#  WATCHLIST PRICES
# ══════════════════════════════════════════════════════════════════
@st.cache_data(ttl=60, show_spinner=False)
def fetch_wl_prices(codes_t: tuple) -> Dict[str, dict]:
    res = {}
    def _f(code):
        try:
            sfx = resolve_suffix(code); tk = yf.Ticker(code+sfx); fi = tk.fast_info
            px = getattr(fi,"last_price",None); pc = getattr(fi,"previous_close",None)
            if px and pc: res[code] = {"price":px,"chg":(px-pc)/pc*100}
        except: pass
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(_f, codes_t))
    return res

# ══════════════════════════════════════════════════════════════════
#  ALERTS
# ══════════════════════════════════════════════════════════════════
def check_alerts(d: dict) -> List[str]:
    code = d.get("code",""); alerts = []
    px = d.get("price"); rsi = d.get("rsi"); ma20 = d.get("ma20")
    cfg = st.session_state.alert_cfg.get(code, {})
    if rsi and rsi > 76: alerts.append(f"{code} RSI={rsi:.0f} 超買")
    if rsi and rsi < 24: alerts.append(f"{code} RSI={rsi:.0f} 超賣")
    if px and ma20 and abs(px-ma20)/ma20 < .004: alerts.append(f"{code} 貼近MA20")
    if d.get("volume_status") == "extreme": alerts.append(f"{code} 爆量 {d.get('volume_ratio',1):.1f}x")
    if d.get("fire",{}).get("ma_bull"): alerts.append(f"{code} 🔥 均線剛形成多頭排列！")
    if d.get("fire",{}).get("vol_explosion"): alerts.append(f"{code} 🔥 爆量長紅訊號！")
    if cfg.get("price_above") and px and px >= cfg["price_above"]: alerts.append(f"{code} 突破 {cfg['price_above']}")
    if cfg.get("price_below") and px and px <= cfg["price_below"]: alerts.append(f"{code} 跌破 {cfg['price_below']}")
    return alerts

# ══════════════════════════════════════════════════════════════════
#  NEWS
# ══════════════════════════════════════════════════════════════════
_POS=["上漲","漲停","創高","突破","強勢","獲利","配息","利多","成長","亮眼","超越","買進","新高","大漲","增加"]
_NEG=["下跌","跌停","創低","破底","虧損","利空","衰退","低於","警示","賣出","停損","大跌","崩跌","減少"]

def sentiment(t: str) -> str:
    s = sum(1 for w in _POS if w in t) - sum(1 for w in _NEG if w in t)
    return "pos" if s>0 else ("neg" if s<0 else "neu")

@st.cache_data(ttl=600, show_spinner=False)
def fetch_news(code: str, name: str) -> List[dict]:
    news = []; hdr = {"User-Agent":"Mozilla/5.0"}
    for url, sel in [
        (f"https://tw.stock.yahoo.com/quote/{code}.TW/news","h3 a"),
        (f"https://news.cnyes.com/news/cat/twstock?code={code}","a[href*='/news/id/']"),
    ]:
        if news: break
        try:
            soup = BeautifulSoup(requests.get(url,headers=hdr,timeout=8,verify=False).text,"lxml")
            for a in soup.select(sel)[:12]:
                t = a.get_text(strip=True)
                if len(t) < 8: continue
                src = "Yahoo Finance" if "yahoo" in url else "鉅亨網"
                news.append({"t":t,"url":a.get("href",""),"s":sentiment(t),"src":src})
        except: pass
    return news[:10]

# ══════════════════════════════════════════════════════════════════
#  PUSH  (Discord only，移除 LINE)
# ══════════════════════════════════════════════════════════════════
def push_discord(url: str, results: List[dict]) -> bool:
    if not url or not results: return False
    try:
        now = datetime.datetime.now().strftime("%Y/%m/%d %H:%M")
        fields = []
        for d in results[:10]:
            p, tp, up = d.get("price"), d.get("target_price"), d.get("upside")
            em = {"BUY":"🟢","WATCH":"🟡","HOLD":"⚪","AVOID":"🔴"}.get(d.get("signal",""),"⚪")
            nm = d.get("name", d.get("code",""))
            fs = d.get("fire_score",0)
            fire_str = "🔥"*min(fs,4) if fs>=3 else ""
            v = f"`現價 {p:.1f}` → `目標 {tp:.1f}` (**{up:+.1f}%**)" if (p and tp and up is not None) else "—"
            fields.append({"name":f"{em} {fire_str} {d['code']} {nm} · {d.get('score',0)}分 · 點火{fs}","value":v,"inline":False})
        
        payload = {"embeds":[{
            "title":f"⚡ 台股狙擊手 v10 · {now}",
            "color":0x00e87a,"fields":fields,
            "footer":{"text":"技術/籌碼/型態三重濾網 · 僅供參考"}
        }]}

        # 稍微加長 timeout，避免雲端環境延遲
        r = requests.post(url, json=payload, timeout=10) 
        
        # 如果不是 200 或 204，代表 Discord 拒絕了你的請求
        if r.status_code not in (200, 204):
            import streamlit as st
            st.error(f"Discord API 錯誤: 狀態碼 {r.status_code}, 回應: {r.text}")
            return False
            
        return True
    except requests.exceptions.Timeout:
        import streamlit as st
        st.error("連線到 Discord 超時 (Timeout)，請檢查伺服器網路狀態。")
        return False
    except Exception as e:
        import streamlit as st
        st.error(f"發送 Discord 時發生例外錯誤: {str(e)}")
        return False

def results_to_csv(results: List[dict]) -> bytes:
    rows = []
    for d in results:
        fr = d.get("fire",{})
        rows.append({
            "代號":d.get("code",""),"名稱":d.get("name",""),"產業":d.get("industry",""),
            "信號":d.get("signal",""),"評分":d.get("score",0),"點火分":d.get("fire_score",0),
            "現價":d.get("price",""),"目標價":d.get("target_price",""),
            "上漲空間%":round(d.get("upside") or 0,1),
            "PE":d.get("pe",""),"PB":d.get("pb",""),
            "ROE%":round((d.get("roe") or 0)*100,1),
            "殖利率%":round((d.get("dividend_yield") or 0)*100,1),
            "RSI":round(d.get("rsi") or 0,1),
            "量能倍率":d.get("volume_ratio",""),
            "均線多頭":fr.get("ma_bull_cont",False),
            "均線剛排列":fr.get("ma_bull",False),
            "爆量長紅":fr.get("vol_explosion",False),
            "均線糾集":fr.get("ma_tangle",False),
            "機構連買":fr.get("inst_buy",False),
            "RSI黃金區":fr.get("rsi_golden",False),
            "MACD金叉":fr.get("macd_cross",False),
            "財務健康":d.get("fin_health_grade",""),
            "Beta":d.get("beta",""),"最大回撤%":d.get("max_drawdown",""),"夏普":d.get("sharpe",""),
            "掃描時間":datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        })
    return pd.DataFrame(rows).to_csv(index=False,encoding="utf-8-sig").encode("utf-8-sig")

# ══════════════════════════════════════════════════════════════════
#  SCHEDULER
# ══════════════════════════════════════════════════════════════════
def _job():
    p = st.session_state.get("scan_params",{}); c = st.session_state.get("scan_codes",[])
    if not c: return
    res = scan_batch(c, **p)
    st.session_state.scan_results = res
    st.session_state.last_scan_time = datetime.datetime.now()
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    st.session_state.sched_log.insert(0, ("ok", f"[{ts}] 排程完成 — 命中 {len(res)} 檔"))
    all_alts = []
    for d in res: all_alts.extend(check_alerts(d))
    if all_alts: st.session_state.alerts = all_alts[-20:]
    wh = st.session_state.get("auto_webhook","")
    if wh and res:
        ok = push_discord(wh, res)
        st.session_state.sched_log.insert(0, ("ok" if ok else "err", f"  Discord {'OK' if ok else 'FAIL'}"))
    st.session_state.sched_log = st.session_state.sched_log[:80]

def start_sched(mode, hour=9, minute=30, interval=30):
    try: st.session_state.scheduler.shutdown(wait=False)
    except: pass
    s = BackgroundScheduler(timezone="Asia/Taipei")
    if mode=="fixed": s.add_job(_job, CronTrigger(hour=hour,minute=minute,day_of_week="mon-fri"))
    else: s.add_job(_job, IntervalTrigger(minutes=interval))
    s.start(); st.session_state.scheduler = s; st.session_state.sched_running = True

def stop_sched():
    try: st.session_state.scheduler.shutdown(wait=False)
    except: pass
    st.session_state.scheduler = None; st.session_state.sched_running = False

# ══════════════════════════════════════════════════════════════════
#  CHART
# ══════════════════════════════════════════════════════════════════
def make_chart(d: dict) -> Optional[go.Figure]:
    hist = d.get("hist")
    if hist is None or hist.empty or len(hist) < 5: return None
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True,
                        row_heights=[.5,.15,.2,.15], vertical_spacing=.016)
    c = hist["Close"]
    fig.add_trace(go.Candlestick(
        x=hist.index, open=hist["Open"], high=hist["High"], low=hist["Low"], close=c,
        name="K", increasing_line_color="#00e87a", decreasing_line_color="#ff2d55",
        increasing_fillcolor="rgba(0,232,122,.85)", decreasing_fillcolor="rgba(255,45,85,.85)"), 1,1)
    for period, col, w in [(5,"#ffd60a",1.2),(10,"#ff7b00",1.0),(20,"#1e90ff",1.6),(60,"#bf5af2",1.2),(120,"#32d2f5",1.0)]:
        ma = c.rolling(period).mean()
        fig.add_trace(go.Scatter(x=hist.index,y=ma,mode="lines",
                                 line=dict(color=col,width=w),name=f"MA{period}",opacity=.88),1,1)
    bu,bm,bl = calc_bb(c)
    fig.add_trace(go.Scatter(x=hist.index,y=bu,mode="lines",
                             line=dict(color="rgba(255,255,255,.07)",width=.8,dash="dot"),showlegend=False),1,1)
    fig.add_trace(go.Scatter(x=hist.index,y=bl,mode="lines",
                             line=dict(color="rgba(255,255,255,.07)",width=.8,dash="dot"),
                             fill="tonexty",fillcolor="rgba(255,255,255,.015)",showlegend=False),1,1)
    # ATR
    atr_s = calc_atr(hist["High"],hist["Low"],hist["Close"],14)
    fig.add_trace(go.Scatter(x=hist.index,y=bm+1.5*atr_s,mode="lines",
                             line=dict(color="rgba(255,123,0,.1)",width=.8),showlegend=False),1,1)
    fig.add_trace(go.Scatter(x=hist.index,y=bm-1.5*atr_s,mode="lines",
                             line=dict(color="rgba(255,123,0,.1)",width=.8),
                             fill="tonexty",fillcolor="rgba(255,123,0,.02)",showlegend=False),1,1)
    # Target/Pivot
    tp = d.get("target_price")
    if tp:
        fig.add_hline(y=tp,line_dash="dash",line_color="rgba(0,232,122,.5)",line_width=1.2,
                      annotation_text=f"⚡ {tp:.1f}",annotation_font=dict(size=9,color="#00e87a"),row=1,col=1)
    for key,col in [("R1","rgba(255,45,85,.4)"),("PP","rgba(255,214,10,.5)"),("S1","rgba(0,232,122,.4)")]:
        v_ = d.get("pivot",{}).get(key)
        if v_:
            fig.add_hline(y=v_,line_dash="dot",line_color=col,line_width=.9,
                          annotation_text=f"{key} {v_:.1f}",annotation_font=dict(size=8,color=col.replace(".4","1").replace(".5","1")),row=1,col=1)
    # Volume
    vcol = ["#00e87a" if cl>=op else "#ff2d55" for cl,op in zip(hist["Close"],hist["Open"])]
    fig.add_trace(go.Bar(x=hist.index,y=hist["Volume"],marker_color=vcol,marker_opacity=.6,name="量"),2,1)
    fig.add_trace(go.Scatter(x=hist.index,y=hist["Volume"].rolling(5).mean(),mode="lines",
                             line=dict(color="#ffd60a",width=1.2,dash="dot"),name="Vol5",showlegend=False),2,1)
    fig.add_trace(go.Scatter(x=hist.index,y=hist["Volume"].rolling(20).mean(),mode="lines",
                             line=dict(color="#ff7b00",width=1),name="Vol20",showlegend=False),2,1)
    # MACD
    ml,sl = calc_macd(c); hm = ml-sl
    fig.add_trace(go.Scatter(x=hist.index,y=ml,mode="lines",line=dict(color="#1e90ff",width=1.5),name="DIF"),3,1)
    fig.add_trace(go.Scatter(x=hist.index,y=sl,mode="lines",line=dict(color="#ff7b00",width=1.2),name="DEA"),3,1)
    fig.add_trace(go.Bar(x=hist.index,y=hm,
                         marker_color=["rgba(0,232,122,.5)" if v>=0 else "rgba(255,45,85,.5)" for v in hm],
                         showlegend=False),3,1)
    # RSI
    r14=calc_rsi(c,14); r6=calc_rsi(c,6)
    fig.add_trace(go.Scatter(x=hist.index,y=r14,mode="lines",line=dict(color="#bf5af2",width=1.5),name="RSI14"),4,1)
    fig.add_trace(go.Scatter(x=hist.index,y=r6,mode="lines",line=dict(color="#32d2f5",width=1,dash="dot"),name="RSI6",opacity=.75),4,1)
    for lv,col in [(70,"rgba(255,45,85,.3)"),(50,"rgba(255,255,255,.08)"),(30,"rgba(0,232,122,.3)")]:
        fig.add_hline(y=lv,line_dash="dot",line_color=col,line_width=.8,row=4,col=1)
    BG="#02060f"
    fig.update_layout(paper_bgcolor=BG,plot_bgcolor=BG,
        font=dict(family="JetBrains Mono",size=9.5,color="#375872"),
        legend=dict(bgcolor="rgba(0,0,0,0)",font=dict(size=9),orientation="h",yanchor="bottom",y=1.01,xanchor="right",x=1),
        margin=dict(l=52,r=12,t=4,b=4),height=570,xaxis_rangeslider_visible=False)
    for i in range(1,5):
        fig.update_yaxes(row=i,col=1,gridcolor="#0c1b30",zerolinecolor="#112038",tickfont=dict(size=9),showgrid=True)
    fig.update_xaxes(gridcolor="#0c1b30",showgrid=False,tickfont=dict(size=9))
    return fig

# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════
def fp(v, d=1): return f"{v:,.{d}f}" if v is not None else "—"
def fpc(v, m=100): return f"{v*m:.1f}%" if v is not None else "—"
def fbil(v): return "—" if v is None else (f"{v/1e12:.2f}兆" if v>=1e12 else f"{v/1e8:.1f}億")
def shex(s): return "hi" if s>=70 else ("md" if s>=50 else "lo")
def fire_cls(fs): return "fs-hi" if fs>=4 else ("fs-md" if fs>=2 else "fs-lo")

def sig_badge(sig: str) -> str:
    dot = {"BUY":"●","WATCH":"◆","HOLD":"○","AVOID":"✕"}.get(sig,"○")
    return f'<span class="sig sig-{sig}">{dot} {sig}</span>'

def dc(k, v, cls=""):
    return f'<div class="dc"><div class="dc-k">{k}</div><div class="dc-v {cls}">{v}</div></div>'

def fire_badges_html(fr: dict) -> str:
    parts = []
    if fr.get("ma_bull"):      parts.append('<span class="fbadge fb-ma">⚡ 均線剛排列</span>')
    elif fr.get("ma_bull_cont"): parts.append('<span class="fbadge fb-ma">✓ 均線多頭</span>')
    if fr.get("vol_explosion"): parts.append(f'<span class="fbadge fb-vol">🔥 爆量{fr.get("vol_ratio",1):.1f}x</span>')
    if fr.get("ma_tangle"):     parts.append(f'<span class="fbadge fb-tangle">⊞ 均線糾集{fr.get("tangle_pct",0):.1f}%</span>')
    if fr.get("inst_buy"):      parts.append(f'<span class="fbadge fb-inst">▲ 機構連買{fr.get("inst_streak",0)}日</span>')
    if fr.get("rsi_golden"):    parts.append(f'<span class="fbadge fb-rsi">RSI {fr.get("rsi_val",0):.0f}</span>')
    if fr.get("macd_cross"):    parts.append('<span class="fbadge fb-macd">MACD金叉</span>')
    return "".join(parts)

# ══════════════════════════════════════════════════════════════════
#  INIT
# ══════════════════════════════════════════════════════════════════
if "fire_params" not in st.session_state:
    st.session_state["fire_params"] = {"vol_mult":2.0,"vol_days":5,"tangle_pct":3.0,"inst_days":3}

with st.spinner("初始化系統..."):
    ALL = load_names()

# ══════════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════════
now_s    = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
sc_run   = st.session_state.sched_running
lt       = st.session_state.last_scan_time
res_count= len(st.session_state.scan_results)
wl_count = len(st.session_state.watchlist)
alt_count= len(st.session_state.alerts)

tape_html = ""
for tc in ["2330","2317","2454","2412","2882","2603","6669","2308"]:
    nm = ALL.get(tc, tc)
    tape_html += f'<span class="ti"><span class="ti-code">{tc}</span><span style="color:var(--t3);margin:0 2px">·</span><span style="color:var(--t1)">{nm}</span></span>'

sched_chip = '<span class="chip chip-on">● 排程運行</span>' if sc_run else '<span class="chip chip-off">○ 排程待機</span>'
alert_chip = f'<span class="chip chip-warn">🔔 {alt_count} 警示</span>' if alt_count else ""

st.markdown(
    '<div class="hdr"><div class="hdr-glow"></div>'
    '<div class="hdr-inner"><div class="hdr-bar"></div>'
    '<div class="hdr-body"><div class="hdr-ico">⚡</div>'
    '<div class="hdr-txt">'
    '<div class="hdr-eyebrow">TAIWAN STOCK · AI SNIPER · REAL-TIME ENGINE v10</div>'
    '<div class="hdr-title">台股 AI 狙擊手 ULTRA</div>'
    '<div class="hdr-sub">均線多頭排列 · 爆量長紅 · 均線糾集 · 機構連買 · Pivot · 回測 · 警示</div>'
    '<div class="hdr-chips">'
    f'<span class="chip chip-live"><span class="chip-dot"></span> LIVE</span>'
    f'<span class="chip chip-time">⏱ {now_s} CST</span>'
    '<span class="chip chip-v10">✦ v10</span>'
    f'{sched_chip}{alert_chip}'
    '</div></div></div>'
    '<div class="hdr-stats">'
    f'<div class="hdr-stat"><div class="hdr-stat-n" style="color:var(--g)">{len(ALL)}</div><div class="hdr-stat-l">股票庫</div></div>'
    f'<div class="hdr-stat"><div class="hdr-stat-n" style="color:var(--b)">{res_count}</div><div class="hdr-stat-l">命中數</div></div>'
    f'<div class="hdr-stat"><div class="hdr-stat-n" style="color:var(--p)">{wl_count}</div><div class="hdr-stat-l">自選股</div></div>'
    f'<div class="hdr-stat"><div class="hdr-stat-n" style="color:var(--y)">{lt.strftime("%H:%M") if lt else "——"}</div><div class="hdr-stat-l">上次掃描</div></div>'
    '</div></div>'
    f'<div class="hdr-tape"><span style="color:var(--t3);margin-right:4px">WATCH //</span>{tape_html}</div>'
    '</div>',
    unsafe_allow_html=True
)

# Alert banner
if st.session_state.alerts:
    items_html = "".join(f'<span style="color:var(--y)">⚠ {a}</span>' for a in st.session_state.alerts[:5])
    st.markdown(
        f'<div class="alt-bar"><span style="font-size:.8rem">🔔</span>'
        f'<div class="alt-items">{items_html}</div>'
        f'<span style="font-family:var(--mono);font-size:.46rem;color:var(--t2)">共{alt_count}條</span></div>',
        unsafe_allow_html=True
    )
    if st.button("清除警示", key="clr_alerts"):
        st.session_state.alerts = []; st.rerun()

# ══════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    # 搜尋
    st.markdown('<div class="sb-hdr"><span class="sb-dot"></span>個股搜尋</div>', unsafe_allow_html=True)
    st.markdown('<div class="sb-body">', unsafe_allow_html=True)
    q = st.text_input("搜尋", label_visibility="collapsed", placeholder="代號或中文名稱  2330 / 台積電", key="sq")
    if q and q.strip():
        hits = search_stocks(q, ALL, 10)
        if hits:
            for code, name in hits:
                ca, cb, cc = st.columns([3,1,1])
                ca.markdown(f'<div class="sh-row"><span class="sh-code">{code}</span><span class="sh-name">{name}</span></div>', unsafe_allow_html=True)
                if cb.button("GO", key=f"go_{code}", use_container_width=True):
                    st.session_state.selected_stock = code
                    st.session_state.detail_cache = {}
                    st.rerun()
                wl_in = code in st.session_state.watchlist
                if cc.button("★" if wl_in else "☆", key=f"wl_{code}", use_container_width=True):
                    if wl_in: st.session_state.watchlist.remove(code)
                    else: st.session_state.watchlist.append(code)
                    st.rerun()
        else:
            st.markdown('<div style="font-family:var(--mono);font-size:.58rem;color:var(--t2);padding:3px 0">查無結果</div>', unsafe_allow_html=True)
    elif st.session_state.selected_stock:
        sc_ = st.session_state.selected_stock
        st.markdown(f'<div style="font-family:var(--mono);font-size:.58rem;padding:3px 0">分析中 <span style="color:var(--g);font-weight:700">{sc_} {ALL.get(sc_,"")}</span></div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # 自選股
    st.markdown('<div class="sb-hdr"><span class="sb-dot"></span>自選股 ★</div>', unsafe_allow_html=True)
    st.markdown('<div class="sb-body">', unsafe_allow_html=True)
    wl = st.session_state.watchlist
    if wl:
        wl_prices = fetch_wl_prices(tuple(wl))
        for code in wl[:15]:
            nm = ALL.get(code, code); info = wl_prices.get(code,{})
            px_ = info.get("price"); chg_ = info.get("chg")
            px_str = fp(px_,1) if px_ else "—"
            chg_str = ""
            if chg_ is not None:
                cls_ = "wl-up" if chg_>=0 else "wl-dn"
                chg_str = f'<span class="{cls_}">{"▲" if chg_>=0 else "▼"}{abs(chg_):.1f}%</span>'
            col1,col2 = st.columns([4,1])
            col1.markdown(f'<div class="wl-row"><span class="wl-code">{code}</span><span class="wl-name">{nm[:6]}</span><span class="wl-px">{px_str}</span>{chg_str}</div>', unsafe_allow_html=True)
            if col2.button("✕", key=f"rm_{code}", use_container_width=True):
                st.session_state.watchlist.remove(code); st.rerun()
        st.text_area("匯出", value=",".join(wl), height=42, label_visibility="collapsed", key="wl_exp")
    else:
        st.markdown('<div style="font-family:var(--mono);font-size:.56rem;color:var(--t2);padding:3px 0">尚無自選股 · 搜尋時按 ☆</div>', unsafe_allow_html=True)
    wl_imp = st.text_input("批量加入", placeholder="2330,2317...", key="wl_imp", label_visibility="collapsed")
    if wl_imp:
        for ca_ in re.split(r"[,\n\s]+", wl_imp):
            ca_ = ca_.strip()
            if len(ca_)==4 and ca_.isdigit() and ca_ not in st.session_state.watchlist:
                st.session_state.watchlist.append(ca_)
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # ★ 點火訊號參數
    st.markdown('<div class="sb-hdr"><span class="sb-dot"></span>🔥 點火訊號參數</div>', unsafe_allow_html=True)
    st.markdown('<div class="sb-body">', unsafe_allow_html=True)
    fp_ = st.session_state.fire_params
    fp_["vol_mult"]   = st.slider("爆量倍率閾值 (×均量)", 1.2, 5.0, float(fp_.get("vol_mult",2.0)), 0.1, key="sl_vm",
                                   help="今日成交量 > N日均量 × 此倍率 + 收紅 = 爆量長紅")
    fp_["vol_days"]   = st.slider("均量基準天數", 3, 20, int(fp_.get("vol_days",5)), 1, key="sl_vd",
                                   help="計算均量的基準天數（預設5日均量）")
    fp_["tangle_pct"] = st.slider("均線糾集容差 %", 1.0, 8.0, float(fp_.get("tangle_pct",3.0)), 0.5, key="sl_tp",
                                   help="MA5/10/20 最大差距 < 此% 視為糾集蓄力")
    fp_["inst_days"]  = st.radio("機構連買判斷天數", [3, 5], index=0 if fp_.get("inst_days",3)==3 else 1, horizontal=True, key="rd_id")
    st.session_state.fire_params = fp_
    st.markdown('</div>', unsafe_allow_html=True)

    # 掃描設定
    st.markdown('<div class="sb-hdr"><span class="sb-dot"></span>批量掃描設定</div>', unsafe_allow_html=True)
    st.markdown('<div class="sb-body">', unsafe_allow_html=True)
    scan_mode = st.radio("範圍", ["熱門100","自選股","全市場","自訂"], label_visibility="collapsed")
    custom_codes = ""
    if scan_mode == "自訂":
        custom_codes = st.text_area("代號", placeholder="2330,2317...", height=52, label_visibility="collapsed")
    min_score  = st.slider("最低評分",      0,100,50,5, key="sl_sc")
    min_upside = st.slider("最低上漲空間%", 1, 50, 5,1, key="sl_up")
    max_pe     = st.slider("最高 PE",       5,150,65,5, key="sl_pe")
    sig_filter = st.selectbox("信號篩選", ["全部","BUY","WATCH","HOLD","AVOID"], label_visibility="collapsed")

    st.markdown('<div style="font-family:var(--mono);font-size:.42rem;color:var(--g);text-transform:uppercase;letter-spacing:.12em;margin:6px 0 4px">🔥 點火濾網（勾選即強制要求）</div>', unsafe_allow_html=True)
    min_fire      = st.slider("最低點火分數", 0, 8, 0, 1, key="sl_fs")
    req_ma_bull   = st.checkbox("✓ 均線多頭排列（MA5>10>20>60）", key="ck_ma")
    req_vol_exp   = st.checkbox("✓ 爆量長紅（量>均量×倍率）", key="ck_vol")
    req_tangle    = st.checkbox("✓ 均線糾集（底部蓄力）", key="ck_tn")
    req_inst      = st.checkbox("✓ 機構連買估算", key="ck_inst")
    st.markdown('</div>', unsafe_allow_html=True)

    # Discord
    st.markdown('<div class="sb-hdr"><span class="sb-dot"></span>Discord 推播</div>', unsafe_allow_html=True)
    st.markdown('<div class="sb-body">', unsafe_allow_html=True)
    webhook = st.text_input("Webhook", placeholder="https://discord.com/api/webhooks/...", type="password", label_visibility="collapsed", key="wh_in")
    st.session_state.auto_webhook = webhook
    st.markdown('</div>', unsafe_allow_html=True)

    # 排程
    st.markdown('<div class="sb-hdr"><span class="sb-dot"></span>自動排程</div>', unsafe_allow_html=True)
    st.markdown('<div class="sb-body">', unsafe_allow_html=True)
    sched_mode = st.radio("排程模式", ["固定時間","間隔"], horizontal=True, label_visibility="collapsed")
    if sched_mode == "固定時間":
        c1s,c2s = st.columns(2)
        sched_h = c1s.number_input("時",0,23,9,label_visibility="collapsed")
        sched_m = c2s.number_input("分",0,59,30,label_visibility="collapsed")
    else:
        sched_interval = st.slider("每隔(分)",5,180,30,5,key="sl_int")
    ca_,cb_ = st.columns(2)
    with ca_:
        if st.button("▶ 啟動", type="primary", use_container_width=True, key="sched_start"):
            if scan_mode=="熱門100":  sc_codes=list(ALL.keys())[:100]
            elif scan_mode=="自選股": sc_codes=list(st.session_state.watchlist)
            elif scan_mode=="全市場": sc_codes=list(ALL.keys())
            else: sc_codes=[x.strip() for x in re.split(r"[,\n\s]+",custom_codes) if x.strip()]
            st.session_state.scan_codes = sc_codes
            st.session_state.scan_params = dict(
                min_score=min_score, min_upside=min_upside, max_pe=max_pe,
                signal_filter=sig_filter, min_fire=min_fire,
                require_ma_bull=req_ma_bull, require_vol_exp=req_vol_exp,
                require_tangle=req_tangle, require_inst=req_inst
            )
            if sched_mode=="固定時間": start_sched("fixed",hour=int(sched_h),minute=int(sched_m))
            else: start_sched("interval",interval=int(sched_interval))
            st.success("排程已啟動 ✓")
    with cb_:
        if st.button("⏹ 停止", use_container_width=True, key="sched_stop"):
            stop_sched(); st.info("已停止")
    if sc_run:
        st.markdown(
            f'<div style="background:var(--bg2);border:1px solid var(--ln);border-radius:4px;'
            f'padding:7px 10px;margin-top:5px;font-family:var(--mono);font-size:.5rem">'
            f'<div style="display:flex;justify-content:space-between;padding:2px 0">'
            f'<span style="color:var(--t2)">狀態</span><span style="color:var(--g)">● RUNNING</span></div>'
            f'<div style="display:flex;justify-content:space-between;padding:2px 0">'
            f'<span style="color:var(--t2)">上次</span><span style="color:var(--b)">{lt.strftime("%H:%M:%S") if lt else "—"}</span></div>'
            f'<div style="display:flex;justify-content:space-between;padding:2px 0">'
            f'<span style="color:var(--t2)">標的</span><span style="color:var(--b)">{len(st.session_state.scan_codes)} 檔</span></div>'
            f'</div>',
            unsafe_allow_html=True
        )
    st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
#  MAIN TABS
# ══════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4 = st.tabs(["📊  個股深度分析","⚡  智能批量掃描","🎯  多空儀表板","📋  排程紀錄"])

# ════════════════════════ TAB 1 ════════════════════════
with tab1:
    sel = st.session_state.selected_stock
    if not sel:
        st.markdown('<div class="empty"><div class="empty-ico">⚡</div><div class="empty-txt">在左側搜尋欄輸入股票代號或名稱<br>支援上市、上櫃全部個股 · 中文搜尋<br>按 ☆ 加入自選股清單</div></div>', unsafe_allow_html=True)
    else:
        cache = st.session_state.detail_cache
        if cache.get("code") != sel:
            with st.spinner(f"載入 {sel} {ALL.get(sel,'')} ..."):
                d = fetch_stock(sel)
            st.session_state.detail_cache = d
        else:
            d = cache

        if d.get("error") and not d.get("price"):
            st.error(f"無法取得 {sel} 資料 — {d['error']}")
        else:
            px=d.get("price"); prev=d.get("prev_close")
            chg=(px-prev) if (px and prev) else None
            chgp=chg/prev*100 if (chg is not None and prev) else None
            score=d.get("score",0); sig=d.get("signal","HOLD")
            name=d.get("name",sel); suffix=d.get("suffix","")
            tp=d.get("target_price"); tl_=d.get("target_low"); th_=d.get("target_high")
            up=d.get("upside"); mc=d.get("market_cap"); ind=d.get("industry","—")
            fhg=d.get("fin_health_grade","C"); fhs=d.get("fin_health_score",0)
            beta=d.get("beta"); sharpe=d.get("sharpe"); mdd=d.get("max_drawdown")
            vol_r=d.get("volume_ratio",1.0); vol_s=d.get("volume_status","normal")
            chg_cls="pos" if (chg and chg>=0) else "neg"
            chg_sym="▲" if (chg and chg>=0) else "▼"
            fr=d.get("fire",{}); fs=d.get("fire_score",0)
            pe=d.get("pe"); pb=d.get("pb"); roe=d.get("roe")
            dy=d.get("dividend_yield"); pm=d.get("profit_margin"); rg=d.get("revenue_growth")
            cr=d.get("current_ratio"); qr=d.get("quick_ratio"); de=d.get("debt_to_equity")
            rsi=d.get("rsi"); ma5v=d.get("ma5"); ma10v=d.get("ma10")
            ma20v=d.get("ma20"); ma60v=d.get("ma60"); ma120v=d.get("ma120")
            macd_v=d.get("macd"); macd_sv=d.get("macd_signal"); atr_v=d.get("atr")
            ac=d.get("analyst_count",0)

            # 操作列
            op1,op2,op3 = st.columns([2,2,6])
            wl_in = sel in st.session_state.watchlist
            if op1.button("★ 移出" if wl_in else "☆ 加入自選", use_container_width=True):
                if wl_in: st.session_state.watchlist.remove(sel)
                else: st.session_state.watchlist.append(sel)
                st.rerun()
            if op2.button("🔄 重新載入", use_container_width=True):
                fetch_stock.clear(); st.session_state.detail_cache={}; st.rerun()

            # vol/fh badges
            vol_badge = (
                '<span class="xbadge xb-vol-x">💥 爆量</span>' if vol_s=="extreme" else
                ('<span class="xbadge xb-vol-h">📈 放量</span>' if vol_s=="high" else "")
            )
            fh_badge = f'<span class="xbadge xb-fh{fhg}">財健 {fhg}</span>'
            alts_now = check_alerts(d)
            alt_html = "".join(f'<div style="font-family:var(--mono);font-size:.47rem;color:var(--y);margin-top:2px">⚠ {a}</div>' for a in alts_now[:3])

            # 火力分數顏色
            fs_col = "var(--g)" if fs>=4 else ("var(--y)" if fs>=2 else "var(--t2)")

            # STOCK HEADER CARD
            st.markdown(
                '<div class="scard"><div class="scard-top"><div class="scard-stripe"></div>'
                f'<div class="scard-id">'
                f'<div class="scard-code">{sel}<span class="scard-sfx">{suffix}</span></div>'
                f'<div class="scard-name">{name}</div>'
                f'<div class="scard-ind">{ind} · 市值 {fbil(mc)}</div>'
                f'<div class="scard-badges">{sig_badge(sig)}{fh_badge}{vol_badge}</div>'
                f'{alt_html}</div>'
                f'<div class="scard-px">'
                f'<div class="scard-price">{fp(px)}<span class="scard-unit">TWD</span></div>'
                f'<div class="scard-chg {chg_cls}">{chg_sym} {f"{abs(chg):.2f}" if chg else "—"}&nbsp;({f"{chgp:+.2f}%" if chgp else "—"})</div>'
                f'<div class="scard-ohlc">'
                f'<span>開 <span style="color:var(--t1)">{fp(d.get("open"))}</span></span>'
                f'<span>高 <span style="color:var(--g)">{fp(d.get("high"))}</span></span>'
                f'<span>低 <span style="color:var(--r)">{fp(d.get("low"))}</span></span>'
                f'<span>昨 <span style="color:var(--t1)">{fp(prev)}</span></span>'
                f'</div>'
                f'<div class="scard-meta">'
                f'<span>量能 <span style="color:{"var(--r)" if vol_s=="extreme" else ("var(--y)" if vol_s=="high" else "var(--t1)")}">{vol_r:.1f}x</span></span>'
                f'<span>Beta <span style="color:var(--t1)">{fp(beta,2) if beta else "—"}</span></span>'
                f'<span>夏普 <span style="color:var(--t1)">{fp(sharpe,2) if sharpe else "—"}</span></span>'
                f'<span>回撤 <span style="color:var(--r)">{f"{mdd:.1f}%" if mdd else "—"}</span></span>'
                f'</div>'
                f'<div class="fire-row">{fire_badges_html(fr)}</div>'
                f'</div>'
                f'<div class="scard-sig-block">'
                f'<div style="display:flex;align-items:center;gap:9px">'
                f'<div class="score-ring {shex(score)}">'
                f'<div class="score-n" style="color:{"var(--g)" if score>=70 else ("var(--y)" if score>=50 else "var(--r)")}">{score}</div>'
                f'<div class="score-lbl">評分</div></div>'
                f'<div>'
                f'<div style="font-family:var(--mono);font-size:.4rem;color:var(--t2);text-transform:uppercase;letter-spacing:.1em">綜合評分 /100</div>'
                f'<div style="font-family:var(--mono);font-size:.52rem;color:var(--t1);margin-top:3px">{"★"*int(score/20)}{"☆"*(5-int(score/20))}</div>'
                f'</div></div>'
                f'<div style="margin-top:6px">'
                f'<div style="font-family:var(--mono);font-size:.42rem;color:var(--t2)">🔥 點火分</div>'
                f'<div style="font-family:var(--mono);font-size:1.1rem;font-weight:700;color:{fs_col}">{fs}/8</div>'
                f'</div>'
                f'<div style="font-family:var(--mono);font-size:.5rem;color:var(--t2);margin-top:4px">'
                f'目標 <span style="color:var(--g);font-weight:700;font-size:.68rem">{fp(tp)}</span>'
                f'<span style="color:var(--g);margin-left:4px">{f"({up:+.1f}%)" if up is not None else ""}</span>'
                f'</div>'
                f'</div></div></div>',
                unsafe_allow_html=True
            )

            col_main, col_side = st.columns([5.5,3])

            with col_main:
                # ★ 點火訊號面板
                fi_items = [
                    ("ma_bull", "⚡ 均線剛形成多頭排列（MA5>10>20>60）", f'MA5={fp(ma5v)} MA10={fp(ma10v)} MA20={fp(ma20v)} MA60={fp(ma60v)}'),
                    ("vol_explosion", f'🔥 爆量長紅（量 {fr.get("vol_ratio",1):.1f}x 均量，收紅）', f'倍率閾值 {st.session_state.fire_params["vol_mult"]}x'),
                    ("ma_tangle", f'⊞ 均線糾集（差距 {fr.get("tangle_pct",0):.2f}%，閾值 {st.session_state.fire_params["tangle_pct"]}%）', '底部蓄力，即將表態'),
                    ("inst_buy", f'▲ 機構連買估算（近{fr.get("inst_streak",0)}日符合）', f'判斷天數 {st.session_state.fire_params["inst_days"]} 日'),
                    ("rsi_golden", f'RSI 黃金區 {fr.get("rsi_val",0):.1f}（35–62）', '未超買，動能空間充足'),
                    ("macd_cross", "MACD DIF>DEA（金叉/持續多頭）", f'DIF={fp(macd_v,3)} DEA={fp(macd_sv,3)}'),
                ]
                fi_html = "".join(
                    f'<div class="fi {"ok" if fr.get(k) else "no"}">'
                    f'<div class="fi-ic">{"✓" if fr.get(k) else "✗"}</div>'
                    f'<div><div class="fi-txt">{label}</div><div class="fi-sub">{sub}</div></div>'
                    f'</div>'
                    for k, label, sub in fi_items
                )
                st.markdown(
                    f'<div class="fire-panel">'
                    f'<div class="fire-panel-title">🔥 點火訊號分析 '
                    f'<span style="font-size:.44rem;color:var(--t2)">（{sum(1 for k,_,_ in fi_items if fr.get(k))}/{len(fi_items)} 個訊號觸發）</span>'
                    f'</div>'
                    f'<div style="display:flex;align-items:center;gap:14px">'
                    f'<div><div class="fire-score" style="color:{fs_col}">{fs}</div>'
                    f'<div style="font-family:var(--mono);font-size:.4rem;color:var(--t2);margin-top:2px">點火分 /8</div></div>'
                    f'<div style="flex:1"><div class="fire-items">{fi_html}</div></div>'
                    f'</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )

                # 基本面 12格
                cells12 = "".join([
                    dc("本益比 PE",  f"{pe:.1f}×" if pe else "—", "warn" if (pe and pe>20) else ("pos" if (pe and pe<15) else "")),
                    dc("淨值比 PB",  f"{pb:.2f}×" if pb else "—"),
                    dc("ROE",        fpc(roe), "pos" if (roe and roe>.12) else ("neg" if (roe and roe<0) else "")),
                    dc("殖利率",     fpc(dy), "pos" if (dy and dy>.04) else ""),
                    dc("淨利率",     fpc(pm), "pos" if (pm and pm>.1) else ""),
                    dc("營收成長",   fpc(rg), "pos" if (rg and rg>0) else ("neg" if (rg and rg<-.05) else "")),
                    dc("流動比率",   f"{cr:.2f}" if cr else "—", "pos" if (cr and cr>=2) else ("warn" if (cr and cr>=1) else "neg")),
                    dc("速動比率",   f"{qr:.2f}" if qr else "—", "pos" if (qr and qr>=1) else "warn"),
                    dc("負債比",     f"{de:.2f}" if de else "—", "pos" if (de is not None and de<.5) else ("warn" if (de is not None and de<1) else "neg")),
                    dc("RSI 14",    f"{rsi:.1f}" if rsi else "—", "neg" if (rsi and rsi>72) else ("warn" if (rsi and rsi>60) else ("pos" if (rsi and rsi<35) else "neu"))),
                    dc("目標價",     fp(tp), "pos"),
                    dc("上漲空間",   f"{up:+.1f}%" if up is not None else "—", "pos"),
                ])
                st.markdown(f'<div class="dg6">{cells12}</div>', unsafe_allow_html=True)

                # 技術數值 8格（新增 MA10）
                macd_ok = bool(macd_v and macd_sv and macd_v>macd_sv)
                cells8 = "".join([
                    dc("MACD/DIF",   f"{macd_v:.3f}" if macd_v else "—"),
                    dc("DEA/Signal", f"{macd_sv:.3f}" if macd_sv else "—"),
                    dc("MACD差值",   f"{(macd_v-macd_sv):+.3f}" if (macd_v and macd_sv) else "—", "pos" if macd_ok else "neg"),
                    dc("ATR 14",     f"{atr_v:.2f}" if atr_v else "—"),
                    dc("MA5",  fp(ma5v),  "pos" if (px and ma5v and px>ma5v) else "neg"),
                    dc("MA10", fp(ma10v), "pos" if (px and ma10v and px>ma10v) else "neg"),
                    dc("MA20", fp(ma20v), "pos" if (px and ma20v and px>ma20v) else "neg"),
                    dc("MA60", fp(ma60v), "pos" if (px and ma60v and px>ma60v) else "neg"),
                ])
                st.markdown(f'<div class="dg8">{cells8}</div>', unsafe_allow_html=True)

                # 圖表
                fig = make_chart(d)
                if fig:
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar":False})
                else:
                    st.markdown('<div style="font-family:var(--mono);font-size:.62rem;color:var(--t2);text-align:center;padding:20px">歷史資料不足</div>', unsafe_allow_html=True)

                # 三大法人
                fn=d.get("foreign_net"); tn=d.get("trust_net"); dn=d.get("dealer_net")
                if fn is not None:
                    max_abs = max(abs(fn or 0),abs(tn or 0),abs(dn or 0),1)
                    def inst_row_h(n_, v_):
                        if v_ is None: return ""
                        pct = min(abs(v_)/max_abs*44, 44)
                        cls_ = "inst-b" if v_>=0 else "inst-s"
                        vc_ = "pos" if v_>=0 else "neg"
                        bg_ = "background:var(--g)" if v_>=0 else "background:var(--r)"
                        return (f'<div class="inst-row"><div class="inst-name">{n_}</div>'
                                f'<div class="inst-wrap"><div class="{cls_}" style="width:{pct}%;{bg_}"></div></div>'
                                f'<div class="inst-val {vc_}">{v_:+.1f}億</div></div>')
                    st.markdown(
                        '<div class="panel"><div class="panel-title">三大法人動向估算（近5日·依價量推估）</div>'
                        + inst_row_h("外資",fn) + inst_row_h("投信",tn) + inst_row_h("自營商",dn)
                        + '<div style="font-family:var(--mono);font-size:.4rem;color:var(--t2);margin-top:5px">※ 非官方數據，僅供參考</div></div>',
                        unsafe_allow_html=True
                    )

                # 回測
                bt = d.get("backtest",{})
                if bt:
                    wr=bt.get("win_rate",0); ar=bt.get("avg_ret",0)
                    st.markdown(
                        '<div class="panel"><div class="panel-title">歷史信號回測（MA金叉策略 · 近一年）</div>'
                        '<div class="bt-grid">'
                        f'<div class="bt-cell"><div class="bt-k">交易次數</div><div class="bt-v">{bt.get("trades","—")}</div></div>'
                        f'<div class="bt-cell"><div class="bt-k">勝率</div><div class="bt-v {"pos" if wr>=55 else ("warn" if wr>=45 else "neg")}">{wr:.1f}%</div></div>'
                        f'<div class="bt-cell"><div class="bt-k">均報酬</div><div class="bt-v {"pos" if ar>0 else "neg"}">{ar:+.1f}%</div></div>'
                        f'<div class="bt-cell"><div class="bt-k">最大獲利</div><div class="bt-v pos">{bt.get("max_win",0):+.1f}%</div></div>'
                        f'<div class="bt-cell"><div class="bt-k">最大虧損</div><div class="bt-v neg">{bt.get("max_loss",0):+.1f}%</div></div>'
                        f'<div class="bt-cell"><div class="bt-k">最大回撤</div><div class="bt-v neg">{mdd:.1f}%</div></div>'
                        f'<div class="bt-cell"><div class="bt-k">夏普比率</div><div class="bt-v {"pos" if (sharpe and sharpe>1) else ""}">{fp(sharpe,2) if sharpe else "—"}</div></div>'
                        f'<div class="bt-cell"><div class="bt-k">Beta</div><div class="bt-v">{fp(beta,2) if beta else "—"}</div></div>'
                        '</div><div style="font-family:var(--mono);font-size:.4rem;color:var(--t2);margin-top:5px">※ 過去績效不代表未來</div></div>',
                        unsafe_allow_html=True
                    )

            with col_side:
                # 目標價
                if px and tp and tl_ and th_:
                    lo_b=min(tl_,px)*.93; hi_b=max(th_,px)*1.07
                    rng=hi_b-lo_b if hi_b>lo_b else 1
                    def pp_(v_): return max(0.,min(100.,(v_-lo_b)/rng*100))
                    p_px=pp_(px); p_tp=pp_(tp); p_tl=pp_(tl_); p_th=pp_(th_)
                    bw=p_th-p_tl
                    up_col="var(--g)" if (up and up>=0) else "var(--r)"
                    st.markdown(
                        '<div class="tp-panel">'
                        '<div class="panel-title">12 個月目標價估算</div>'
                        '<div class="tp-row">'
                        f'<div><div class="tp-lbl">目標低</div><div class="tp-val lo">{fp(tl_)}</div></div>'
                        f'<div><div class="tp-lbl">現價</div><div class="tp-val cur">{fp(px)}</div></div>'
                        f'<div style="text-align:center"><div class="tp-big {"pos" if (up and up>=0) else "neg"}">{f"{up:+.1f}%" if up is not None else "—"}</div><div class="tp-lbl" style="margin-top:2px">預期報酬</div></div>'
                        f'<div><div class="tp-lbl">目標價</div><div class="tp-val tp">{fp(tp)}</div></div>'
                        f'<div style="text-align:right"><div class="tp-lbl">目標高</div><div class="tp-val hi">{fp(th_)}</div></div>'
                        '</div>'
                        '<div class="tp-track">'
                        f'<div class="tp-zone" style="left:{p_tl:.1f}%;width:{bw:.1f}%"></div>'
                        f'<div class="tp-cur" style="left:{p_px:.1f}%"><div class="tp-lbl2" style="top:-15px;color:#fff;font-size:.4rem">現 {fp(px)}</div></div>'
                        f'<div class="tp-tp" style="left:{p_tp:.1f}%;background:{up_col};box-shadow:0 0 8px {up_col}88"><div class="tp-lbl2" style="top:13px;color:{up_col};font-size:.4rem">目 {fp(tp)}</div></div>'
                        '</div>'
                        f'<div style="font-family:var(--mono);font-size:.42rem;color:var(--t2);line-height:1.7">'
                        f'基礎溢價+基本面+技術{"+ 分析師（" + str(ac) + "人）" if ac>=3 else ""}<br>'
                        f'區間：{fp(tl_)} – {fp(th_)} · 覆蓋 {ac} 人</div>'
                        '</div>',
                        unsafe_allow_html=True
                    )

                # 評分明細
                det = d.get("score_detail",{})
                maxes = {"技術":30,"點火":15,"基本面":27,"動能":18,"財務健康":10}
                bars_html = ""
                for k, mx in maxes.items():
                    v_ = det.get(k,0); pct = int(v_/mx*100)
                    fc = "g" if pct>=65 else ("y" if pct>=35 else "r")
                    bars_html += (f'<div class="sbar"><div class="sbar-k">{k}</div>'
                                  f'<div class="sbar-track"><div class="sbar-fill {fc}" style="width:{pct}%"></div></div>'
                                  f'<div class="sbar-n">{v_}/{mx}</div></div>')
                st.markdown(f'<div class="panel"><div class="panel-title">評分明細 · 5維度</div>{bars_html}</div>', unsafe_allow_html=True)

                # 財務健康
                fh_colors={"A":"var(--g)","B":"var(--b)","C":"var(--y)","D":"var(--r)"}
                fh_descs={"A":"優良","B":"良好","C":"一般","D":"偏弱"}
                fhc=fh_colors.get(fhg,"var(--y)")
                st.markdown(
                    '<div class="panel"><div class="panel-title">財務健康評級</div>'
                    '<div class="fh-wrap">'
                    f'<div class="fh-ring {fhg}"><div class="fh-grade" style="color:{fhc}">{fhg}</div><div class="fh-score">{fhs}分</div></div>'
                    '<div style="flex:1">'
                    f'<div style="font-family:var(--mono);font-size:.56rem;font-weight:700;color:{fhc};margin-bottom:5px">{fh_descs.get(fhg,"")}</div>'
                    '<div class="fh-details">'
                    f'<div class="fh-dc"><div class="fh-dk">流動比</div><div class="fh-dv" style="color:{"var(--g)" if (cr and cr>=2) else ("var(--y)" if (cr and cr>=1) else "var(--r)")}">{f"{cr:.2f}" if cr else "—"}</div></div>'
                    f'<div class="fh-dc"><div class="fh-dk">速動比</div><div class="fh-dv" style="color:{"var(--g)" if (qr and qr>=1) else "var(--y)"}">{f"{qr:.2f}" if qr else "—"}</div></div>'
                    f'<div class="fh-dc"><div class="fh-dk">負債比</div><div class="fh-dv" style="color:{"var(--g)" if (de is not None and de<.5) else ("var(--y)" if (de is not None and de<1) else "var(--r)")}">{f"{de:.2f}" if de else "—"}</div></div>'
                    '</div></div></div></div>',
                    unsafe_allow_html=True
                )

                # 風險
                def rb(v_,lo_,hi_,col_):
                    if v_ is None: return ""
                    pct_=max(0,min(100,(v_-lo_)/(hi_-lo_)*100))
                    return f'<div class="risk-bar"><div class="risk-bar-fill" style="width:{pct_}%;background:{col_}"></div></div>'
                beta_col="var(--g)" if (beta and beta<1) else ("var(--y)" if (beta and beta<1.5) else "var(--r)")
                sha_col ="var(--g)" if (sharpe and sharpe>1) else ("var(--y)" if (sharpe and sharpe>0) else "var(--r)")
                st.markdown(
                    '<div class="panel"><div class="panel-title">風險指標</div>'
                    '<div class="risk-grid">'
                    f'<div class="risk-cell"><div class="risk-k">Beta 系統風險</div><div class="risk-v" style="color:{beta_col}">{f"{beta:.2f}" if beta else "—"}</div>{rb(beta,0,2,"var(--b)") if beta else ""}</div>'
                    f'<div class="risk-cell"><div class="risk-k">夏普比率</div><div class="risk-v" style="color:{sha_col}">{f"{sharpe:.2f}" if sharpe else "—"}</div>{rb(sharpe,-1,3,"var(--g)") if sharpe else ""}</div>'
                    f'<div class="risk-cell"><div class="risk-k">最大回撤</div><div class="risk-v" style="color:var(--r)">{f"{mdd:.1f}%" if mdd else "—"}</div>{rb(abs(mdd) if mdd else 0,0,50,"var(--r)") if mdd else ""}</div>'
                    '</div></div>',
                    unsafe_allow_html=True
                )

                # Pivot
                pv = d.get("pivot",{})
                if pv and px:
                    st.markdown(
                        '<div class="panel"><div class="panel-title">Pivot Point 支撐壓力</div>'
                        '<div class="pv-grid">'
                        f'<div class="pv-cell R"><div class="pv-k">R3</div><div class="pv-v">{fp(pv.get("R3"))}</div></div>'
                        f'<div class="pv-cell R"><div class="pv-k">R2</div><div class="pv-v">{fp(pv.get("R2"))}</div></div>'
                        f'<div class="pv-cell R"><div class="pv-k">R1</div><div class="pv-v">{fp(pv.get("R1"))}</div></div>'
                        f'<div class="pv-cell P" style="grid-column:span 3"><div class="pv-k">PP 樞紐</div><div class="pv-v">{fp(pv.get("PP"))}</div></div>'
                        f'<div class="pv-cell S"><div class="pv-k">S1</div><div class="pv-v">{fp(pv.get("S1"))}</div></div>'
                        f'<div class="pv-cell S"><div class="pv-k">S2</div><div class="pv-v">{fp(pv.get("S2"))}</div></div>'
                        f'<div class="pv-cell S"><div class="pv-k">S3</div><div class="pv-v">{fp(pv.get("S3"))}</div></div>'
                        '</div>'
                        f'<div style="font-family:var(--mono);font-size:.42rem;color:var(--t2);margin-top:5px">'
                        f'現價 {fp(px)} · {"PP 上方壓力區" if px>(pv.get("PP",0)) else "PP 下方支撐區"}</div>'
                        '</div>',
                        unsafe_allow_html=True
                    )

                # 信號解讀
                sig_map={"BUY":("強勢買入","技術多頭 + 點火訊號觸發 + 基本面健康，建議分批建倉。"),
                         "WATCH":("觀察等待","訊號逐步到位，等成交量與均線確認再進場。"),
                         "HOLD":("持有中立","趨勢不明，持倉繼續持有，空手暫緩。"),
                         "AVOID":("暫時迴避","指標偏弱或估值過高，等待更佳時機。")}
                si=sig_map.get(sig,("中立","—"))
                sc={"BUY":"var(--g)","WATCH":"var(--y)","HOLD":"var(--t1)","AVOID":"var(--r)"}.get(sig,"var(--t1)")
                st.markdown(f'<div class="sig-card {sig}"><div class="sig-card-t" style="color:{sc}">{sig} · {si[0]}</div><div class="sig-card-b">{si[1]}</div></div>', unsafe_allow_html=True)

                # Checklist
                macd_ok2=bool(macd_v and macd_sv and macd_v>macd_sv)
                checks=[
                    ("現價>MA5",   bool(px and ma5v and px>ma5v)),
                    ("現價>MA10",  bool(px and ma10v and px>ma10v)),
                    ("現價>MA20",  bool(px and ma20v and px>ma20v)),
                    ("現價>MA60",  bool(px and ma60v and px>ma60v)),
                    ("均線多頭排列",fr.get("ma_bull_cont",False)),
                    ("均線剛排列", fr.get("ma_bull",False)),
                    ("爆量長紅",   fr.get("vol_explosion",False)),
                    ("均線糾集",   fr.get("ma_tangle",False)),
                    ("機構連買",   fr.get("inst_buy",False)),
                    ("MACD 金叉",  macd_ok2),
                    ("RSI 黃金區", fr.get("rsi_golden",False)),
                    ("PE ≤20",     bool(pe and pe<=20)),
                    ("殖利率>3%",  bool(dy and dy>.03)),
                    ("ROE>12%",    bool(roe and roe>.12)),
                    ("財務健康A/B",fhg in("A","B")),
                ]
                ok_n=sum(1 for _,ok in checks if ok)
                chk_html="".join(
                    f'<div class="chk {"ok" if ok else "no"}"><span class="chk-ic">{"✓" if ok else "✗"}</span><span class="chk-txt">{lbl}</span></div>'
                    for lbl,ok in checks
                )
                st.markdown(f'<div class="panel"><div class="panel-title">條件核對 · {ok_n}/{len(checks)} 通過</div>{chk_html}</div>', unsafe_allow_html=True)

            # 新聞
            with st.expander("📰  相關新聞 · 情緒分析", expanded=True):
                news=fetch_news(sel,name)
                if news:
                    pos_n=sum(1 for n in news if n.get("s")=="pos"); neg_n=sum(1 for n in news if n.get("s")=="neg")
                    sent_txt="偏多 📈" if pos_n>neg_n else ("偏空 📉" if neg_n>pos_n else "中性 ➡")
                    sent_col="var(--g)" if pos_n>neg_n else ("var(--r)" if neg_n>pos_n else "var(--t2)")
                    st.markdown(f'<div style="font-family:var(--mono);font-size:.48rem;color:var(--t2);margin-bottom:5px">情緒 <span style="color:{sent_col};font-weight:700">{sent_txt}</span> · 正{pos_n} 負{neg_n}</div>', unsafe_allow_html=True)
                    html="".join(
                        f'<div class="news-item"><div class="news-ic {n.get("s","neu")}">{"↑" if n.get("s")=="pos" else ("↓" if n.get("s")=="neg" else "·")}</div>'
                        f'<div><div class="news-t">{n["t"]}</div><div class="news-m">{n.get("src","")}</div></div></div>'
                        for n in news
                    )
                    st.markdown(html, unsafe_allow_html=True)
                else:
                    st.markdown('<div style="font-family:var(--mono);font-size:.6rem;color:var(--t2);padding:8px 0">暫無新聞</div>', unsafe_allow_html=True)

            # 警示設定
            with st.expander("🔔  自訂警示設定", expanded=False):
                cfg=st.session_state.alert_cfg.get(sel,{})
                ac1_,ac2_=st.columns(2)
                pa_=ac1_.number_input("突破價位(0=停用)",min_value=0.0,value=float(cfg.get("price_above") or 0),step=1.0,key=f"pa_{sel}")
                pb__=ac2_.number_input("跌破價位(0=停用)",min_value=0.0,value=float(cfg.get("price_below") or 0),step=1.0,key=f"pbb_{sel}")
                if st.button("儲存警示",key=f"sv_{sel}"):
                    st.session_state.alert_cfg[sel]={"price_above":pa_ if pa_>0 else None,"price_below":pb__ if pb__>0 else None}
                    st.success(f"{sel} 警示已設定 ✓")

# ════════════════════════ TAB 2 ════════════════════════
with tab2:
    op1_,op2_,op3_ = st.columns([3.5,1.5,1.5])
    with op1_:
        if st.button("⚡  立即掃描", type="primary", use_container_width=True, key="scan_now"):
            if scan_mode=="熱門100":  c2s=list(ALL.keys())[:100]
            elif scan_mode=="自選股": c2s=list(st.session_state.watchlist)
            elif scan_mode=="全市場": c2s=list(ALL.keys())
            else: c2s=[x.strip() for x in re.split(r"[,\n\s]+",custom_codes) if x.strip()]
            if not c2s:
                st.warning("請設定掃描範圍")
            else:
                ph=st.empty(); txh=st.empty()
                def _prog(done,total,code):
                    ph.progress(done/total)
                    txh.markdown(f'<div class="ll inf">⚡ [{done}/{total}] {code} {ALL.get(code,"")}</div>', unsafe_allow_html=True)
                with st.spinner(""):
                    res_new=scan_batch(c2s,min_score=min_score,min_upside=min_upside,max_pe=max_pe,
                                       signal_filter=sig_filter,min_fire=min_fire,
                                       require_ma_bull=req_ma_bull,require_vol_exp=req_vol_exp,
                                       require_tangle=req_tangle,require_inst=req_inst,
                                       progress_cb=_prog,max_workers=16)
                ph.empty(); txh.empty()
                st.session_state.scan_results=res_new
                st.session_state.last_scan_time=datetime.datetime.now()
                all_alts=[]
                for d_ in res_new: all_alts.extend(check_alerts(d_))
                if all_alts: st.session_state.alerts=all_alts[-20:]
                fire_count=sum(1 for r in res_new if r.get("fire_score",0)>=4)
                st.success(f"✓ {len(c2s)} 檔 · 命中 {len(res_new)} 檔 · 🔥高點火 {fire_count} 檔 · 警示 {len(all_alts)} 條")
    with op2_:
        if st.button("📤 Discord", use_container_width=True, key="push_dc"):
            if not st.session_state.scan_results: st.warning("無掃描結果")
            elif not webhook: st.warning("請填入 Webhook")
            else: (st.success("✓") if push_discord(webhook,st.session_state.scan_results) else st.error("✗"))
    with op3_:
        res_now=st.session_state.scan_results
        if res_now:
            st.download_button("📥 CSV", data=results_to_csv(res_now),
                               file_name=f"scan_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                               mime="text/csv",use_container_width=True,key="dl_csv")

    res=st.session_state.scan_results
    if res:
        buy_n=sum(1 for r in res if r.get("signal")=="BUY")
        wch_n=sum(1 for r in res if r.get("signal")=="WATCH")
        hld_n=sum(1 for r in res if r.get("signal")=="HOLD")
        avd_n=sum(1 for r in res if r.get("signal")=="AVOID")
        avg_sc=int(np.mean([r.get("score",0) for r in res]))
        ups=[r.get("upside") or 0 for r in res]
        avg_up=np.mean(ups); top_up=max(ups)
        fh_a=sum(1 for r in res if r.get("fin_health_grade")=="A")
        fire_hi=sum(1 for r in res if r.get("fire_score",0)>=4)
        avg_fire=round(np.mean([r.get("fire_score",0) for r in res]),1)

        st.markdown(
            '<div class="kpi-row">'
            f'<div class="kpi g"><div class="kpi-l">BUY</div><div class="kpi-v g">{buy_n}</div><div class="kpi-d">強買</div></div>'
            f'<div class="kpi y"><div class="kpi-l">WATCH</div><div class="kpi-v y">{wch_n}</div><div class="kpi-d">觀察</div></div>'
            f'<div class="kpi w"><div class="kpi-l">HOLD</div><div class="kpi-v">{hld_n}</div><div class="kpi-d">持有</div></div>'
            f'<div class="kpi r"><div class="kpi-l">AVOID</div><div class="kpi-v r">{avd_n}</div><div class="kpi-d">迴避</div></div>'
            f'<div class="kpi o"><div class="kpi-l">🔥高點火</div><div class="kpi-v o">{fire_hi}</div><div class="kpi-d">≥4分</div></div>'
            f'<div class="kpi b"><div class="kpi-l">平均評分</div><div class="kpi-v b">{avg_sc}</div><div class="kpi-d">/100</div></div>'
            f'<div class="kpi p"><div class="kpi-l">平均上漲</div><div class="kpi-v p">{avg_up:+.1f}%</div><div class="kpi-d">預期</div></div>'
            f'<div class="kpi g"><div class="kpi-l">財健A級</div><div class="kpi-v g">{fh_a}</div><div class="kpi-d">檔</div></div>'
            '</div>',
            unsafe_allow_html=True
        )

        rows_html=""
        for r in res:
            cd=r.get("code",""); nm=r.get("name",cd)
            pv_=r.get("price"); tv_=r.get("target_price"); uv_=r.get("upside")
            sc_=r.get("score",0); sg_=r.get("signal","HOLD")
            pev_=r.get("pe"); rov_=r.get("roe"); dyv_=r.get("dividend_yield")
            rsv_=r.get("rsi"); rgv_=r.get("revenue_growth")
            fr_=r.get("fire",{}); fs_=r.get("fire_score",0)
            fhg_=r.get("fin_health_grade","—"); vr_=r.get("volume_ratio",1.0); vs_=r.get("volume_status","normal")
            ma20v_=r.get("ma20"); macd_=r.get("macd"); macds_=r.get("macd_signal")
            macd_ok_=bool(macd_ and macds_ and macd_>macds_)
            up_td="c-up" if (uv_ and uv_>=0) else "c-dn"
            fhg_c={"A":"c-pos","B":"c-neu","C":"c-warn","D":"c-neg"}.get(fhg_,"c-dim")
            vc_="c-neg" if vs_=="extreme" else ("c-warn" if vs_=="high" else "c-dim")
            sc_col_="var(--g)" if sc_>=70 else ("var(--y)" if sc_>=50 else "var(--r)")
            fs_c=fire_cls(fs_)

            # 點火指示格
            fire_icons=(
                ("🔥" if fr_.get("ma_bull") else ("✓" if fr_.get("ma_bull_cont") else "·"))
                +(" 量" if fr_.get("vol_explosion") else "")
                +(" 糾" if fr_.get("ma_tangle") else "")
                +(" 機" if fr_.get("inst_buy") else "")
            )
            rows_html+=(
                f'<tr class="{sg_}">'
                f'<td><div class="score-ring {shex(sc_)}" style="width:28px;height:28px;border-width:1.5px">'
                f'<div class="score-n" style="font-size:.58rem;color:{sc_col_}">{sc_}</div></div></td>'
                f'<td class="c-pri"><div>{cd}</div><div style="font-size:.48rem;color:var(--t2)">{nm[:7]}</div></td>'
                f'<td>{sig_badge(sg_)}</td>'
                f'<td><div class="{fs_c} fire-score-sm">{fs_}</div></td>'
                f'<td style="font-family:var(--mono);font-size:.52rem;color:var(--t2)">{fire_icons}</td>'
                f'<td class="c-pri">{fp(pv_)}</td>'
                f'<td class="c-tp">{fp(tv_)}</td>'
                f'<td class="{up_td}">{f"{uv_:+.1f}%" if uv_ is not None else "—"}</td>'
                f'<td class="c-dim">{f"{pev_:.1f}x" if pev_ else "—"}</td>'
                f'<td class="{"c-pos" if (rov_ and rov_>.12) else "c-dim"}">{fpc(rov_)}</td>'
                f'<td class="{"c-pos" if (dyv_ and dyv_>.04) else "c-dim"}">{fpc(dyv_)}</td>'
                f'<td class="{"c-neg" if (rsv_ and rsv_>70) else ("c-pos" if (rsv_ and rsv_<35) else "c-dim")}">{f"{rsv_:.0f}" if rsv_ else "—"}</td>'
                f'<td class="{"c-pos" if (rgv_ and rgv_>.1) else "c-dim"}">{fpc(rgv_)}</td>'
                f'<td class="{"c-pos" if macd_ok_ else "c-dim"}">{"金叉↑" if macd_ok_ else "—"}</td>'
                f'<td class="{fhg_c}">{fhg_}</td>'
                f'<td class="{vc_}">{f"{vr_:.1f}x" if vr_ else "—"}</td>'
                f'</tr>'
            )

        st.markdown(
            '<div class="rt-wrap"><table class="rt"><thead><tr>'
            '<th>分</th><th>個股</th><th>信號</th>'
            '<th>🔥</th><th>訊號</th>'
            '<th>現價</th><th>目標</th><th>上漲</th>'
            '<th>PE</th><th>ROE</th><th>殖利率</th>'
            '<th>RSI</th><th>成長</th><th>MACD</th>'
            '<th>財健</th><th>量能</th>'
            f'</tr></thead><tbody>{rows_html}</tbody></table></div>',
            unsafe_allow_html=True
        )
        # 快速跳轉
        st.markdown('<hr><div style="font-family:var(--mono);font-size:.44rem;color:var(--t2);text-transform:uppercase;letter-spacing:.14em;margin-bottom:5px">// 快速跳轉 TOP 10（依點火+評分排序）</div>', unsafe_allow_html=True)
        top10=res[:10]
        if top10:
            jcols=st.columns(len(top10))
            for jcol,r in zip(jcols,top10):
                nm_s=r.get("name",r["code"])[:4]; sc__=r.get("score",0); fs__=r.get("fire_score",0)
                with jcol:
                    if st.button(f"{r['code']}\n{nm_s}\n{sc__}分 🔥{fs__}", use_container_width=True, key=f"jmp_{r['code']}"):
                        st.session_state.selected_stock=r["code"]
                        st.session_state.detail_cache={}
                        st.rerun()
    else:
        st.markdown(
            '<div class="empty"><div class="empty-ico">🔍</div>'
            '<div class="empty-txt">點擊「立即掃描」開始篩選<br>'
            '🔥 三大點火濾網：均線多頭排列 · 爆量長紅 · 均線糾集<br>'
            '+ 機構連買估算 · RSI黃金區 · MACD金叉<br>'
            'CSV匯出包含完整點火訊號欄位</div></div>',
            unsafe_allow_html=True
        )

# ════════════════════════ TAB 3 ════════════════════════
with tab3:
    res_d=st.session_state.scan_results
    if not res_d:
        st.markdown('<div class="empty"><div class="empty-ico">🎯</div><div class="empty-txt">請先執行批量掃描</div></div>', unsafe_allow_html=True)
    else:
        buy_n_=sum(1 for r in res_d if r.get("signal")=="BUY")
        wch_n_=sum(1 for r in res_d if r.get("signal")=="WATCH")
        avd_n_=sum(1 for r in res_d if r.get("signal")=="AVOID")
        total_=len(res_d)
        bull_pct=int((buy_n_+wch_n_*.5)/total_*100) if total_>0 else 50
        bear_pct=100-bull_pct
        st.markdown(
            '<div class="bb-panel"><div class="panel-title">市場多空力道</div>'
            f'<div style="display:flex;justify-content:space-between;font-family:var(--mono);font-size:.48rem;color:var(--t2);margin-bottom:4px">'
            f'<span style="color:var(--g)">多方 {bull_pct}%  BUY {buy_n_} · WATCH {wch_n_}</span>'
            f'<span style="color:var(--r)">空方 {bear_pct}%  AVOID {avd_n_}</span></div>'
            f'<div class="bb-gauge"><div class="bb-fill" style="width:{bull_pct}%"></div></div></div>',
            unsafe_allow_html=True
        )
        ch1,ch2=st.columns(2)
        with ch1:
            scores_=[r.get("score",0) for r in res_d]
            sigs_=[r.get("signal","HOLD") for r in res_d]
            cm={"BUY":"#00e87a","WATCH":"#ffd60a","HOLD":"#7aaac8","AVOID":"#ff2d55"}
            colors_=[cm.get(s,"#7aaac8") for s in sigs_]
            codes_=[r.get("code","") for r in res_d]
            names_=[r.get("name",r.get("code",""))[:4] for r in res_d]
            fig_bar=go.Figure(go.Bar(x=[f"{c_}<br>{n_}" for c_,n_ in zip(codes_,names_)],y=scores_,
                marker_color=colors_,marker_opacity=.85,text=[str(s) for s in scores_],textposition="outside",
                textfont=dict(family="JetBrains Mono",size=9,color="#375872")))
            fig_bar.update_layout(paper_bgcolor="#02060f",plot_bgcolor="#02060f",
                font=dict(family="JetBrains Mono",size=9,color="#375872"),
                margin=dict(l=20,r=10,t=30,b=60),height=280,
                title=dict(text="評分分布",font=dict(size=11,color="#7aaac8"),x=0),
                xaxis=dict(tickfont=dict(size=7),tickangle=-45,gridcolor="#0c1b30"),
                yaxis=dict(range=[0,112],gridcolor="#0c1b30",tickfont=dict(size=9)),showlegend=False)
            st.plotly_chart(fig_bar,use_container_width=True,config={"displayModeBar":False})
        with ch2:
            ups_=[r.get("upside") or 0 for r in res_d]
            fire_sizes=[max(6,min(20,r.get("fire_score",0)*3+6)) for r in res_d]
            fig_sc=go.Figure(go.Scatter(x=scores_,y=ups_,mode="markers+text",text=codes_,
                textposition="top center",textfont=dict(family="JetBrains Mono",size=8,color="#375872"),
                marker=dict(size=fire_sizes,color=colors_,opacity=.85,line=dict(color="#02060f",width=1)),
                hovertemplate="<b>%{text}</b><br>評分:%{x}<br>上漲:%{y:.1f}%<extra></extra>"))
            fig_sc.add_hline(y=8,line_dash="dot",line_color="rgba(0,232,122,.2)",line_width=1)
            fig_sc.add_vline(x=70,line_dash="dot",line_color="rgba(0,232,122,.2)",line_width=1)
            fig_sc.update_layout(paper_bgcolor="#02060f",plot_bgcolor="#02060f",
                font=dict(family="JetBrains Mono",size=9,color="#375872"),
                margin=dict(l=40,r=10,t=30,b=30),height=280,
                title=dict(text="評分 vs 上漲（泡泡大小=點火分）",font=dict(size=11,color="#7aaac8"),x=0),
                xaxis=dict(title="評分",gridcolor="#0c1b30",tickfont=dict(size=9)),
                yaxis=dict(title="上漲空間%",gridcolor="#0c1b30",tickfont=dict(size=9)),showlegend=False)
            st.plotly_chart(fig_sc,use_container_width=True,config={"displayModeBar":False})

        # Top lists
        top_fire=[r for r in sorted(res_d,key=lambda x:x.get("fire_score",0),reverse=True) if r.get("fire_score",0)>=3][:8]
        top_buy_=[r for r in res_d if r.get("signal")=="BUY"][:8]
        cb1,cb2=st.columns(2)
        def rank_card(r,bc,tc):
            up__=r.get("upside") or 0; nm__=r.get("name",r.get("code","")); sc__=r.get("score",0); fs__=r.get("fire_score",0)
            sc_c__="var(--g)" if sc__>=70 else ("var(--y)" if sc__>=50 else "var(--r)")
            return (f'<div class="rank-card" style="border-color:{bc};border-left-color:{tc}">'
                    f'<div><span style="font-family:var(--mono);font-size:.74rem;font-weight:700;color:var(--t0)">{r["code"]}</span>'
                    f'<span style="font-size:.58rem;color:var(--t2);margin-left:6px">{nm__[:6]}</span></div>'
                    f'<div style="text-align:right"><div style="font-family:var(--mono);font-size:.68rem;color:var(--t0)">{fp(r.get("price"))}</div>'
                    f'<div style="font-family:var(--mono);font-size:.56rem;color:{tc}">{up__:+.1f}% → {fp(r.get("target_price"))}</div></div>'
                    f'<div style="font-family:var(--mono);font-size:.6rem;font-weight:700;color:{sc_c__};'
                    f'background:var(--bg2);border:1px solid var(--ln2);border-radius:4px;'
                    f'width:26px;height:26px;display:flex;align-items:center;justify-content:center;margin-left:6px">{sc__}</div>'
                    f'<div style="font-family:var(--mono);font-size:.56rem;color:var(--o);margin-left:4px">🔥{fs__}</div>'
                    f'</div>')
        with cb1:
            st.markdown('<div style="font-family:var(--mono);font-size:.48rem;color:var(--o);text-transform:uppercase;letter-spacing:.14em;margin-bottom:6px">🔥 高點火訊號（≥3分）</div>', unsafe_allow_html=True)
            for r in top_fire: st.markdown(rank_card(r,"rgba(255,123,0,.2)","var(--o)"),unsafe_allow_html=True)
        with cb2:
            st.markdown('<div style="font-family:var(--mono);font-size:.48rem;color:var(--g);text-transform:uppercase;letter-spacing:.14em;margin-bottom:6px">🟢 TOP BUY 強買</div>', unsafe_allow_html=True)
            for r in top_buy_: st.markdown(rank_card(r,"rgba(0,232,122,.2)","var(--g)"),unsafe_allow_html=True)
        # 財務健康
        st.markdown('<hr>', unsafe_allow_html=True)
        fh_c={"A":0,"B":0,"C":0,"D":0}
        for r in res_d:
            g_=r.get("fin_health_grade","C")
            if g_ in fh_c: fh_c[g_]+=1
        fh_cols=st.columns(4)
        fh_info={"A":("var(--g)","優良"),"B":("var(--b)","良好"),"C":("var(--y)","一般"),"D":("var(--r)","偏弱")}
        for i,(grade,cnt) in enumerate(fh_c.items()):
            col_,desc_=fh_info.get(grade,("var(--t2)","—"))
            fh_cols[i].markdown(
                f'<div style="background:var(--bg1);border:1px solid var(--ln2);border-radius:6px;padding:12px;text-align:center">'
                f'<div style="font-family:var(--mono);font-size:1.7rem;font-weight:700;color:{col_}">{grade}</div>'
                f'<div style="font-family:var(--mono);font-size:.44rem;color:var(--t2)">{desc_}</div>'
                f'<div style="font-family:var(--mono);font-size:.95rem;font-weight:700;color:{col_};margin-top:4px">{cnt}</div>'
                f'<div style="font-family:var(--mono);font-size:.42rem;color:var(--t2)">檔</div></div>',
                unsafe_allow_html=True
            )

# ════════════════════════ TAB 4 ════════════════════════
with tab4:
    cc1_,cc2_=st.columns([5,1])
    with cc1_: st.markdown('<div style="font-family:var(--mono);font-size:.46rem;color:var(--t2);text-transform:uppercase;letter-spacing:.18em;margin-bottom:7px">// SCHEDULER LOG · v10</div>', unsafe_allow_html=True)
    with cc2_:
        if st.button("CLR",use_container_width=True,key="clr_log"): st.session_state.sched_log=[]; st.rerun()
    log=st.session_state.sched_log
    if log:
        st.markdown('<div class="logbox">'+"".join(f'<div class="ll {t}">{m}</div>' for t,m in log)+'</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div style="font-family:var(--mono);font-size:.6rem;color:var(--t2);padding:18px 0">尚無排程紀錄 · 啟動排程後此處顯示執行紀錄</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
if st.session_state.sched_running:
    time.sleep(1); st.rerun()
