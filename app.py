"""台風データベース（1ファイル版・気象庁ベストトラック準拠）
  ビルド:  python app.py build      # IBTrACSを取得して typhoon.db を作成
  起動:    uvicorn app:app --host 0.0.0.0 --port $PORT   (DBが無ければ起動時に自動ビルド)
表記: 「2026年台風第26号 Surigae」。号数は気象庁方式（熱帯低気圧の段階は数えず、
      台風の強さに初めて達した順に年ごとに採番）で再計算する。
"""
import asyncio, csv, io, os, re, sqlite3, sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import requests
from concurrent.futures import ThreadPoolExecutor
from fastapi.middleware.gzip import GZipMiddleware
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

URL = ("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/"
       "v04r01/access/csv/ibtracs.WP.list.v04r01.csv")
SRC = os.environ.get("SOURCE_CSV")  # ローカルテスト用
JMA = "https://www.data.jma.go.jp/typhoon"
F = "%Y-%m-%d %H:%M:%S"
DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "typhoon.db")

# 気象庁が正式に名称を付けた台風（年, 国際名）
NICK = {(1954, "MARIE"): "洞爺丸台風", (1958, "IDA"): "狩野川台風", (1959, "SARAH"): "宮古島台風",
        (1959, "VERA"): "伊勢湾台風", (1961, "NANCY"): "第2室戸台風", (1966, "CORA"): "第2宮古島台風",
        (2019, "FAXAI"): "令和元年房総半島台風", (2019, "HAGIBIS"): "令和元年東日本台風"}

def title(year, no, en):
    s = f"{year}年台風第{no}号"
    if en: s += f" {en.title()}"
    if (year, en) in NICK: s += f"（{NICK[(year, en)]}）"
    return s

# ---------------- DB作成 ----------------
def num(v):
    try:
        f = float(v)
        return f if f > -900 else None
    except (TypeError, ValueError):
        return None

def jst(iso):
    return datetime.strptime(iso[:19], "%Y-%m-%d %H:%M:%S") + timedelta(hours=9)

NEED = ("SID", "NAME", "ISO_TIME", "TRACK_TYPE", "TOKYO_LAT", "TOKYO_LON",
        "TOKYO_GRADE", "TOKYO_WIND", "TOKYO_PRES")

def load_rows():
    """CSVを流し読みし、必要な列だけの辞書を返す（列番号で読むのでDictReaderより高速）。"""
    def lines():
        if SRC:
            with open(SRC, encoding="utf-8") as f:
                yield from (ln.rstrip("\n") for ln in f)
        else:
            print("Downloading", URL, flush=True)
            with requests.get(URL, stream=True, timeout=300) as r:
                r.raise_for_status()
                r.encoding = "utf-8"
                yield from r.iter_lines(chunk_size=1 << 20, decode_unicode=True)
    it = lines()
    head = next(csv.reader([next(it)]))
    next(it)  # 2行目は単位行
    idx = [(k, head.index(k)) for k in NEED if k in head]
    last = max(i for _, i in idx)
    # SIDは先頭が西暦。気象庁の号数は1951年からなので、それ以前は読み飛ばす
    for row in csv.reader(ln for ln in it if ln[:4] >= "1951"):
        if len(row) > last:
            yield {k: row[i] for k, i in idx}

def insert(con, sid, year, no, en, p, prov=0):
    """p: [(time_utc, lat, lon, wind_kt, pres_hPa), ...] を1台風ぶんDBへ書き込む。"""
    ws = [x[3] for x in p if x[3]]; ps = [x[4] for x in p if x[4]]
    t0, t1 = (datetime.strptime(p[i][0][:19], F) for i in (0, -1))
    con.execute("INSERT OR REPLACE INTO typhoons VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (sid, year, no, title(year, no, en), en, p[0][0], p[-1][0], max(ws) if ws else None,
                 min(ps) if ps else None, (t0 + timedelta(hours=9)).month,
                 round((t1 - t0).total_seconds() / 86400, 1), prov))
    con.executemany("INSERT INTO points VALUES(?,?,?,?,?,?)", [(sid, *x[:5]) for x in p])

def build():
    tmp = DB + ".tmp"  # 失敗しても既存DBを壊さないよう、一時ファイルに作って最後に置換
    if os.path.exists(tmp):
        os.remove(tmp)
    con = sqlite3.connect(tmp)
    con.executescript("""
    CREATE TABLE typhoons(sid TEXT PRIMARY KEY, year INT, number INT, title TEXT, name_en TEXT,
        start_time TEXT, end_time TEXT, max_wind REAL, min_pres REAL, month INT, days REAL, prov INT);
    CREATE TABLE points(sid TEXT, time TEXT, lat REAL, lon REAL, wind REAL, pres REAL);
    CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT);
    """)
    con.executescript("PRAGMA synchronous=OFF; PRAGMA journal_mode=OFF;")  # ビルド専用の一時DBなので高速化
    metas = []

    def flush(m, p):
        # 気象庁の階級が「台風の強さ(TS以上)」に一度でも達したものだけ採用（気象庁の台風の定義）
        ts = [x[5] for x in p]
        if any(ts):
            metas.append(dict(m, t0=jst(p[ts.index(True)][0]), p=[x[:5] for x in p]))

    cur, meta, pts = None, None, []  # IBTrACSはSIDごとに連続して並んでいる
    for r in load_rows():
        if r.get("TRACK_TYPE") == "spur-track":
            continue
        lat, lon = num(r.get("TOKYO_LAT")), num(r.get("TOKYO_LON"))  # 気象庁の観測点のみ（補間点を除外）
        if lat is None or lon is None:
            continue
        sid = r["SID"]
        if sid != cur:
            if cur is not None:
                flush(meta, pts)
            name = r["NAME"].strip().upper()
            meta = dict(sid=sid, en="" if name in ("NOT_NAMED", "UNNAMED") else name)
            cur, pts = sid, []
        wind, pres = num(r.get("TOKYO_WIND")), num(r.get("TOKYO_PRES"))
        is_ts = (num(r.get("TOKYO_GRADE")) in (3, 4, 5, 7)) or (wind is not None and wind >= 34)
        pts.append((r["ISO_TIME"], lat, lon, wind or None, pres or None, is_ts))
    if cur is not None:
        flush(meta, pts)

    # 号数: TSの強さに初めて達した日時(JST)の順に、年ごとに1から採番
    metas.sort(key=lambda m: m["t0"])
    seq = {}
    for m in metas:
        y = m["t0"].year
        seq[y] = seq.get(y, 0) + 1
        insert(con, m["sid"], y, seq[y], m["en"], m["p"])
    con.executescript("CREATE INDEX idx_points_sid ON points(sid); CREATE INDEX idx_ty_year ON typhoons(year, number);")
    con.execute("INSERT INTO meta VALUES('built_at',?)", (datetime.now().strftime("%Y-%m-%d"),))
    con.commit(); con.close()
    os.replace(tmp, DB)
    print(f"Done: {len(metas)} typhoons（今年の速報値は起動後に自動で取り込みます）")

# ---------------- 気象庁の速報値（今年ぶん） ----------------
HEAD = re.compile(r"(\d{4})年台風第\s*(\d+)号\s+([A-Za-z][A-Za-z\-]*)")
ROW = re.compile(r"^(?:(\d{1,2})\s+)?(?:(\d{1,2})\s+)?(\d{1,2})\s+(\d+\.\d)(?:\s+N)?\s+(\d+\.\d)(?:\s+E)?\s+(\d{3,4}|--)\s+(\d{1,2}|--)(?=\s|$)")

def parse_pdf(text):
    """気象庁の台風位置表PDF（日本時・風速m/s）→ (年, 号数, 名前, 点列(UTC・kt), 速報か)"""
    h = HEAD.search(text)
    if not h:
        return None
    year, no, name = int(h[1]), int(h[2]), h[3].upper()
    mo = da = None; pts = []
    for ln in text.splitlines():
        r = ROW.match(ln.strip())
        if not r:
            continue
        ints = [int(x) for x in r.groups()[:3] if x]
        if len(ints) == 3:
            if mo and ints[0] < mo: year += 1
            mo, da, hr = ints
        elif len(ints) == 2:
            da, hr = ints
        else:
            hr = ints[0]
        if mo is None or da is None:
            continue
        t = datetime(year, mo, da) + timedelta(hours=hr - 9)  # JST→UTC
        pres = None if r[6] == "--" else float(r[6])
        wind = None if r[7] == "--" else round(int(r[7]) * 1.94384)  # m/s→kt
        pts.append((t.strftime(F), float(r[4]), float(r[5]), wind, pres))
    return (year, no, name, pts, "速報値" in text) if pts else None

def fetch_pdf(c):
    import pdfplumber
    b = requests.get(f"{JMA}/data/T{c}.pdf", timeout=60).content
    with pdfplumber.open(io.BytesIO(b)) as pdf:
        return c, parse_pdf("\n".join(pg.extract_text() or "" for pg in pdf.pages))

def refresh_recent():
    """今年の台風を気象庁の位置表PDFから取り込む。確定済みで取得済みのものは再取得しない。"""
    con = None
    try:
        yr = datetime.now().year
        html = requests.get(f"{JMA}/position_table/table{yr}.html", timeout=60).text
        codes = sorted({c for c in re.findall(r"T(\d{4})\.pdf", html) if c[:2] == str(yr)[2:]})
        if not codes:
            return
        con = sqlite3.connect(DB, timeout=60)
        done = {r[0] for r in con.execute("SELECT sid FROM typhoons WHERE sid LIKE 'JMA%' AND prov=0")}
        todo = [c for c in codes if f"JMA{c}" not in done]
        with ThreadPoolExecutor(4) as ex:
            got = [g for g in ex.map(fetch_pdf, todo) if g[1]]
        if got:  # IBTrACS由来の今年ぶんは気象庁の値で置き換える
            con.execute("DELETE FROM points WHERE sid IN (SELECT sid FROM typhoons WHERE year=? AND sid NOT LIKE 'JMA%')", (yr,))
            con.execute("DELETE FROM typhoons WHERE year=? AND sid NOT LIKE 'JMA%'", (yr,))
        for c, (y, no, en, pts, prov) in got:
            con.execute("DELETE FROM points WHERE sid=?", (f"JMA{c}",))
            insert(con, f"JMA{c}", y, no, en, pts, int(prov))
        con.execute("INSERT OR REPLACE INTO meta VALUES('jma_at',?)", (datetime.now(timezone.utc).strftime(F),))
        con.commit()
        print(f"JMA {yr}: {len(got)}/{len(todo)} fetched", flush=True)
    except Exception as e:  # 速報の取得に失敗しても既存データで動かす
        print("refresh_recent failed:", repr(e), flush=True)
    finally:
        if con: con.close()

# ---------------- API ----------------
@asynccontextmanager
async def lifespan(_):
    if not os.path.exists(DB):
        await asyncio.to_thread(build)

    async def loop():
        while True:  # 速報値は3時間ごとに再取得
            await asyncio.to_thread(refresh_recent)
            await asyncio.sleep(3 * 3600)
    task = asyncio.create_task(loop())
    yield
    task.cancel()

app = FastAPI(title="Typhoon DB", lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)

def q(sql, args=()):
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, args)]
    finally:
        con.close()

@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE

@app.get("/api/years")
def years():
    r = q("SELECT MIN(year) AS min, MAX(year) AS max FROM typhoons")[0]
    for k in ("built_at", "jma_at"):
        r[k] = (q("SELECT v FROM meta WHERE k=?", (k,)) or [{"v": ""}])[0]["v"]
    return r

SORTS = {"number": "year {d}, number {d}", "date": "start_time {d}",
         "wind": "max_wind IS NULL, max_wind {d}", "pres": "min_pres IS NULL, min_pres {d}",
         "days": "days {d}", "name": "name_en = '', name_en {d}"}

@app.get("/api/typhoons")
def typhoons(name: str | None = None, year_from: int | None = None, year_to: int | None = None,
             month: int | None = None, wind_min: float | None = None, wind_max: float | None = None,
             pres_max: float | None = None, days_min: float | None = None, named: bool = False,
             sort: str = "number", order: str = "desc", limit: int = Query(300, le=1000)):
    where, args = [], []
    def add(c, v): where.append(c); args.append(v)
    if name and name.strip():
        where.append("(title LIKE ? OR name_en LIKE ?)")
        args += [f"%{name.strip()}%", f"%{name.strip().upper()}%"]
    if year_from is not None: add("year>=?", year_from)
    if year_to is not None: add("year<=?", year_to)
    if month is not None: add("month=?", month)
    if wind_min is not None: add("max_wind>=?", wind_min)
    if wind_max is not None: add("max_wind<=?", wind_max)
    if pres_max is not None: add("min_pres<=?", pres_max)
    if days_min is not None: add("days>=?", days_min)
    if named: where.append("name_en<>''")
    w = ("WHERE " + " AND ".join(where)) if where else ""
    ob = SORTS.get(sort, SORTS["number"]).format(d="ASC" if order == "asc" else "DESC")
    total = q(f"SELECT COUNT(*) AS n FROM typhoons {w}", args)[0]["n"]
    return {"total": total, "rows": q(f"SELECT * FROM typhoons {w} ORDER BY {ob} LIMIT ?", (*args, limit))}

@app.get("/api/typhoons/{sid}")
def detail(sid: str):
    t = q("SELECT * FROM typhoons WHERE sid=?", (sid,))
    if not t: raise HTTPException(404, "台風が見つかりません")
    return {**t[0], "track": q(
        "SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time", (sid,))}

# ---------------- 画面 ----------------
PAGE = r"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>台風データベース</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=Zen+Kaku+Gothic+New:wght@400;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#0a1120;--s1:#101a30;--s2:#17233f;--line:#22314f;--ink:#e9eefb;--sub:#8b9bbb;--acc:#38bdf8;--r:14px}
*{box-sizing:border-box}
body{margin:0;height:100dvh;display:grid;grid-template-columns:400px 1fr;background:var(--bg);color:var(--ink);font:14px/1.5 "Zen Kaku Gothic New",system-ui,sans-serif}
aside{display:flex;flex-direction:column;min-height:0;background:var(--s1);border-right:1px solid var(--line)}
header{padding:18px 16px 10px;display:grid;gap:10px}
.top{display:flex;justify-content:space-between;align-items:center;gap:8px}
h1{margin:0;font-size:20px;font-weight:900;letter-spacing:.02em}
.pill{font-size:11px;color:var(--sub);border:1px solid var(--line);border-radius:99px;padding:2px 10px;white-space:nowrap}
.pill b{color:var(--acc);font-weight:700}
input,select,button{font:inherit;color:var(--ink)}
.f{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.f input:not([type=checkbox]),.f select{width:100%;padding:9px 12px;background:var(--s2);border:1px solid transparent;border-radius:10px}
.f input:not([type=checkbox]):focus,.f select:focus{border-color:var(--acc);outline:0}
.f .wide,.f details{grid-column:span 2}
.chips{display:flex;gap:6px;flex-wrap:wrap}
.chip,#dir,#reset{background:var(--s2);border:1px solid transparent;border-radius:99px;padding:5px 12px;font-size:12px;cursor:pointer}
.chip.on{background:var(--acc);color:#04121f;font-weight:700}
.sort{display:grid;grid-template-columns:1fr auto;gap:8px}
#dir{border-radius:10px;padding:0 14px;font-size:16px}
summary{cursor:pointer;color:var(--sub);font-size:13px;padding:2px 0}
.f2{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}
.f label{font-size:12px;color:var(--sub);display:flex;flex-direction:column;gap:3px}
.f label.chk{flex-direction:row;align-items:center;gap:6px}
#reset{border-radius:10px}
:focus-visible{outline:2px solid var(--acc);outline-offset:1px}
#count{padding:0 16px 8px;color:var(--sub);font-size:12px}
#list{flex:1;overflow:auto;margin:0;padding:0 10px 10px;list-style:none;display:grid;gap:6px;align-content:start}
#list li{display:flex;gap:12px;align-items:center;padding:10px 12px;border-radius:12px;background:transparent;cursor:pointer;border:1px solid transparent}
#list li:hover{background:var(--s2)}#list li.on{background:var(--s2);border-color:var(--acc)}
.bar{width:4px;align-self:stretch;border-radius:4px;flex:none}
.t{flex:1;min-width:0}.t b{display:block;font-size:14px}
.t span{color:var(--sub);font-size:12px}
.t em{font-style:normal;font-size:10px;margin-left:6px;padding:1px 6px;border-radius:99px;background:#f0803c22;color:#f0a06c;vertical-align:1px}
.m{height:3px;background:var(--line);border-radius:3px;margin-top:6px}.m i{display:block;height:100%;border-radius:3px}
.v{text-align:right;line-height:1.2}.v strong{font:700 20px "Space Grotesk",sans-serif}.v small{display:block;color:var(--sub);font-size:11px}
footer{padding:8px 16px;border-top:1px solid var(--line);color:var(--sub);font-size:11px}
main{position:relative;min-height:0}#map{height:100%;background:#0b1522}
.card{position:absolute;z-index:500;background:rgba(16,26,48,.88);backdrop-filter:blur(12px);border:1px solid var(--line);border-radius:var(--r)}
#info{right:14px;top:14px;padding:14px 16px;width:300px;max-width:calc(100% - 28px)}
#info h2{margin:0 0 2px;font-size:17px}#info .sub{color:var(--sub);font-size:12px}
.g{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}
.g div{background:var(--s2);border-radius:10px;padding:8px 10px}
.g small{display:block;color:var(--sub);font-size:11px}.g strong{font:700 17px "Space Grotesk",sans-serif}.g strong span{font-size:11px;font-weight:400;color:var(--sub)}
#legend{left:14px;bottom:24px;padding:8px 12px;font-size:12px}
#legend div{display:flex;gap:8px;align-items:center}#legend i{width:14px;height:4px;border-radius:2px;display:inline-block}
.leaflet-tooltip{background:var(--s1);color:var(--ink);border:1px solid var(--line);border-radius:8px}
@media(max-width:760px){body{grid-template-columns:1fr;grid-template-rows:42dvh 1fr}aside{order:2;border:0}main{order:1}#info{width:auto;left:10px;right:10px;top:auto;bottom:10px;padding:10px 12px}.g{margin-top:8px}#legend{display:none}}
</style></head><body>
<aside>
 <header>
  <div class="top"><h1>台風データベース</h1><span class="pill" id="st"></span></div>
  <form class="f" id="f" onsubmit="return false">
   <input class="wide" name="name" placeholder="名前・号数で検索（例: SURIGAE / 15号）">
   <div class="chips wide">
    <button type="button" class="chip" data-w="">全ての強さ</button><button type="button" class="chip" data-w="64">強い〜</button>
    <button type="button" class="chip" data-w="85">非常に強い〜</button><button type="button" class="chip" data-w="105">猛烈な</button></div>
   <div class="sort wide"><select name="sort"><option value="number">号数順</option><option value="date">発生日順</option><option value="wind">最大風速順</option><option value="pres">最低気圧順</option><option value="days">継続日数順</option><option value="name">名前順</option></select>
    <button type="button" id="dir" title="昇順・降順">↓</button><input type="hidden" name="order" value="desc"></div>
   <details><summary>詳細条件</summary><div class="f2">
    <label>年（から）<select name="year_from"><option value="">指定なし</option></select></label>
    <label>年（まで）<select name="year_to"><option value="">指定なし</option></select></label>
    <label>発生月<select name="month"><option value="">全て</option></select></label>
    <label>表示件数<select name="limit"><option>100</option><option selected>300</option><option>1000</option></select></label>
    <label>最大風速 kt 以上<input type="number" name="wind_min" min="0"></label>
    <label>最大風速 kt 以下<input type="number" name="wind_max" min="0"></label>
    <label>最低気圧 hPa 以下<input type="number" name="pres_max" placeholder="例: 930"></label>
    <label>継続日数 以上<input type="number" name="days_min" min="0" step="0.5"></label>
    <label class="chk"><input type="checkbox" name="named">名前付きのみ</label>
    <button type="button" id="reset">条件をリセット</button>
   </div></details>
  </form></header>
 <div id="count"></div><ul id="list"></ul>
 <footer>出典: 気象庁（ベストトラック／位置表。IBTrACS経由）。風速は10分平均、時刻は日本時間。「速報」は速報値で後日修正されます。</footer>
</aside>
<main><div id="map"></div><div id="info" class="card" hidden></div><div id="legend" class="card"></div></main>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const CLS=[[105,"猛烈な","#ff4d7d"],[85,"非常に強い","#ff8a3d"],[64,"強い","#ffd24a"],[34,"台風","#4fc3f7"],[0,"熱帯低気圧","#64789a"]];
const UNK=[null,"不明","#3d4f66"];
const K=w=>w==null?UNK:CLS.find(c=>w>=c[0]), col=w=>K(w)[2], cls=w=>K(w)[1];
const spd=w=>w==null?"-":`${w}kt（${Math.round(w*0.5144)}m/s）`;
const jst=(s,full)=>{const d=new Date(new Date(s.replace(" ","T")+"Z").getTime()+9*3600e3),z=n=>String(n).padStart(2,"0");
  return `${full?d.getUTCFullYear()+"/":""}${d.getUTCMonth()+1}/${d.getUTCDate()} ${z(d.getUTCHours())}時`};
const $=s=>document.querySelector(s), f=$("#f"), chips=document.querySelectorAll(".chip");
const map=L.map("map",{worldCopyJump:true,zoomControl:false}).setView([25,135],4);
L.control.zoom({position:"bottomright"}).addTo(map);
const ESRI="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/";
L.tileLayer(ESRI+"World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",{attribution:"Tiles © Esri — Esri, DeLorme, NAVTEQ | 気象庁 / IBTrACS (NOAA)",maxZoom:12}).addTo(map);
map.createPane("labels").style.zIndex=450;map.getPane("labels").style.pointerEvents="none";
L.tileLayer(ESRI+"World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}",{pane:"labels",maxZoom:12}).addTo(map);
let layer=L.layerGroup().addTo(map), first=true;
$("#legend").innerHTML=[...CLS,UNK].map(c=>`<div><i style="background:${c[2]}"></i>${c[1]}</div>`).join("");

async function init(){
  const y=await (await fetch("/api/years")).json();
  for(let i=y.max;i>=y.min;i--){f.year_from.add(new Option(i+"年",i));f.year_to.add(new Option(i+"年",i))}
  for(let m=1;m<=12;m++) f.month.add(new Option(m+"月",m));
  if(y.jma_at) $("#st").innerHTML=`速報 <b>${jst(y.jma_at)}</b> 更新`;
  $("#reset").onclick=()=>{f.reset();f.order.value="desc";load()};
  $("#dir").onclick=()=>{f.order.value=f.order.value=="desc"?"asc":"desc";load()};
  chips.forEach(c=>c.onclick=()=>{f.wind_min.value=c.dataset.w;f.wind_max.value="";load()});
  f.addEventListener("input",()=>{clearTimeout(init.t);init.t=setTimeout(load,250)});
  load();
}
async function load(){
  const p=new URLSearchParams();
  for(const k of ["name","year_from","year_to","month","wind_min","wind_max","pres_max","days_min","sort","order","limit"]) if(f[k].value) p.set(k,f[k].value);
  if(f.named.checked) p.set("named","true");
  chips.forEach(c=>c.classList.toggle("on",c.dataset.w===f.wind_min.value&&!f.wind_max.value));
  $("#dir").textContent=f.order.value=="desc"?"↓":"↑";
  try{
    const {total,rows}=await (await fetch("/api/typhoons?"+p)).json();
    $("#count").textContent=total?`${total}件`+(total>rows.length?`中 ${rows.length}件を表示（件数を増やすか条件を絞ってください）`:""):"該当なし。条件を変えてください";
    $("#list").innerHTML=rows.map(r=>`<li data-sid="${r.sid}"><span class="bar" style="background:${col(r.max_wind)}"></span>
     <div class="t"><b>${r.title}${r.prov?"<em>速報</em>":""}</b><span>${jst(r.start_time,1)}〜 ／ ${r.days}日</span>
     <div class="m"><i style="width:${Math.min(100,(r.max_wind||0)/1.3)}%;background:${col(r.max_wind)}"></i></div></div>
     <div class="v"><strong>${r.max_wind??"-"}</strong><small>kt</small><small>${r.min_pres??"-"}hPa</small></div></li>`).join("");
    if(first){first=false;const s=location.hash.slice(1)||(rows[0]&&rows[0].sid);if(s)show(s)}  // 共有リンク or 最新の台風を自動表示
  }catch(e){$("#count").textContent="読み込みに失敗しました。再読み込みしてください"}
}
$("#list").addEventListener("click",e=>{const li=e.target.closest("li");if(li)show(li.dataset.sid,li)});
async function show(sid,li){
  document.querySelectorAll("#list li.on").forEach(x=>x.classList.remove("on"));
  li=li||document.querySelector(`#list li[data-sid="${sid}"]`);
  if(li){li.classList.add("on");li.scrollIntoView({block:"nearest"})}
  history.replaceState(null,"","#"+sid);
  const r=await fetch("/api/typhoons/"+sid);if(!r.ok)return;
  const t=await r.json();
  layer.clearLayers();
  const pts=t.track;
  // 日付変更線をまたぐ場合に備えて経度を連続化
  let prev=null;const ll=pts.map(p=>{let lo=p.lon;if(prev!==null){while(lo-prev>180)lo-=360;while(lo-prev<-180)lo+=360}prev=lo;return [p.lat,lo]});
  L.polyline(ll,{color:"#fff",weight:9,opacity:.08}).addTo(layer);
  for(let i=1;i<pts.length;i++) L.polyline([ll[i-1],ll[i]],{color:col(pts[i].wind),weight:4,lineCap:"round"}).addTo(layer);
  pts.forEach((p,i)=>L.circleMarker(ll[i],{radius:i==0||i==pts.length-1?6:3,color:"#fff",weight:1,fillColor:col(p.wind),fillOpacity:1})
    .bindTooltip(`${i==0?"発生 ":i==pts.length-1?"終了 ":""}${jst(p.time,1)}<br>${spd(p.wind)} / ${p.pres??"-"}hPa`).addTo(layer));
  map.fitBounds(L.latLngBounds(ll).pad(.3));
  const i=$("#info");i.hidden=false;
  i.innerHTML=`<h2>${t.title}${t.prov?'<span class="pill" style="margin-left:8px">速報値</span>':""}</h2>
   <div class="sub">${jst(t.start_time,1)} 〜 ${jst(t.end_time,1)}（日本時間）</div>
   <div class="g"><div><small>最大風速</small><strong>${t.max_wind??"-"}<span> kt</span></strong></div>
   <div><small>強さ</small><strong style="color:${col(t.max_wind)}">${cls(t.max_wind)}</strong></div>
   <div><small>最低気圧</small><strong>${t.min_pres??"-"}<span> hPa</span></strong></div>
   <div><small>継続</small><strong>${t.days}<span> 日</span></strong></div></div>`;
}
init();
</script></body></html>
"""

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        build()
    else:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
