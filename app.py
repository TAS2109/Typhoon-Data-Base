"""台風データベース（1ファイル版・気象庁ベストトラック準拠）
  ビルド:  python app.py build      # IBTrACSを取得して typhoon.db を作成
  起動:    uvicorn app:app --host 0.0.0.0 --port $PORT   (DBが無ければ起動時に自動ビルド)
表記: 「2026年台風第26号 Surigae」。号数は気象庁方式（熱帯低気圧の段階は数えず、
      台風の強さに初めて達した順に年ごとに採番）で再計算する。
"""
import csv, os, sqlite3, sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

URL = ("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/"
       "v04r01/access/csv/ibtracs.WP.list.v04r01.csv")
SRC = os.environ.get("SOURCE_CSV")  # ローカルテスト用
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

def load_rows():
    """CSVを1行ずつ流し読みする（全体をメモリに載せない）。"""
    def lines():
        if SRC:
            with open(SRC, encoding="utf-8") as f:
                yield from (ln.rstrip("\n") for ln in f)
        else:
            print("Downloading", URL, flush=True)
            with requests.get(URL, stream=True, timeout=300) as r:
                r.raise_for_status()
                r.encoding = "utf-8"
                yield from r.iter_lines(decode_unicode=True)
    rd = csv.DictReader(lines())
    next(rd)  # 2行目は単位行
    return rd

def build():
    tmp = DB + ".tmp"  # 失敗しても既存DBを壊さないよう、一時ファイルに作って最後に置換
    if os.path.exists(tmp):
        os.remove(tmp)
    con = sqlite3.connect(tmp)
    con.executescript("""
    CREATE TABLE typhoons(sid TEXT PRIMARY KEY, year INT, number INT, title TEXT, name_en TEXT,
        start_time TEXT, end_time TEXT, max_wind REAL, min_pres REAL, month INT, days REAL);
    CREATE TABLE points(sid TEXT, time TEXT, lat REAL, lon REAL, wind REAL, pres REAL);
    CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT);
    CREATE INDEX idx_points_sid ON points(sid);
    CREATE INDEX idx_ty_year ON typhoons(year, number);
    """)
    metas = []

    def flush(m, p):
        # 気象庁の階級が「台風の強さ(TS以上)」に一度でも達したものだけ採用（気象庁の台風の定義）
        ts = [x[0] for x in p if x[5]]
        if not ts:
            return
        winds = [x[3] for x in p if x[3]]
        press = [x[4] for x in p if x[4]]
        con.executemany("INSERT INTO points VALUES(?,?,?,?,?,?)", [(m["sid"], *x[:5]) for x in p])
        metas.append(dict(m, t0=jst(ts[0]), start=p[0][0], end=p[-1][0],
                          mw=max(winds) if winds else None, mp=min(press) if press else None,
                          days=round((datetime.strptime(p[-1][0][:19], "%Y-%m-%d %H:%M:%S")
                                      - datetime.strptime(p[0][0][:19], "%Y-%m-%d %H:%M:%S")).total_seconds() / 86400, 1)))

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
        grade = num(r.get("TOKYO_GRADE"))
        is_ts = (grade in (3, 4, 5, 7)) or (wind is not None and wind >= 34)
        pts.append((r["ISO_TIME"], lat, lon, wind or None, pres or None, is_ts))
    if cur is not None:
        flush(meta, pts)

    # 号数: TSの強さに初めて達した日時(JST)の順に、年ごとに1から採番
    metas.sort(key=lambda m: m["t0"])
    seq = {}
    for m in metas:
        y = m["t0"].year
        seq[y] = seq.get(y, 0) + 1
        con.execute("INSERT INTO typhoons VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (m["sid"], y, seq[y], title(y, seq[y], m["en"]), m["en"],
                     m["start"], m["end"], m["mw"], m["mp"], m["t0"].month, m["days"]))
    con.execute("INSERT INTO meta VALUES('built_at',?)", (datetime.now().strftime("%Y-%m-%d"),))
    con.commit(); con.close()
    os.replace(tmp, DB)
    print(f"Done: {len(metas)} typhoons")

# ---------------- API ----------------
@asynccontextmanager
async def lifespan(_):
    if not os.path.exists(DB):
        build()
    yield

app = FastAPI(title="Typhoon DB", lifespan=lifespan)

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
    r["built_at"] = (q("SELECT v FROM meta WHERE k='built_at'") or [{"v": ""}])[0]["v"]
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
<link href="https://fonts.googleapis.com/css2?family=Zen+Kaku+Gothic+New:wght@400;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#0f1b2b;--panel:#16283d;--line:#2a4160;--ink:#e8eef5;--sub:#8fa5bf;--acc:#f2c14e}
*{box-sizing:border-box}
body{margin:0;height:100vh;display:grid;grid-template-columns:360px 1fr;background:var(--bg);color:var(--ink);font-family:"Zen Kaku Gothic New",sans-serif}
aside{display:flex;flex-direction:column;min-height:0;border-right:1px solid var(--line)}
header{padding:18px 18px 12px}
h1{margin:0 0 12px;font-size:22px;font-weight:900;letter-spacing:.04em}
.f{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.f input,.f select{width:100%;padding:8px;background:var(--panel);border:1px solid var(--line);color:var(--ink);border-radius:4px;font:inherit}
.f input[name=name],.f details{grid-column:span 2}
.f2{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}
.f label{font-size:12px;color:var(--sub);display:flex;flex-direction:column;gap:2px}
.f label.chk{flex-direction:row;align-items:center;gap:6px}.f input[type=checkbox]{width:auto}
.f summary{cursor:pointer;color:var(--sub);font-size:13px}.f button{padding:8px;background:var(--panel);border:1px solid var(--line);color:var(--ink);border-radius:4px;font:inherit;cursor:pointer}
:focus-visible{outline:2px solid var(--acc);outline-offset:1px}
#count{padding:0 18px 8px;color:var(--sub);font-size:13px}
#list{flex:1;overflow:auto;margin:0;padding:0;list-style:none}
#list li{padding:10px 18px;border-top:1px solid var(--line);cursor:pointer;display:flex;gap:10px;align-items:center}
#list li:hover,#list li.on{background:var(--panel)}
.sw{width:10px;height:34px;border-radius:2px;flex:none}
.t b{display:block;font-size:14px}.t span{color:var(--sub);font-size:12px}
footer{padding:8px 18px;border-top:1px solid var(--line);color:var(--sub);font-size:11px;line-height:1.5}
main{position:relative;min-height:0}.leaflet-tile-pane{filter:invert(1) hue-rotate(180deg) brightness(.85) contrast(.9) saturate(.6)}#map{height:100%;background:#0b1522}
#info{position:absolute;z-index:500;right:14px;top:14px;background:rgba(15,27,43,.92);border:1px solid var(--line);padding:12px 14px;border-radius:6px;font-size:13px;line-height:1.6;min-width:220px;max-width:calc(100% - 28px)}
#info h2{margin:0 0 4px;font-size:17px}
#legend{position:absolute;z-index:500;left:14px;bottom:24px;background:rgba(15,27,43,.92);border:1px solid var(--line);padding:8px 12px;border-radius:6px;font-size:12px}
#legend div{display:flex;gap:8px;align-items:center}#legend i{width:14px;height:4px;display:inline-block}
@media(max-width:760px){body{grid-template-columns:1fr;grid-template-rows:45vh 1fr}aside{order:2;border:0}main{order:1}}
</style></head><body>
<aside>
 <header><h1>台風データベース</h1>
  <form class="f" id="f" onsubmit="return false">
   <input name="name" placeholder="名前・号数で検索（例: SURIGAE / 15号）">
   <select name="sort"><option value="number">号数順</option><option value="date">発生日順</option><option value="wind">最大風速順</option><option value="pres">最低気圧順</option><option value="days">継続日数順</option><option value="name">名前順</option></select>
   <select name="order"><option value="desc">降順（新しい・大きい）</option><option value="asc">昇順（古い・小さい）</option></select>
   <details><summary>詳細条件</summary><div class="f2">
    <label>年（から）<select name="year_from"><option value="">指定なし</option></select></label>
    <label>年（まで）<select name="year_to"><option value="">指定なし</option></select></label>
    <label>発生月<select name="month"><option value="">全て</option></select></label>
    <label>表示件数<select name="limit"><option>100</option><option selected>300</option><option>1000</option></select></label>
    <label>最大風速 kt 以上<input type="number" name="wind_min" min="0" placeholder="例: 85"></label>
    <label>最大風速 kt 以下<input type="number" name="wind_max" min="0"></label>
    <label>最低気圧 hPa 以下<input type="number" name="pres_max" placeholder="例: 930"></label>
    <label>継続日数 以上<input type="number" name="days_min" min="0" step="0.5"></label>
    <label class="chk"><input type="checkbox" name="named">名前付きのみ</label>
    <button type="button" id="reset">条件をリセット</button>
   </div></details>
  </form></header>
 <div id="count"></div><ul id="list"></ul>
 <footer id="src">出典: 気象庁ベストトラック（NOAA IBTrACS経由）。風速は10分平均、時刻は日本時間(JST)。</footer>
</aside>
<main><div id="map"></div><div id="info" hidden></div>
 <div id="legend"></div></main>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
// 気象庁の強さの階級（10分平均風速 kt）
const CLS=[[105,"猛烈な","#e0245e"],[85,"非常に強い","#f0803c"],[64,"強い","#f2c14e"],[34,"台風（階級なし）","#6fa8dc"],[0,"熱帯低気圧","#5b6f88"]];
const UNK=[null,"不明","#3d4f66"];
const K=w=>w==null?UNK:CLS.find(c=>w>=c[0]);
const col=w=>K(w)[2], cls=w=>K(w)[1];
const ms=kt=>Math.round(kt*0.5144);
const spd=w=>w==null?"-":`${w}kt（${ms(w)}m/s）`;
const jst=(s,full)=>{const d=new Date(new Date(s.replace(" ","T")+"Z").getTime()+9*3600e3),z=n=>String(n).padStart(2,"0");
  return `${full?d.getUTCFullYear()+"/":""}${d.getUTCMonth()+1}/${d.getUTCDate()} ${z(d.getUTCHours())}時`};
const $=s=>document.querySelector(s), f=$("#f");
const map=L.map("map",{worldCopyJump:true}).setView([25,135],4);
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png",{attribution:"© OpenStreetMap contributors | 気象庁 / IBTrACS (NOAA)",maxZoom:8}).addTo(map);
let layer=L.layerGroup().addTo(map);
$("#legend").innerHTML=[...CLS,UNK].map(c=>`<div><i style="background:${c[2]}"></i>${c[1]}</div>`).join("");

async function init(){
  const y=await (await fetch("/api/years")).json();
  for(let i=y.max;i>=y.min;i--){f.year_from.add(new Option(i+"年",i));f.year_to.add(new Option(i+"年",i))}
  for(let m=1;m<=12;m++) f.month.add(new Option(m+"月",m));
  $("#reset").onclick=()=>{f.reset();load()};
  if(y.built_at) $("#src").textContent+=` データ更新: ${y.built_at}`;
  f.addEventListener("input",()=>{clearTimeout(init.t);init.t=setTimeout(load,250)});
  load();
}
async function load(){
  const p=new URLSearchParams();
  for(const k of ["name","year_from","year_to","month","wind_min","wind_max","pres_max","days_min","sort","order","limit"]) if(f[k].value) p.set(k,f[k].value);
  if(f.named.checked) p.set("named","true");
  try{
    const {total,rows}=await (await fetch("/api/typhoons?"+p)).json();
    $("#count").textContent=total?`${total}件`+(total>rows.length?`中 ${rows.length}件を表示（件数を増やすか条件を絞ってください）`:""):"該当なし。条件を変えてください";
    $("#list").innerHTML=rows.map(r=>`<li data-sid="${r.sid}"><span class="sw" style="background:${col(r.max_wind)}"></span>
     <div class="t"><b>${r.title}</b><span>${jst(r.start_time,1)}〜 ／ 最大 ${spd(r.max_wind)} ／ ${r.min_pres??"-"}hPa ／ ${r.days}日</span></div></li>`).join("");
  }catch(e){$("#count").textContent="読み込みに失敗しました。再読み込みしてください"}
}
$("#list").addEventListener("click",e=>{const li=e.target.closest("li");if(li)show(li.dataset.sid,li)});
async function show(sid,li){
  document.querySelectorAll("#list li.on").forEach(x=>x.classList.remove("on"));li.classList.add("on");
  const t=await (await fetch("/api/typhoons/"+sid)).json();
  layer.clearLayers();
  const pts=t.track;
  // 日付変更線をまたぐ場合に備えて経度を連続化
  let prev=null;const ll=pts.map(p=>{let lo=p.lon;if(prev!==null){while(lo-prev>180)lo-=360;while(lo-prev<-180)lo+=360}prev=lo;return [p.lat,lo]});
  for(let i=1;i<pts.length;i++) L.polyline([ll[i-1],ll[i]],{color:col(pts[i].wind),weight:4}).addTo(layer);
  pts.forEach((p,i)=>L.circleMarker(ll[i],{radius:3,color:"#fff",weight:1,fillColor:col(p.wind),fillOpacity:1})
    .bindTooltip(`${jst(p.time,1)}<br>${spd(p.wind)} / ${p.pres??"-"}hPa`).addTo(layer));
  map.fitBounds(L.latLngBounds(ll).pad(.3));
  const i=$("#info");i.hidden=false;
  i.innerHTML=`<h2>${t.title}</h2>${t.name_en?`国際名: ${t.name_en}<br>`:""}期間: ${jst(t.start_time,1)} 〜 ${jst(t.end_time,1)}<br>最大風速: ${spd(t.max_wind)}${t.max_wind!=null?"　"+cls(t.max_wind):""}<br>最低気圧: ${t.min_pres??"-"}hPa<br>継続: ${t.days}日`;
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
