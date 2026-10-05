"""台風データベース（1ファイル版・気象庁ベストトラック準拠）
  ビルド:  python app.py build      # IBTrACSを取得して typhoon.db を作成
  起動:    uvicorn app:app --host 0.0.0.0 --port $PORT   (DBが無ければ起動時に自動ビルド)
表記: 「2026年台風第26号 Surigae」。号数は気象庁方式（熱帯低気圧の段階は数えず、
      台風の強さに初めて達した順に年ごとに採番）で再計算する。
風速はm/s表示（DB内部はkt保持、API出力時に換算）。
今年ぶんは 位置表PDF（確定・速報）＋防災情報JSON（発生中の台風）＋デジタル台風（NII、PDF未掲載の消滅済み台風の補完）で随時更新する。
データ出典: 気象庁 / IBTrACS / デジタル台風（国立情報学研究所・北本朗）
画面: 1ページ・地図共有の4タブ構成（予報 / 過去の台風 / 統計 / 実況）。旧 /forecast は /#fc へ転送。
実況タブ: 発生中の台風の実況・進路予報（気象庁 防災情報JSON）＋アメダスの観測値（風速・瞬間風速・気圧・雨量）＋警報・注意報。
  取得元の確認:  /api/live/diag をブラウザで開く（JSONの形が変わった時の診断用）
統計タブ: 概要（今年の発生ペース・強さ別・風速分布）/ 推移（年別・年代別）/ 季節（月別）/ 記録（Top10）の4サブタブ。
  グラフをなぞると内訳が出て、年・月・強さ・風速帯をタップすると「過去の台風」へ絞り込んで移る。集計対象は「過去の台風」の絞り込み条件と共通。
実況タブの警報・注意報は 気象庁の新体系(2026年5月〜: 警報/危険警報/特別警報)の r8/map.json から取得し、地図に1次細分区域・市町村等ごとに薄く塗りつぶす。
  地図の形(GeoJSON)は /api/live/geo/{name} で気象庁から取得してサーバーに24時間キャッシュする。
強風域・暴風域（気象庁の半径）は radii テーブルに保存し、過去の台風タブで表示する（既定ON）。
  過去の台風は IBTrACS（ベストトラック）、今年の台風は位置表PDF（速報・確定）と防災情報JSONの実況から取り込む。予報には付けない。
台風の大きさ（気象庁の階級）: 強風域(15m/s以上)の最大半径が 500km以上=大型、800km以上=超大型。過去の台風は生涯の最大半径、予報・実況は現在の半径で判定する。
  実況タブのアメダス観測値の範囲は、既定を「自動」とし、台風の強さと大きさ（強風域の半径）から決める。
  さらに、波浪・高潮の注意報以上（その他の台風関係は警報以上）の発表地域が、その範囲の外に及ぶ時は、その地域まで範囲を広げる。
めずらしい台風（復活・越境・ループ・急発達）と移動距離は、各台風の点列から自動判定して feat テーブルに保存する。
  判定結果の確認:  python app.py featdiag
"""
import asyncio, csv, io, json, math, os, re, sqlite3, sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import requests
from concurrent.futures import ThreadPoolExecutor
from itertools import groupby
from fastapi.middleware.gzip import GZipMiddleware
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

URL = ("https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-stewardship-ibtracs/"
       "v04r01/access/csv/ibtracs.WP.list.v04r01.csv")
SRC = os.environ.get("SOURCE_CSV")  # ローカルテスト用
JMA = "https://www.data.jma.go.jp/typhoon"
F = "%Y-%m-%d %H:%M:%S"
DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "typhoon.db")
BOSAI = "https://www.jma.go.jp/bosai/typhoon/data"  # 発生中の台風（防災情報JSON）
UA = {"User-Agent": "typhoon-db/1.0"}
DT = "https://agora.ex.nii.ac.jp/digital-typhoon"  # デジタル台風（気象庁の速報値を号数つきで公開。PDF掲載前の補完用）
INTERVAL = int(os.environ.get("REFRESH_MINUTES", "60")) * 60  # 速報の再取得間隔（秒）
KT2MS, MS2KT = 0.514444, 1.94384

def to_ms(kt):
    """DBはkt保持。表示用にm/s（整数）へ換算する。m/s→kt→m/sは常に元の整数に戻る。"""
    return None if kt is None else int(kt * KT2MS + 0.5)

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

def rad_of(r):
    """IBTrACSの1行 → (暴風域の向き, 長径, 短径, 強風域の向き, 長径, 短径)（半径は海里）。半径が1つも無ければ None。
    向き: 1=NE 2=E 3=SE 4=S 5=SW 6=W 7=NW 8=N 9=全方向（円）。長径が無い・0 の側は None にする。"""
    out = []
    for k in ("R50", "R30"):
        d, lo, sh = (num(r.get(f"TOKYO_{k}_{x}")) for x in ("DIR", "LONG", "SHORT"))
        if lo and lo > 0:
            out += [int(d) if d is not None else 9, lo, sh if sh and sh > 0 else lo]
        else:
            out += [None, None, None]
    return tuple(out) if out[1] or out[4] else None

# ---- 半径（位置表PDF・防災情報JSON用）。radii テーブルは (向き, 長径, 短径) を海里で持つ ----
NM = 1.852
DIR8 = {"NE": 1, "E": 2, "SE": 3, "S": 4, "SW": 5, "W": 6, "NW": 7, "N": 8}
JDIR8 = {"北東": 1, "東": 2, "南東": 3, "南": 4, "南西": 5, "西": 6, "北西": 7, "北": 8}
_SPEC = r"(---|[NSEW]{1,2}:\s*\d+(?:\s+[NSEW]{1,2}:\s*\d+)?|\d+)"  # 位置表の「暴風域半径」「強風域半径」の1列ぶん
RAD_COLS = re.compile(r"^\s*" + _SPEC + r"\s+" + _SPEC)

def _pair_area(pr):
    """[(向きコード or None, 半径km), ...] → (向き, 長径km, 短径km)。半径が1つだけ・向き不明なら全方向の円(9)。"""
    pr = sorted((x for x in pr if x[1] and x[1] > 0), key=lambda x: -x[1])
    if not pr:
        return None
    if len(pr) == 1 or pr[0][0] is None:
        return (9, pr[0][1], pr[0][1])
    return (pr[0][0], pr[0][1], pr[1][1])

def _rad_km(storm, gale):
    """(暴風域, 強風域) = 各 (向き, 長径km, 短径km) or None → radii 行の値（半径は海里）。どちらも無ければ None。"""
    if not storm and not gale:
        return None
    out = []
    for a in (storm, gale):
        out += [a[0], a[1] / NM, a[2] / NM] if a else [None, None, None]
    return tuple(out)

def _text_area(sp):
    sp = sp.strip()
    if sp.startswith("-"):
        return None
    if sp.isdigit():
        return _pair_area([(None, int(sp))])
    return _pair_area([(DIR8.get(d), int(v)) for d, v in re.findall(r"([NSEW]{1,2}):\s*(\d+)", sp)])

def rad_from_text(rest):
    """位置表PDFの1行の風速より右側（例 ' 85  E: 300 W: 165  － 強い'）→ radii 行の値。無ければ None。
    列は 暴風域半径(km)・強風域半径(km)。'---'=無し、数字1つ=円、'E: 300 W: 165'=長い側と短い側。"""
    m = RAD_COLS.match(rest)
    return _rad_km(_text_area(m[1]), _text_area(m[2])) if m else None

def ensure_radii(con):
    con.execute("CREATE TABLE IF NOT EXISTS radii(sid TEXT, time TEXT, d50 INT, l50 REAL, s50 REAL, d30 INT, l30 REAL, s30 REAL)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_radii_sid ON radii(sid)")

def set_radii(con, sid, rads, upto):
    """位置表PDFの半径で、PDFの最終時刻までの分を置き換える（PDFより新しい実況の半径は残す）。"""
    ensure_radii(con)
    con.execute("DELETE FROM radii WHERE sid=? AND time<=?", (sid, upto))
    if rads:
        con.executemany("INSERT INTO radii VALUES(?,?,?,?,?,?,?,?)", [(sid, *x) for x in rads])

def jst(iso):
    return datetime.strptime(iso[:19], "%Y-%m-%d %H:%M:%S") + timedelta(hours=9)

NEED = ("SID", "NAME", "ISO_TIME", "TRACK_TYPE", "TOKYO_LAT", "TOKYO_LON",
        "TOKYO_GRADE", "TOKYO_WIND", "TOKYO_PRES",
        "TOKYO_R50_DIR", "TOKYO_R50_LONG", "TOKYO_R50_SHORT",
        "TOKYO_R30_DIR", "TOKYO_R30_LONG", "TOKYO_R30_SHORT")  # 半径は海里。R50=暴風域(25m/s以上)、R30=強風域(15m/s以上)

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
    CREATE TABLE radii(sid TEXT, time TEXT, d50 INT, l50 REAL, s50 REAL, d30 INT, l30 REAL, s30 REAL);
    """)
    con.executescript("PRAGMA synchronous=OFF; PRAGMA journal_mode=OFF;")  # ビルド専用の一時DBなので高速化
    metas = []

    def flush(m, p):
        # 気象庁の階級が「台風の強さ(TS以上)」に一度でも達したものだけ採用（気象庁の台風の定義）
        ts = [x[5] for x in p]
        if any(ts):
            metas.append(dict(m, t0=jst(p[ts.index(True)][0]), p=[x[:5] for x in p],
                              rad=[(x[0], *x[6]) for x in p if x[6]]))

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
        rd = rad_of(r)
        pts.append((r["ISO_TIME"], lat, lon, wind or None, pres or None, is_ts, rd))
    if cur is not None:
        flush(meta, pts)

    # 号数: TSの強さに初めて達した日時(JST)の順に、年ごとに1から採番
    metas.sort(key=lambda m: m["t0"])
    seq = {}
    for m in metas:
        y = m["t0"].year
        seq[y] = seq.get(y, 0) + 1
        insert(con, m["sid"], y, seq[y], m["en"], m["p"])
        con.executemany("INSERT INTO radii VALUES(?,?,?,?,?,?,?,?)", [(m["sid"], *x) for x in m["rad"]])
    con.executescript("CREATE INDEX idx_radii_sid ON radii(sid); CREATE INDEX idx_points_sid ON points(sid); CREATE INDEX idx_ty_year ON typhoons(year, number);")
    con.execute("INSERT INTO meta VALUES('built_at',?)", (datetime.now().strftime("%Y-%m-%d"),))
    feat_update(con, full=True)  # 移動距離・復活・越境などの特徴量
    con.commit(); con.close()
    os.replace(tmp, DB)
    print(f"Done: {len(metas)} typhoons（今年の速報値は起動後に自動で取り込みます）")

# ---------------- 気象庁の速報値（今年ぶん） ----------------
HEAD = re.compile(r"(\d{4})年台風第\s*(\d+)号\s+([A-Za-z][A-Za-z\-]*)")
ROW = re.compile(r"^(?:(\d{1,2})\s+)?(?:(\d{1,2})\s+)?(\d{1,2})\s+(\d+\.\d)(?:\s+N)?\s+(\d+\.\d)(?:\s+E)?\s+(\d{3,4}|--)\s+(\d{1,2}|--)(?=\s|$)")

def parse_pdf(text):
    """気象庁の台風位置表PDF（日本時・風速m/s）→ (年, 号数, 名前, 点列(UTC・kt), 速報か, 半径行[(時刻UTC, 暴風域3つ, 強風域3つ)])"""
    h = HEAD.search(text)
    if not h:
        return None
    year, no, name = int(h[1]), int(h[2]), h[3].upper()
    y0 = year  # 号数の年（年またぎでも変えない）
    mo = da = None; pts = []; rads = []
    for ln in text.splitlines():
        ln = ln.strip()
        r = ROW.match(ln)
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
        wind = None if r[7] == "--" else round(int(r[7]) * MS2KT)  # m/s→kt
        pts.append((t.strftime(F), float(r[4]), float(r[5]), wind, pres))
        rd = rad_from_text(ln[r.end():])
        if rd:
            rads.append((t.strftime(F), *rd))
    return (y0, no, name, pts, "速報値" in text, rads) if pts else None

def fetch_pdf(c, since=None):
    """位置表PDFを取得して解析。戻り値: (コード, 解析結果 or None, Last-Modified)。
    未掲載(404)・変更なし(304)・解析失敗は None（1件の失敗で全体を止めない）。"""
    try:
        import pdfplumber
        h = dict(UA, **({"If-Modified-Since": since} if since else {}))
        r = requests.get(f"{JMA}/data/T{c}.pdf", headers=h, timeout=60)
        if r.status_code != 200 or not r.content.startswith(b"%PDF"):
            if r.status_code not in (200, 304, 404):
                print(f"T{c}.pdf: HTTP {r.status_code}", flush=True)
            return c, None, None
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            text = "\n".join(pg.extract_text() or "" for pg in pdf.pages)
        res = parse_pdf(text)
        if not res:
            print(f"T{c}.pdf: parse failed", flush=True)
        return c, res, r.headers.get("Last-Modified")
    except Exception as e:
        print(f"T{c}.pdf failed:", repr(e), flush=True)
        return c, None, None

def refresh_pdf(con, yr):
    """位置表PDFから取り込む。確定済みは再取得せず、速報はLast-Modifiedが変わった時だけ取り直す。
    一覧ページの掲載は遅れる（数号ぶん）ので、掲載済みの次の号のPDFも探す。"""
    yy = str(yr)[2:]
    html = requests.get(f"{JMA}/position_table/table{yr}.html", headers=UA, timeout=60).text
    codes = sorted({c for c in re.findall(r"T(\d{4})\.pdf", html) if c[:2] == yy})
    top = int(codes[-1][2:]) if codes else 0
    codes += [f"{yy}{n:02d}" for n in range(top + 1, top + 6)]  # 一覧の掲載は遅れるので先の号も探す(404は無視)
    ensure_radii(con)
    done = {r[0] for r in con.execute("SELECT sid FROM typhoons WHERE sid LIKE 'JMA%' AND prov=0")}
    have = {r[0] for r in con.execute("SELECT sid FROM typhoons WHERE sid LIKE 'JMA%'")}
    lm = dict(con.execute("SELECT k, v FROM meta WHERE k LIKE 'lm:%'"))
    rb = {r[0][3:] for r in con.execute("SELECT k FROM meta WHERE k LIKE 'rb:%'")}  # 半径を取り込み済みの台風
    bf = {x for x in have if x not in rb}  # 半径対応前に取り込んだ台風は、確定済み・変更なしでも一度だけ取り直す
    todo = [c for c in codes if f"JMA{c}" not in done or f"JMA{c}" in bf]
    since = lambda c: lm.get(f"lm:{c}") if (f"JMA{c}" in have and f"JMA{c}" not in bf) else None
    with ThreadPoolExecutor(4) as ex:
        got = [g for g in ex.map(lambda c: fetch_pdf(c, since(c)), todo) if g[1]]
    for c, (y, no, en, pts, prov, rads), mod in got:
        con.execute("DELETE FROM points WHERE sid=?", (f"JMA{c}",))
        insert(con, f"JMA{c}", y, no, en, pts, int(prov))
        set_radii(con, f"JMA{c}", rads, pts[-1][0])
        con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (f"rb:JMA{c}", "1"))
        if mod:
            con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (f"lm:{c}", mod))
        con.execute("DELETE FROM meta WHERE k=?", (f"dt:JMA{c}",))  # 位置表PDFが載ったのでDT補完は不要
        _est_set(con, f"JMA{c}", set())
    print(f"JMA {yr}: PDF {len(got)}/{len(todo)} updated", flush=True)

TRACK_STEP_H = float(os.environ.get("TRACK_STEP_H", "3"))  # forecast.json の過去軌跡の点間隔（時間）。時刻は推定値
TRACK_MATCH_KM = float(os.environ.get("TRACK_MATCH_KM", "150"))  # 過去軌跡と実データの最初の点を「同じ場所」とみなす距離

def _est_get(con, sid):
    """時刻が推定値の点（過去軌跡由来）の時刻一覧。推定点は実データと区別して、毎回作り直す。"""
    r = con.execute("SELECT v FROM meta WHERE k=?", (f"est:{sid}",)).fetchone()
    try:
        return set(json.loads(r[0])) if r else set()
    except (TypeError, ValueError):
        return set()

def _est_set(con, sid, times):
    if times:
        con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (f"est:{sid}", json.dumps(sorted(times))))
    else:
        con.execute("DELETE FROM meta WHERE k=?", (f"est:{sid}",))

def tidy(p):
    """点列を時刻順にそろえる。同時刻の点は風速・気圧が入っている方を残し、
    前後の点と大きく食い違う孤立した1点（別ソース混在による位置の飛び）は捨てる。"""
    by = {}
    for x in sorted(p, key=lambda x: x[0]):
        o = by.get(x[0])
        if o is None or ((x[3] is not None or x[4] is not None) and o[3] is None and o[4] is None):
            by[x[0]] = x
    out = [by[k] for k in sorted(by)]
    i = 1
    while 0 < i < len(out) - 1:
        a, b, c = out[i - 1], out[i], out[i + 1]
        d1, d2 = _gc(a[1], a[2], b[1], b[2]), _gc(b[1], b[2], c[1], c[2])
        if min(d1, d2) > 400 and _gc(a[1], a[2], c[1], c[2]) < min(d1, d2) / 2:
            del out[i]
            continue
        i += 1
    return out

def est_back(real, trk):
    """forecast.json の過去軌跡（時刻なし）から、実データより前の部分だけを推定点として作る。
    軌跡上で実データの最初の点と同じ場所の点を探し、その手前だけを使う（時刻はそこから TRACK_STEP_H 時間刻みで遡る）。
    同じ場所が見つからなければ作らない。→ 実データの期間と重なって位置が行き戻りすることがない。"""
    if not real or not trk:
        return []
    la0, lo0 = real[0][1], real[0][2]
    j, d = min(((i, _gc(la, lo, la0, lo0)) for i, (la, lo) in enumerate(trk)), key=lambda x: x[1])
    if d > TRACK_MATCH_KM or j == 0:
        return []
    t0 = datetime.strptime(real[0][0][:19], F)
    return [((t0 - timedelta(hours=TRACK_STEP_H * (j - i))).strftime(F), la, lo, None, None)
            for i, (la, lo) in enumerate(trk[:j])]

def repair_once(con):
    """旧版は推定時刻つきの点を普通の点として保存していたため、DT・位置表が載った後も残って
    『進んだのに元の場所へ戻る』原因になった。速報扱いの台風から、その点（風速・気圧とも無い点）を一度だけ取り除く。"""
    if con.execute("SELECT 1 FROM meta WHERE k='trk_fix'").fetchone():
        return
    sel = "SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time"
    for sid, yr, no, en in con.execute("SELECT sid, year, number, name_en FROM typhoons WHERE sid LIKE 'JMA%' AND prov=1").fetchall():
        pts = [tuple(p) for p in con.execute(sel, (sid,)).fetchall()]
        keep = [p for i, p in enumerate(pts) if p[3] is not None or p[4] is not None or i == len(pts) - 1]
        if keep and len(keep) != len(pts):
            con.execute("DELETE FROM points WHERE sid=?", (sid,))
            insert(con, sid, yr, no, en, keep, 1)
        con.execute("DELETE FROM meta WHERE k=?", (f"lm:{sid[3:]}",))  # 位置表PDFを取り直す
    con.execute("INSERT OR REPLACE INTO meta VALUES('trk_fix','1')")

def get_json(url, default=None):
    try:
        r = requests.get(url, headers=UA, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"GET {url} failed:", repr(e), flush=True)
        return default

def _title_en(arr):
    t = next((r for r in arr if isinstance(r, dict) and r.get("part") == "title"), {})
    return ((t.get("name") or {}).get("en") or "").strip().upper()

def past_track(track):
    """forecast.json の実況 track（preTyphoon＋typhoon）→ [(lat, lon)]（古い順・連続重複除去）"""
    out = []
    if isinstance(track, dict):
        for k in ("preTyphoon", "typhoon"):
            for p in track.get(k) or []:
                try:
                    la, lo = float(p[0]), float(p[1])
                except (TypeError, ValueError, IndexError, KeyError):
                    continue
                if abs(la) <= 90 and abs(lo) <= 360 and (not out or out[-1] != (la, lo)):
                    out.append((la, lo))
    return out

def live_point(spec, fc=()):
    """発生中の台風 → (英語名, 実況点(時刻UTC, 緯度, 経度, 風速kt, 気圧), 過去軌跡[(lat, lon)])。実況が無ければ None。
    実況は specifications.json（風速・気圧つき）を優先し、無ければ forecast.json の実況(advancedHours=0)を使う。
    過去軌跡は forecast.json の実況に付いている track（時刻は持たない）。"""
    en = _title_en(spec) or _title_en(fc)
    pt, past = None, []
    for r in spec:
        p = r.get("part") if isinstance(r, dict) else None
        if isinstance(p, dict) and p.get("jp") == "実況":
            try:
                lat, lon = r["position"]["deg"][:2]
                w = num(((r.get("maximumWind") or {}).get("sustained") or {}).get("m/s"))
                t = datetime.strptime(r["validtime"]["UTC"][:19], "%Y-%m-%dT%H:%M:%S")
                pt = (t.strftime(F), float(lat), float(lon),
                      round(w * MS2KT) if w else None, num(r.get("pressure")) or None)
            except (KeyError, TypeError, ValueError, IndexError):
                pass
            break
    for r in fc:
        if isinstance(r, dict) and r.get("advancedHours") == 0:
            past = past_track(r.get("track"))
            if pt is None:
                try:
                    t = datetime.strptime(r["validtime"]["UTC"][:19], "%Y-%m-%dT%H:%M:%S")
                    pt = (t.strftime(F), float(r["center"][0]), float(r["center"][1]), None, None)
                except (KeyError, TypeError, ValueError, IndexError):
                    pass
            break
    return (en, pt, past) if pt else None

def _warn_area(w):
    """防災情報JSONの stormWarning / galeWarning → (向き, 長径km, 短径km)。形が想定と違えば None。
    実際の形は配列で、方向別なら要素が2つ、全方向なら1つ（暴風域が無い時は要素ごと出ない）:
      [{"area": "南", "range": {"km": 500, "nm": 270}}, {"area": "北", "range": {"km": 390, "nm": 210}}]
      [{"area": {"jp": "全域"}, "range": {"km": 140, "nm": 75}}]
    （方向は文字列のことも {"jp": ..} のこともある。旧実装の想定 {"areas": [{"radius", "direction"}]} も読める）"""
    if isinstance(w, list):
        areas = w
    elif isinstance(w, dict):
        areas = w.get("areas") if isinstance(w.get("areas"), list) else [w]
    else:
        return None
    pr = []
    for a in areas:
        if not isinstance(a, dict):
            continue
        rd = a.get("range", a.get("radius"))
        if isinstance(rd, dict):
            km = num(rd.get("km"))
            if km is None:
                nm = num(rd.get("nm"))
                km = nm * NM if nm else None
        else:
            km = num(rd)
        d = a.get("area", a.get("direction", a.get("dir")))
        if isinstance(d, dict):
            d = d.get("en") or d.get("jp")
        d = str(d or "").strip().replace("側", "")
        pr.append((DIR8.get(d.upper()) or JDIR8.get(d), km))   # 「全域」などは向きなし → 円
    return _pair_area(pr)

def live_radii(spec):
    """発生中の台風の実況 → (時刻UTC, 半径の値)。実況に半径が無い・読めなければ None。"""
    for r in spec:
        p = r.get("part") if isinstance(r, dict) else None
        if isinstance(p, dict) and p.get("jp") == "実況":
            try:
                t = datetime.strptime(r["validtime"]["UTC"][:19], "%Y-%m-%dT%H:%M:%S").strftime(F)
            except (KeyError, TypeError, ValueError):
                return None
            storm = gale = None
            for k, v in r.items():
                kl = str(k).lower()
                if "storm" in kl:
                    storm = _warn_area(v)
                elif "gale" in kl:
                    gale = _warn_area(v)
            rd = _rad_km(storm, gale)
            if rd is None:
                print("live radii: 半径を読み取れません keys =", sorted(r.keys()), flush=True)
            return (t, rd) if rd else None
    return None

def merge_live(con, sid, year, no, en, pt, past=()):
    """発生中の台風の実況を取り込む。
    ・時刻が確かな点（位置表PDF・デジタル台風・過去の実況）を土台にする。無ければ同名のIBTrACS由来の点
    ・最新の実況1点を末尾に追記する
    ・forecast.json の過去軌跡は時刻を持たないので、土台の最初の点より前の部分だけを推定点として補う。
      推定点は est: に記録し、毎回捨てて作り直す（位置表PDF・デジタル台風が載れば、そちらの確かな値で置き換わる）
    戻り値: 更新したか。"""
    sel = "SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time"
    cur = [tuple(p) for p in con.execute(sel, (sid,)).fetchall()]
    est = _est_get(con, sid)
    real = [p for p in cur if p[0] not in est]
    if not real and en:
        alt = con.execute("SELECT sid FROM typhoons WHERE year=? AND name_en=? AND sid NOT LIKE 'JMA%'",
                          (year, en)).fetchone()
        if alt:
            real = [tuple(p) for p in con.execute(sel, (alt[0],)).fetchall()]
    if not real or real[-1][0] < pt[0]:
        real.append(pt)
    back = est_back(real, list(past))
    new = tidy(back + real)
    if new == cur:
        return False
    con.execute("DELETE FROM points WHERE sid=?", (sid,))
    insert(con, sid, year, no, en, new, 1)
    _est_set(con, sid, {b[0] for b in back})
    return True

def refresh_live(con):
    """発生中の台風を気象庁の防災情報JSONから取り込む（位置表PDFの掲載は遅れるため）。
    注意: JSONのIDは TC2632 のように熱帯低気圧を含む通し番号で、台風の号数(typhoonNumber=2626)とは別。
    1つの台風の失敗で他を止めない。"""
    r = requests.get(f"{BOSAI}/targetTc.json", headers=UA, timeout=30)
    r.raise_for_status()
    lst = r.json()
    print("JMA live list:", [(t.get("tropicalCyclone"), t.get("typhoonNumber"), t.get("category")) for t in lst], flush=True)
    n = 0
    for t in lst:
        tn, tc = str(t.get("typhoonNumber", "")), t.get("tropicalCyclone")
        if not tc or not re.fullmatch(r"\d{4}", tn):  # 熱帯低気圧(号数なし)は対象外
            continue
        try:
            spec = get_json(f"{BOSAI}/{tc}/specifications.json", [])
            fc = get_json(f"{BOSAI}/{tc}/forecast.json", [])
            got = live_point(spec, fc)
            if not got:
                print(f"live {tc} (台風{tn}): 実況なし", flush=True)
                continue
            en, pt, past = got
            n += merge_live(con, f"JMA{tn}", 2000 + int(tn[:2]), int(tn[2:]), en, pt, past)
            rr = live_radii(spec)  # 実況の強風域・暴風域（その時刻の1行。更新のたびに積み上がる）
            if rr:
                ensure_radii(con)
                con.execute("DELETE FROM radii WHERE sid=? AND time=?", (f"JMA{tn}", rr[0]))
                con.execute("INSERT INTO radii VALUES(?,?,?,?,?,?,?,?)", (f"JMA{tn}", rr[0], *rr[1]))
        except Exception as e:
            print(f"live {tc} failed:", repr(e), flush=True)
    print(f"JMA live: {n} updated", flush=True)

def dt_track(no6):
    """デジタル台風のGeoJSON → (英語名, 点列[(時刻UTC, 緯度, 経度, 風速kt, 気圧)])。取れなければ None。
    6桁番号は 西暦4桁+気象庁の号数2桁（例 202625）。点は3時間刻みで、熱帯低気圧の段階から温帯低気圧化まで含む。"""
    j = get_json(f"{DT}/geojson/wnp/{no6}.en.json")
    if not isinstance(j, dict):
        return None
    pts = []
    for f in j.get("features") or []:
        try:
            p, (lon, lat) = f["properties"], f["geometry"]["coordinates"][:2]
            t = datetime.fromtimestamp(int(p["time"]), timezone.utc).strftime(F)
            pts.append((t, float(lat), float(lon), num(p.get("wind")) or None, num(p.get("pressure")) or None))
        except (KeyError, TypeError, ValueError):
            continue
    pts.sort()
    return ((j.get("properties") or {}).get("name") or "").strip().upper(), pts

def refresh_dt(con, yr):
    """位置表PDFが未掲載の台風（消滅直後など）を、デジタル台風から補う。
    対象: 今年の号数のうち ①DBに無いもの ②以前ここから補ったもの（PDFが載るまで追いかける）。
    PDF確定・速報が取れた台風は対象外。PDFが載れば refresh_pdf 側で置き換わる。"""
    yy = str(yr)[2:]
    html = requests.get(f"{DT}/year/wnp/{yr}.html.en", headers=UA, timeout=60).text
    nums = sorted({int(n) for y, n in re.findall(r"/(\d{4})(\d{2})\.html", html) if y == str(yr)})
    fin = {r[0] for r in con.execute("SELECT sid FROM typhoons WHERE sid LIKE 'JMA%' AND prov=0")}
    have = {r[0] for r in con.execute("SELECT sid FROM typhoons WHERE sid LIKE 'JMA%'")}
    pend = {r[0][3:] for r in con.execute("SELECT k FROM meta WHERE k LIKE 'dt:%'")}
    todo = [n for n in nums if (f"JMA{yy}{n:02d}" not in have or f"JMA{yy}{n:02d}" in pend)
            and f"JMA{yy}{n:02d}" not in fin]
    with ThreadPoolExecutor(3) as ex:
        got = list(ex.map(lambda n: (n, dt_track(f"{yr}{n:02d}")), todo))
    sel = "SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time"
    upd = 0
    for n, g in got:
        if not g or len(g[1]) < 2:
            continue
        en, pts = g
        sid = f"JMA{yy}{n:02d}"
        cur = [tuple(p) for p in con.execute(sel, (sid,)).fetchall()]
        est = _est_get(con, sid)
        old = [p for p in cur if p[0] not in est]  # 推定時刻の点はここで捨てる（残すと位置が行き戻りする）
        new = tidy(pts + [p for p in old if p[0] > pts[-1][0]])  # 防災情報JSONの実況がDTより新しければ残す
        con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (f"dt:{sid}", pts[-1][0]))
        if new != cur:
            con.execute("DELETE FROM points WHERE sid=?", (sid,))
            insert(con, sid, yr, n, en, new, 1)
            _est_set(con, sid, set())
            upd += 1
    print(f"DT {yr}: {len(todo)} target, {upd} updated", flush=True)

def dedupe(con):
    """気象庁データで置き換わった同名のIBTrACS由来データだけを消す（それ以外は残す）。"""
    dup = ("SELECT t.sid FROM typhoons t WHERE t.sid NOT LIKE 'JMA%' AND t.name_en<>'' AND EXISTS "
           "(SELECT 1 FROM typhoons j WHERE j.sid LIKE 'JMA%' AND j.year=t.year AND j.name_en=t.name_en)")
    con.execute(f"DELETE FROM points WHERE sid IN ({dup})")
    try:
        con.execute(f"DELETE FROM radii WHERE sid IN ({dup})")
    except sqlite3.OperationalError:
        pass  # 半径テーブルが無い旧DB
    con.execute(f"DELETE FROM typhoons WHERE sid IN ({dup})")

# ---------------- 特徴量（移動距離・復活・越境・ループ・急発達） ----------------
FEAT_V = "2"  # 判定ロジックを変えたら上げる（起動時に全件を再計算する）
# めずらしい台風の絞り込み条件（tv ビューの列）。複数指定は「全てに当てはまる」
SIZE_L, SIZE_XL = 500, 800  # 強風域の最大半径[km]。気象庁の階級: 大型=500km以上800km未満、超大型=800km以上
SIZES = {"l": f"(r30>={SIZE_L} AND r30<{SIZE_XL})", "xl": f"r30>={SIZE_XL}"}
TAGS = {"rev": "rev>0", "cin": "cin=1", "cout": "cout=1", "lp": "lp=1", "ri": "ri>=30"}
FEAT_SQL = """
CREATE TABLE IF NOT EXISTS feat(sid TEXT PRIMARY KEY, dist REAL, net REAL, rev INT, rev_a TEXT, rev_t TEXT,
    cin INT, cout INT, lp INT, ri INT, r30 REAL, r50 REAL);
CREATE VIEW IF NOT EXISTS tv AS SELECT t.*, f.dist, f.net, f.rev, f.rev_a, f.rev_t, f.cin, f.cout, f.lp, f.ri, f.r30, f.r50
    FROM typhoons t LEFT JOIN feat f ON f.sid = t.sid;
"""

def _gc(la1, lo1, la2, lo2):
    """2点間の大圏距離[km]"""
    p1, p2 = math.radians(la1), math.radians(la2)
    c = math.sin(p1) * math.sin(p2) + math.cos(p1) * math.cos(p2) * math.cos(math.radians((lo2 - lo1 + 540) % 360 - 180))
    return 6371.0 * math.acos(max(-1.0, min(1.0, c)))

def _brg(la1, lo1, la2, lo2):
    """1点目から2点目への方位[度]（-180〜180）"""
    p1, p2, d = math.radians(la1), math.radians(la2), math.radians((lo2 - lo1 + 540) % 360 - 180)
    return math.degrees(math.atan2(math.sin(d) * math.cos(p2),
                                   math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(d)))

def track_feats(p):
    """p: [(time_utc, lat, lon, wind_kt, pres), ...]（時刻順）→
    (移動距離km, 直線距離km, 復活の回数, 弱まる直前の時刻, 再び台風になった時刻, 越境(入), 域外へ(出), ループ, 24h最大増速kt)

    ・復活台風: 風速34kt(17.2m/s)以上 → 34kt未満の時間帯 → 再び34kt以上（号数は同じまま）。
      風速が空欄の点が続くだけの場合は、3点(約18時間)以上続いた時だけ「弱まった」とみなす。
    ・越境台風: 軌跡の最初の点が東経178度以東（=東経180度の東側から入ってきた）。
    ・域外へ: 最初は東経180度の西側にいて、のちに東経180度を越えて東側へ進んだ。
    ・ループ: 進路の向きの累積変化（時計回り・反時計回り）の幅が300度以上＝ほぼ1周した。
    ・急発達: 24時間で最大風速が30kt（約15m/s）以上強まった（ri に最大増速kt）。"""
    ts = [datetime.strptime(x[0][:19], F) for x in p]
    dist = sum(_gc(p[i - 1][1], p[i - 1][2], p[i][1], p[i][2]) for i in range(1, len(p)))
    net = _gc(p[0][1], p[0][2], p[-1][1], p[-1][2])
    # 復活
    rev, rev_a, rev_t = 0, None, None
    last, weak, unk, unk_max = None, 0, 0, 0
    for i, x in enumerate(p):
        w = x[3]
        if w is not None and w >= 34:
            if (last is not None and i - last > 1 and ts[i] - ts[last] >= timedelta(hours=12)
                    and (weak >= 1 or unk_max >= 3)):
                rev += 1
                if rev_t is None:
                    rev_a, rev_t = p[last][0], x[0]
            last, weak, unk, unk_max = i, 0, 0, 0
        elif last is not None:
            if w is None:
                unk += 1; unk_max = max(unk_max, unk)
            else:
                weak += 1; unk = 0
    # 急発達
    byt = {ts[i]: x[3] for i, x in enumerate(p) if x[3] is not None}
    ri = 0
    for t, w in byt.items():
        w2 = byt.get(t + timedelta(hours=24))
        if w2 is not None and w2 - w > ri:
            ri = w2 - w
    # 越境（経度は 0〜360 に直し、日付変更線をまたいでも連続になるよう補正）
    lu, prev = [], None
    for x in p:
        lo = x[2] % 360
        if prev is not None:
            while lo - prev > 180: lo -= 360
            while lo - prev < -180: lo += 360
        lu.append(lo); prev = lo
    cin = 1 if lu[0] >= 178.0 else 0
    cout = 1 if (not cin and max(lu) >= 180.0) else 0
    # ループ（30km未満の動きは向きが不安定なので飛ばす）
    cum = cmin = cmax = 0.0
    hp, a = None, p[0]
    for x in p[1:]:
        if _gc(a[1], a[2], x[1], x[2]) < 30:
            continue
        h = _brg(a[1], a[2], x[1], x[2])
        if hp is not None:
            cum += (h - hp + 540) % 360 - 180
            cmin, cmax = min(cmin, cum), max(cmax, cum)
        hp, a = h, x
    lp = 1 if cmax - cmin >= 300 else 0
    return (round(dist), round(net), rev, rev_a, rev_t, cin, cout, lp, int(round(ri)))

def tags_of(r):
    """DBの行 → めずらしい台風の種類 ['rev','cin',...]"""
    t = []
    if r.get("rev"): t.append("rev")
    if r.get("cin"): t.append("cin")
    if r.get("cout"): t.append("cout")
    if r.get("lp"): t.append("lp")
    if (r.get("ri") or 0) >= 30: t.append("ri")
    return t

def feat_update(con, full=False):
    """特徴量を points から計算して feat に保存する。
    通常は「未計算の台風」と「直近2年（速報で変わる）」だけ。判定ロジックの版(FEAT_V)が違えば全件。"""
    ver = (con.execute("SELECT v FROM meta WHERE k='feat_v'").fetchone() or [None])[0]
    if ver != FEAT_V:
        con.executescript("DROP VIEW IF EXISTS tv; DROP TABLE IF EXISTS feat;")
        full = True
    con.executescript(FEAT_SQL)
    if full:
        sids = [r[0] for r in con.execute("SELECT sid FROM typhoons")]
    else:
        top = con.execute("SELECT MAX(year) FROM typhoons").fetchone()[0] or 0
        sids = [r[0] for r in con.execute(
            "SELECT sid FROM typhoons WHERE year>=? OR sid NOT IN (SELECT sid FROM feat)", (top - 1,))]
    if sids:
        rows = con.execute("SELECT sid, time, lat, lon, wind, pres FROM points "
                           "WHERE sid IN (SELECT value FROM json_each(?)) ORDER BY sid, time", (json.dumps(sids),))
        try:  # 強風域・暴風域の最大半径（km）。半径テーブルが無い旧DBでは空
            rmax = {r[0]: (r[1], r[2]) for r in con.execute("SELECT sid, MAX(l30), MAX(l50) FROM radii GROUP BY sid")}
        except sqlite3.OperationalError:
            rmax = {}
        out = [(sid, *track_feats([r[1:] for r in g]), *(round(v * NM) if v else None for v in rmax.get(sid, (None, None))))
               for sid, g in groupby(rows, key=lambda r: r[0])]
        con.executemany("INSERT OR REPLACE INTO feat VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", out)
    con.execute("DELETE FROM feat WHERE sid NOT IN (SELECT sid FROM typhoons)")
    con.execute("INSERT OR REPLACE INTO meta VALUES('feat_v',?)", (FEAT_V,))

def feat_init():
    """旧バージョンで作ったDBに特徴量が無い場合の移行。起動時に1回だけ全件を計算する（数秒）。"""
    try:
        con = sqlite3.connect(DB, timeout=60)
        try:
            feat_update(con); con.commit()
        finally:
            con.close()
    except Exception as e:
        print("feat_init failed:", repr(e), flush=True)

def featdiag():
    """python app.py featdiag : 復活・越境などの判定結果の一覧。気象庁の公表と見比べて確認する用。"""
    con = sqlite3.connect(DB, timeout=60)
    feat_update(con); con.commit()
    names = {"rev": "復活台風", "cin": "越境台風（東経180度の東から入った）", "cout": "東経180度を越えて東へ抜けた",
             "lp": "ループ・迷走", "ri": "急発達（24時間で30kt以上）"}
    for k, cond in TAGS.items():
        rows = con.execute(f"SELECT title, rev, rev_a, rev_t, ri FROM tv WHERE {cond} ORDER BY year, number").fetchall()
        print(f"\n== {names[k]}: {len(rows)}個")
        for t, n, a, b, ri in rows:
            extra = f"  復活{n}回 {jst(a):%m/%d %H時}→{jst(b):%m/%d %H時}（日本時間）" if k == "rev" else (f"  +{ri}kt/24h" if k == "ri" else "")
            print("  ", t + extra)
    n = con.execute("SELECT COUNT(*) FROM typhoons").fetchone()[0]
    print(f"\n台風 {n}個 / 移動距離の平均 {con.execute('SELECT AVG(dist) FROM feat').fetchone()[0]:.0f}km")
    con.close()

def refresh_recent():
    """今年の台風を取り込む。PDFとJSONは別々に失敗しても、取れたぶんで動かす。"""
    con, ok = None, False
    try:
        con = sqlite3.connect(DB, timeout=60)
        try:
            repair_once(con)
        except Exception as e:
            print("repair_once failed:", repr(e), flush=True)
        now = datetime.now(timezone(timedelta(hours=9)))
        for yr in [now.year] + ([now.year - 1] if now.month == 1 else []):  # 1月は前年の台風も継続しうる
            try:
                refresh_pdf(con, yr); ok = True
            except Exception as e:
                print(f"JMA table {yr} failed:", repr(e), flush=True)
            try:
                refresh_dt(con, yr); ok = True
            except Exception as e:
                print(f"DT {yr} failed:", repr(e), flush=True)
        try:
            refresh_live(con); ok = True
        except Exception as e:
            print("JMA live failed:", repr(e), flush=True)
        dedupe(con)
        try:
            feat_update(con)
        except Exception as e:
            print("feat failed:", repr(e), flush=True)
        if ok:
            con.execute("INSERT OR REPLACE INTO meta VALUES('jma_at',?)", (datetime.now(timezone.utc).strftime(F),))
        con.commit()
    except Exception as e:
        print("refresh_recent failed:", repr(e), flush=True)
    finally:
        if con: con.close()

def diag():
    """python app.py diag : 台風が取れない時の切り分け用。JMA側の状態とDBの中身を表示する。"""
    now = datetime.now(timezone(timedelta(hours=9))); yy = str(now.year)[2:]
    print("== targetTc.json")
    for t in get_json(f"{BOSAI}/targetTc.json", []):
        tc = t.get("tropicalCyclone"); print(t)
        got = live_point(get_json(f"{BOSAI}/{tc}/specifications.json", []), get_json(f"{BOSAI}/{tc}/forecast.json", []))
        print("   ->", got and (got[0], got[1], f"過去軌跡{len(got[2])}点"))
    print("== 位置表PDF")
    for n in range(max(1, int(os.environ.get("DIAG_FROM", "20"))), 36):
        try:
            r = requests.get(f"{JMA}/data/T{yy}{n:02d}.pdf", headers=UA, timeout=30, stream=True); r.close()
            print(f"T{yy}{n:02d}.pdf", r.status_code, r.headers.get("Last-Modified", ""))
        except Exception as e:
            print(f"T{yy}{n:02d}.pdf", repr(e))
    print("== デジタル台風（PDF未掲載の補完元）")
    for n in range(20, 36):
        g = dt_track(f"{now.year}{n:02d}")
        if g: print(f"{now.year}{n:02d}", g[0], f"{len(g[1])}点", g[1][-1][0] if g[1] else "")
    print("== DB")
    con = sqlite3.connect(DB)
    for r in con.execute("SELECT sid, number, title, prov, start_time, end_time, (SELECT COUNT(*) FROM points p WHERE p.sid=t.sid) "
                         "FROM typhoons t WHERE year=? ORDER BY number", (now.year,)):
        print(r)
    con.close()

# ---------------- 予報モデル比較：取得元の診断 ----------------
RAL = "https://hurricanes.ral.ucar.edu/realtime"  # UCAR RAL TCGP：台風ごとの ATCF a-deck（各機関の予報進路）を公開
# a-deck の技術ID → モデル名の「推定」。実データでの確認前なので、fdiag の出力を見て本実装で確定する
GUESS = {"JTWC": "JTWC公式", "AVNO": "GFS", "AVNI": "GFS(補間)", "GFSO": "GFS", "EMX": "ECMWF", "EMXI": "ECMWF(補間)",
         "ECMF": "ECMWF", "UKM": "UKMET", "UKMI": "UKMET(補間)", "UKX": "UKMET", "UKXI": "UKMET(補間)",
         "JGSM": "JMA-GSM", "JGSI": "JMA-GSM(補間)", "NVGM": "NAVGEM", "NGX": "NAVGEM", "CTCX": "COAMPS-TC",
         "HWRF": "HWRF", "HAFS": "HAFS", "BEST": "ベストトラック"}

def parse_adeck(text):
    """ATCF a-deck → [(初期時刻yyyymmddhh, 技術ID, 予報時間tau[h], 緯度, 経度, 風速kt, 気圧hPa)]。壊れた行は捨てる。"""
    out = []
    for ln in text.splitlines():
        c = [x.strip() for x in ln.split(",")]
        if len(c) < 10 or not c[2].isdigit() or not c[5].lstrip("-").isdigit():
            continue
        try:
            la = int(c[6][:-1]) / 10 * (-1 if c[6][-1] == "S" else 1)
            lo = int(c[7][:-1]) / 10 * (-1 if c[7][-1] == "W" else 1)
        except (ValueError, IndexError):
            continue
        v, p = (int(x) if x.lstrip("-").isdigit() and int(x) > 0 else None for x in (c[8], c[9]))
        out.append((c[2], c[4], int(c[5]), la, lo, v, p))
    return out

def fdiag(P=print, storms=None):
    """python app.py fdiag : 予報比較の取得元を確認する（JMA予報円・a-deckに載っているモデルID・更新時刻）。
    環境変数 FDIAG_STORMS="wp25,wp26"（または /api/fdiag?storms=wp25）で対象を指定できる（未指定なら UCAR の「現在の活動中」ページから自動検出）。"""
    P("== JMA 予報（防災情報JSON forecast.json）")
    for t in get_json(f"{BOSAI}/targetTc.json", []):
        tc = t.get("tropicalCyclone")
        fc = get_json(f"{BOSAI}/{tc}/forecast.json", [])
        pts = [(r.get("advancedHours"), r.get("center")) for r in fc if isinstance(r, dict) and r.get("advancedHours") is not None]
        P(tc, t.get("typhoonNumber"), t.get("category"), f"予報{len(pts)}点", pts[:8])
    P("== UCAR RAL a-deck")
    yr = datetime.now(timezone.utc).year
    ids = [x.strip().lower() for x in (storms or os.environ.get("FDIAG_STORMS", "")).split(",") if x.strip()]
    if not ids:
        try:
            html = requests.get(f"{RAL}/current/", headers=UA, timeout=30).text
            ids = sorted(set(re.findall(r"northwestpacific/\d{4}/(wp\d{2})\d{4}/", html)))
        except Exception as e:
            P("活動中ページの取得に失敗:", repr(e))
    P("対象:", ids or "なし（FDIAG_STORMS=wp25 のように指定できます）")
    for sid in ids:
        url = f"{RAL}/plots/northwestpacific/{yr}/{sid}{yr}/a{sid}{yr}.dat"
        try:
            r = requests.get(url, headers=UA, timeout=60)
        except Exception as e:
            P(sid, "取得失敗", repr(e)); continue
        P(f"\n{sid}: HTTP {r.status_code}  Last-Modified: {r.headers.get('Last-Modified', '-')}  {len(r.content)//1024}KB\n  {url}")
        rows = parse_adeck(r.text) if r.ok else []
        if not rows:
            P("  パースできる行なし"); continue
        latest = max(x[0] for x in rows)
        P(f"  最新の初期時刻: {latest} UTC ／ 行数 {len(rows)} ／ 初期時刻の種類 {len({x[0] for x in rows})}")
        P("  技術ID  推定モデル      初期時刻数  最新初期時刻  最新の点数  最大tau  +72h位置")
        for tech in sorted({x[1] for x in rows}):
            mine = [x for x in rows if x[1] == tech]
            li = max(x[0] for x in mine); cur = [x for x in mine if x[0] == li]
            p72 = next(((x[3], x[4]) for x in cur if x[2] == 72), None)
            P(f"  {tech:<7} {GUESS.get(tech, '?'):<14} {len({x[0] for x in mine}):>8}  {li:>12}  {len(cur):>8}  {max(x[2] for x in cur):>6}  {p72 or '-'}")
    P("\n== Google DeepMind Weather Lab（AIアンサンブル）")
    now = datetime.now(timezone.utc)
    base = now.replace(hour=now.hour // 6 * 6, minute=0, second=0, microsecond=0)
    for model in WL_MODELS:
        for i in range(6):
            cyc = base - timedelta(hours=6 * i)
            url = WL_URL.format(model=model, kind="ensemble", ts=cyc.strftime("%Y_%m_%dT%H_00"))
            try:
                r = requests.get(url, headers=UA, timeout=60)
            except Exception as e:
                P(model, cyc.strftime("%m/%d %HZ"), "取得失敗", repr(e)); break
            if r.status_code != 200 or r.text.lstrip()[:1] == "<":
                P(model, cyc.strftime("%m/%d %HZ"), f"HTTP {r.status_code}"); continue
            lines = r.text.splitlines()
            g = wl_parse(r.text)
            P(f"{model} {cyc.strftime('%m/%d %HZ')} OK {len(r.content)//1024}KB  {url}")
            P("  列名:", lines[0][:300])
            P("  1行目:", lines[1][:300] if len(lines) > 1 else "-")
            P("  パース結果:", {k: len(v) for k, v in list(g.items())[:12]}, "（台風ID: メンバー数）")
            break

# ---------------- API ----------------
@asynccontextmanager
async def lifespan(_):
    if not os.path.exists(DB):
        await asyncio.to_thread(build)
    else:
        await asyncio.to_thread(feat_init)  # 旧DBに特徴量テーブルが無ければここで作る

    async def loop():
        while True:  # 速報値・発生中の台風を定期的に再取得（既定60分。REFRESH_MINUTESで変更可）
            await asyncio.to_thread(refresh_recent)
            await asyncio.sleep(INTERVAL)
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

_fd = {"t": 0, "k": None, "txt": ""}
@app.get("/api/fdiag")
def api_fdiag(storms: str = ""):
    """予報比較の取得元診断をブラウザで見る用（Renderの無料プランはShellが使えないため）。5分間は結果を使い回す。"""
    from fastapi.responses import PlainTextResponse
    import time
    if _fd["k"] != storms or time.time() - _fd["t"] > 300:
        out = []
        try: fdiag(lambda *a: out.append(" ".join(str(x) for x in a)), storms)
        except Exception as e: out.append(f"エラー: {e!r}")
        _fd.update(t=time.time(), k=storms, txt="\n".join(out))
    return PlainTextResponse(_fd["txt"])

@app.get("/api/years")
def years():
    r = q("SELECT MIN(year) AS min, MAX(year) AS max FROM typhoons")[0]
    for k in ("built_at", "jma_at"):
        r[k] = (q("SELECT v FROM meta WHERE k=?", (k,)) or [{"v": ""}])[0]["v"]
    return r

SORTS = {"number": "year {d}, number {d}", "date": "start_time {d}",
         "wind": "max_wind IS NULL, max_wind {d}", "pres": "min_pres IS NULL, min_pres {d}",
         "days": "days {d}", "dist": "dist IS NULL, dist {d}", "size": "r30 IS NULL, r30 {d}", "name": "name_en = '', name_en {d}"}

import math, json
from fastapi import Depends
from fastapi.responses import Response, JSONResponse, RedirectResponse

DIRB = {1: 45, 2: 90, 3: 135, 4: 180, 5: 225, 6: 270, 7: 315, 8: 0}  # 向きコード→方位[度]

def _dest(la, lo, b, km):
    d, t, p1 = km / 6371.0, math.radians(b), math.radians(la)
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(t))
    dl = math.atan2(math.sin(t) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2))
    return math.degrees(p2), lo + math.degrees(dl)

def zone_ids(la, lo, key):
    """指定地点が 暴風域(r50)・強風域(r30) に入った台風のsid。半径は(中心のずれ, 半径)の円として判定し、
    隣り合う観測（12時間以内）の間は円を直線的に補間して見る。"""
    d, l, sh = ("d50", "l50", "s50") if key == "r50" else ("d30", "l30", "s30")
    try:
        rows = q(f"SELECT r.sid, r.time, p.lat, p.lon, r.{d} AS d, r.{l} AS l, r.{sh} AS s FROM radii r "
                 f"JOIN points p ON p.sid=r.sid AND p.time=r.time WHERE r.{l}>0 AND p.lat BETWEEN ? AND ? "
                 f"ORDER BY r.sid, r.time", (la - 25, la + 25))
    except sqlite3.OperationalError:
        return set()  # 半径テーブルが無い旧DB
    ids, prev = set(), None
    for r in rows:
        sid = r["sid"]
        if sid in ids:
            prev = None
            continue
        lg = r["l"] * NM; s2 = min(r["s"] or r["l"], r["l"]) * NM
        off = (lg - s2) / 2 if (r["d"] in DIRB and s2 < lg) else 0
        c = _dest(r["lat"], r["lon"], DIRB[r["d"]], off) if off else (r["lat"], r["lon"])
        cur = (sid, datetime.strptime(r["time"][:19], F), c[0], c[1], (lg + s2) / 2 if off else lg)
        if _gc(la, lo, cur[2], cur[3]) <= cur[4]:
            ids.add(sid); prev = None
            continue
        if prev and prev[0] == sid and (cur[1] - prev[1]).total_seconds() <= 12 * 3600:
            dlo = (cur[3] - prev[3] + 540) % 360 - 180
            for k in range(1, 6):
                t = k / 6
                if _gc(la, lo, prev[2] + (cur[2] - prev[2]) * t, prev[3] + dlo * t) <= prev[4] + (cur[4] - prev[4]) * t:
                    ids.add(sid)
                    break
        prev = cur
    return ids

def flt(name: str | None = None, year_from: int | None = None, year_to: int | None = None,
        month: int | None = None, wind_min: float | None = None, wind_max: float | None = None,
        pres_max: float | None = None, days_min: float | None = None, named: bool = False,
        dist_min: float | None = None, dist_max: float | None = None, tag: str | None = None,
        near: str | None = None, size: str | None = None):
    """絞り込み条件（一覧・重ね表示・統計で共通）→ (WHERE句, 引数)。near は 'lat,lon,km'（km の代わりに r30=強風域に入った / r50=暴風域に入った）"""
    where, args = [], []
    def add(c, v): where.append(c); args.append(v)
    if name and name.strip():
        where.append("(title LIKE ? OR name_en LIKE ?)")
        args += [f"%{name.strip()}%", f"%{name.strip().upper()}%"]
    if year_from is not None: add("year>=?", year_from)
    if year_to is not None: add("year<=?", year_to)
    if month is not None: add("month=?", month)
    if wind_min is not None: add(f"CAST(max_wind*{KT2MS}+0.5 AS INTEGER)>=?", wind_min)  # m/s指定
    if wind_max is not None: add(f"CAST(max_wind*{KT2MS}+0.5 AS INTEGER)<=?", wind_max)
    if pres_max is not None: add("min_pres<=?", pres_max)
    if days_min is not None: add("days>=?", days_min)
    if named: where.append("name_en<>''")
    if dist_min is not None: add("dist>=?", dist_min)  # 移動距離[km]（各点を結んだ道のり）
    if dist_max is not None: add("dist<=?", dist_max)
    for t in (tag or "").split(","):  # めずらしい台風（復活 rev / 越境 cin / 域外へ cout / ループ lp / 急発達 ri）
        t = t.strip()
        if not t: continue
        if t not in TAGS: raise HTTPException(422, "tag は rev,cin,cout,lp,ri のいずれかで指定してください")
        where.append(TAGS[t])
    if size:  # 大きさ（生涯の最大の強風域半径）: l=大型 / xl=超大型（カンマ区切りで両方）
        ks = [x.strip() for x in size.split(",") if x.strip()]
        if not ks or any(x not in SIZES for x in ks): raise HTTPException(422, "size は l,xl のいずれか（カンマ区切り可）で指定してください")
        where.append("(" + " OR ".join(SIZES[x] for x in ks) + ")")
    if near:
        pt = near.split(",")
        try: la, lo, zk = float(pt[0]), float(pt[1]), pt[2].strip()
        except (ValueError, IndexError): raise HTTPException(422, "near は lat,lon,km の形式で指定してください")
        if zk in ("r30", "r50"):
            where.append("sid IN (SELECT value FROM json_each(?))"); args.append(json.dumps(sorted(zone_ids(la, lo, zk))))
            return (("WHERE " + " AND ".join(where)) if where else ""), args
        try: km = float(zk)
        except ValueError: raise HTTPException(422, "near は lat,lon,km の形式で指定してください")
        dla, dlo = km / 111.0, km / (111.0 * max(0.05, math.cos(math.radians(la))))
        ids = set()
        for sh in (0, 360, -360):  # 経度が -180..180 でも 0..360 でも拾えるように
            for r in q("SELECT sid, lat, lon FROM points WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?",
                       (la - dla, la + dla, lo + sh - dlo, lo + sh + dlo)):
                if r["sid"] in ids: continue
                p1, p2, d = math.radians(r["lat"]), math.radians(la), math.radians((r["lon"] - lo + 540) % 360 - 180)
                c = math.sin(p1) * math.sin(p2) + math.cos(p1) * math.cos(p2) * math.cos(d)
                if 6371 * math.acos(max(-1.0, min(1.0, c))) <= km: ids.add(r["sid"])
        where.append("sid IN (SELECT value FROM json_each(?))"); args.append(json.dumps(sorted(ids)))
    return (("WHERE " + " AND ".join(where)) if where else ""), args

@app.get("/api/typhoons")
def typhoons(fl=Depends(flt), sort: str = "number", order: str = "desc", limit: int = Query(300, le=1000)):
    w, args = fl
    ob = SORTS.get(sort, SORTS["number"]).format(d="ASC" if order == "asc" else "DESC")
    total = q(f"SELECT COUNT(*) AS n FROM tv {w}", args)[0]["n"]
    rows = q(f"SELECT * FROM tv {w} ORDER BY {ob} LIMIT ?", (*args, limit))
    for r in rows:
        r["tg"] = tags_of(r)
        r["max_wind"] = to_ms(r["max_wind"])
        r["ri"] = to_ms(r["ri"]) if r["ri"] else 0  # 24時間の最大増速（m/s）
    return {"total": total, "rows": rows}

@app.get("/api/tracks")
def tracks(fl=Depends(flt), limit: int = Query(300, le=600)):
    """条件に合う台風の進路をまとめて返す（地図への重ね表示用。点は間引く）"""
    w, args = fl
    sub = f"SELECT sid, title, max_wind FROM tv {w} ORDER BY year DESC, number DESC LIMIT ?"
    ts = {r["sid"]: {"sid": r["sid"], "title": r["title"], "w": to_ms(r["max_wind"]), "p": []} for r in q(sub, (*args, limit))}
    for r in q(f"SELECT sid, lat, lon FROM points WHERE sid IN (SELECT sid FROM ({sub})) ORDER BY sid, time", (*args, limit)):
        ts[r["sid"]]["p"].append([round(r["lat"], 2), round(r["lon"], 2)])
    for t in ts.values(): t["p"] = t["p"][::2] + ([t["p"][-1]] if len(t["p"]) % 2 == 0 else [])
    return list(ts.values())

def _ci(v):
    """風速(m/s) → 強さ階級の番号（0=猛烈な 1=非常に強い 2=強い 3=台風 4=それ以下・不明）"""
    return 4 if v is None else 0 if v >= 54 else 1 if v >= 44 else 2 if v >= 33 else 3 if v >= 17 else 4

def _pace():
    """今年の発生ペース。平年（1991〜2020年）の「同じ月日までの発生数」と比べる。絞り込み条件とは無関係。
    平年のぶんのデータが足りなければ None。"""
    now = datetime.now(timezone.utc) + timedelta(hours=9)
    y, md = now.year, now.strftime("%m-%d")
    cnt = {}  # 年 -> [同じ月日までの数, 年間の数]
    for r in q("SELECT year, substr(datetime(start_time,'+9 hours'),6,5) AS md FROM typhoons WHERE year<=?", (y,)):
        c = cnt.setdefault(r["year"], [0, 0])
        c[1] += 1
        if r["md"] <= md: c[0] += 1
    base = range(1991, 2021)
    if sum(1 for k in base if k in cnt) < 25: return None
    n = cnt.get(y, [0, 0])[0]
    others = [v[0] for k, v in cnt.items() if k != y]
    return {"year": y, "md": f"{now.month}/{now.day}", "n": n,
            "normal": round(sum(cnt.get(k, [0, 0])[0] for k in base) / 30, 1),
            "full_normal": round(sum(cnt.get(k, [0, 0])[1] for k in base) / 30, 1),
            "rank": 1 + sum(1 for v in others if v > n), "of": len(others) + 1}

@app.get("/api/stats")
def stats(fl=Depends(flt), year_from: int | None = None, year_to: int | None = None):
    """統計タブ用の集計。絞り込み条件は一覧と共通。"""
    w, args = fl
    rows = q(f"SELECT sid, year, month, max_wind, min_pres, days, title, dist, r30, rev, cin, cout, lp, ri FROM tv {w}", args)
    span = q("SELECT MIN(year) AS a, MAX(year) AS b FROM typhoons")[0]
    lo, hi = (year_from or span["a"]), (year_to or span["b"])
    cur = (datetime.now(timezone.utc) + timedelta(hours=9)).year  # 途中の年は平均から外すため、クライアントへ渡す
    yr = {y: [0, 0, 0, 0.0, 0] for y in range(lo, hi + 1)} if lo is not None and hi is not None else {}  # 年 -> [数, 強い以上, 非常に強い以上, 風速の合計, 風速ありの数]
    mon = [[0] * 5 for _ in range(12)]
    cls, hist = {}, {b: 0 for b in [0] + list(range(15, 71, 5))}
    for r in rows:
        v = to_ms(r["max_wind"]); c = _ci(v)
        e = yr.setdefault(r["year"], [0, 0, 0, 0.0, 0])
        e[0] += 1
        if c <= 2: e[1] += 1
        if c <= 1: e[2] += 1
        if v is not None:
            e[3] += v; e[4] += 1
            hist[0 if v < 15 else min(70, v // 5 * 5)] += 1
        if r["month"] and 1 <= r["month"] <= 12: mon[r["month"] - 1][c] += 1
        k = "不明" if v is None else next(n for t, n in ((54, "猛烈な"), (44, "非常に強い"), (33, "強い"), (17, "台風"), (0, "熱帯低気圧")) if v >= t)
        cls[k] = cls.get(k, 0) + 1
    ds = [r["days"] for r in rows if r["days"] is not None]
    dd = [r["dist"] for r in rows if r["dist"] is not None]
    full = [v[0] for y, v in yr.items() if y != cur]  # 年平均は、まだ終わっていない今年を除く
    tg = {k: 0 for k in TAGS}
    sz = {"xl": 0, "l": 0, "n": 0, "x": 0}  # 超大型 / 大型 / 大型未満 / 半径データなし
    for r in rows:
        for k in tags_of(r): tg[k] += 1
        k30 = r["r30"]; sz["x" if not k30 else "xl" if k30 >= SIZE_XL else "l" if k30 >= SIZE_L else "n"] += 1
    def top(items, val, **ex):
        return [{"sid": r["sid"], "title": r["title"], "v": val(r), **{k: f(r) for k, f in ex.items()}} for r in items[:10]]
    wv = sorted((r for r in rows if r["max_wind"] is not None), key=lambda r: (-r["max_wind"], r["min_pres"] or 9999))
    pv = sorted((r for r in rows if r["min_pres"]), key=lambda r: (r["min_pres"], -(r["max_wind"] or 0)))
    rec = {"wind": top(wv, lambda r: to_ms(r["max_wind"]), p=lambda r: round(r["min_pres"]) if r["min_pres"] else None),
           "pres": top(pv, lambda r: round(r["min_pres"]), w=lambda r: to_ms(r["max_wind"])),
           "days": top(sorted((r for r in rows if r["days"] is not None), key=lambda r: -r["days"]), lambda r: r["days"]),
           "dist": top(sorted((r for r in rows if r["dist"] is not None), key=lambda r: -r["dist"]), lambda r: round(r["dist"])),
           "size": top(sorted((r for r in rows if r["r30"]), key=lambda r: -r["r30"]), lambda r: round(r["r30"]))}
    return {"total": len(rows), "classes": cls, "tags": tg, "sizes": sz, "months": [sum(m) for m in mon], "mon_cls": mon,
            "years": [[y, *v[:3], round(v[3], 1), v[4]] for y, v in sorted(yr.items())],
            "wind_hist": sorted(hist.items()), "cur_year": cur,
            "avg_per_year": round(sum(full) / len(full), 1) if full else None,
            "avg_days": round(sum(ds) / len(ds), 1) if ds else None,
            "avg_dist": round(sum(dd) / len(dd)) if dd else None,
            "rec": rec, "pace": _pace()}

# ---- PWA（ホーム画面に追加・オフライン時は直近のデータを表示）----
ICON = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><rect width="512" height="512" fill="#0a1120"/>'
        '<g fill="none" stroke="#38bdf8" stroke-linecap="round" stroke-width="32"><circle cx="256" cy="256" r="26" fill="#38bdf8"/>'
        '<path d="M256 170a86 86 0 0 1 86 86M256 342a86 86 0 0 1-86-86M256 110a146 146 0 0 1 146 146M256 402a146 146 0 0 1-146-146" opacity=".85"/></g></svg>')
SW = r"""const V="tf-v3";
self.addEventListener("install",e=>{e.waitUntil(caches.open(V).then(c=>c.addAll(["/"])));self.skipWaiting()});
self.addEventListener("activate",e=>e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!=V).map(x=>caches.delete(x)))).then(()=>clients.claim())));
self.addEventListener("fetch",e=>{const r=e.request,u=new URL(r.url);
  if(r.method!="GET"||/arcgisonline/.test(u.host)||u.pathname.startsWith("/api/live/geo/"))return;  // 地図タイル・警報の塗りの形（大きい）はキャッシュしない
  e.respondWith(fetch(r).then(x=>{if(x.ok||x.type=="opaque"){const c=x.clone();caches.open(V).then(k=>k.put(r,c))}return x}).catch(()=>caches.match(r)))});
"""
@app.get("/manifest.webmanifest")
def manifest():
    return JSONResponse({"name": "台風データベース", "short_name": "台風DB", "start_url": "/", "display": "standalone",
                         "background_color": "#0a1120", "theme_color": "#0a1120",
                         "icons": [{"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any maskable"}]},
                        media_type="application/manifest+json")
@app.get("/icon.svg")
def icon(): return Response(ICON, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})
@app.get("/sw.js")
def sw(): return Response(SW, media_type="application/javascript", headers={"Cache-Control": "no-cache"})

@app.get("/api/typhoons/{sid}")
def detail(sid: str):
    t = q("SELECT * FROM tv WHERE sid=?", (sid,))
    if not t: raise HTTPException(404, "台風が見つかりません")
    track = q("SELECT time, lat, lon, wind, pres FROM points WHERE sid=? ORDER BY time", (sid,))
    try:  # 強風域・暴風域（半径は海里→km）。データがある台風・時刻だけに付く。DB再構築前の旧DBでは空
        rd = {x["time"]: [x["d50"], x["l50"], x["s50"], x["d30"], x["l30"], x["s30"]]
              for x in q("SELECT * FROM radii WHERE sid=?", (sid,))}
    except sqlite3.OperationalError:
        rd = {}
    for p in track:
        p["wind"] = to_ms(p["wind"])
        if p["time"] in rd:
            x = rd[p["time"]]
            p["r"] = [x[0], *(None if v is None else round(v * 1.852) for v in x[1:3]),
                      x[3], *(None if v is None else round(v * 1.852) for v in x[4:6])]
    r = t[0]; r["tg"] = tags_of(r)
    return {**r, "max_wind": to_ms(r["max_wind"]), "ri": to_ms(r["ri"]) if r["ri"] else 0, "track": track,
            "has_r": any("r" in p for p in track)}

# ---------------- 予報モデル比較（別タブ /forecast）----------------
import time as _t
_fc_cache = {}
def _cached(key, ttl, fn):
    v = _fc_cache.get(key)
    if v and _t.time() - v[0] < ttl: return v[1]
    r = fn(); _fc_cache[key] = (_t.time(), r); return r

def ll_dist(a, b):
    """2点間の距離[km]"""
    r = math.radians
    c = math.sin(r(a[0])) * math.sin(r(b[0])) + math.cos(r(a[0])) * math.cos(r(b[0])) * math.cos(r((a[1] - b[1] + 540) % 360 - 180))
    return 6371 * math.acos(max(-1.0, min(1.0, c)))

# a-deck の技術ID。実データ（fdiag）で確認できたもの: UKM=UKMET, NGX=NAVGEM, CMC=CMC。
# アンサンブルは ATCF の慣例: GEFS=AC00/AP01-30/AEMN、CMC=CC00/CP01-20/CEMN、NAVGEM=NC00/NP01-20/NEMN
DET = {"UKM": "UKMET", "NGX": "NAVGEM", "CMC": "CMC", "JTWC": "JTWC公式", "AVNO": "GFS", "GFSO": "GFS",
       "EMX": "ECMWF", "ECMF": "ECMWF", "JGSM": "JMA-GSM", "HWRF": "HWRF", "HAFS": "HAFS", "CTCX": "COAMPS-TC"}
# アンサンブルの技術ID（ATCF慣例）: 先頭1文字=機関、メンバー=xP01〜、コントロール=xC00/xE00、平均=xEMN
#   A=GEFS(NCEP) C=CMC-EPS N=NAVGEM-EPS E=ECMWF-ENS U=UKMET(MOGREPS) J=JMA-GEPS
# a-deck に載っているものを自動検出する（載っていない機関は黙って飛ばし、画面の注記に「無いモデル」として出す）
ENS_LET = {"A": "GEFS", "C": "CMC-EPS", "N": "NAVGEM-EPS", "E": "ECMWF-ENS", "U": "UKMET-ENS", "J": "JMA-GEPS"}
ENS_ID = re.compile(r"([A-Z])(P\d\d|C00|E00)")
# a-deck に載っている「上のDETに無いモデル」も自動で拾う。既知の名前だけ日本語表記にし、それ以外は技術IDのまま出す
CON_ID = {"TVCN": "合成TVCN", "TVCX": "合成TVCX", "GUNA": "合成GUNA", "CONW": "合成CONW", "TCON": "合成TCON", "ICNW": "合成ICNW"}
EXTRA_NAME = {"COTC": "COAMPS-TC(COTC)", "HWFI": "HWRF(補間)", "HMON": "HMON", "ECM2": "ECMWF(ECM2)", "EGRR": "UKMET(EGRR)",
              "NVGM": "NAVGEM(NVGM)", "GFDN": "GFDN", "AEMN": "GEFS平均"}
SKIP_TECH = {"BEST", "CARQ", "WRNG", "TCLP", "SHIP", "DSHP", "LGEM", "SHFR", "SHF5", "OFCL", "OFCI", "NAME", "SHNS", "DRCL", "OCD5", "XTRP", "CLIP", "CLP5"}
EXTRA_MAX = 10   # 自動検出で足すモデルの上限（画面が混まないように）

# Google DeepMind Weather Lab（AIの台風アンサンブル。WNV3=64メンバー、FNV3=50メンバー、GENC=GenCast）
# URLは環境変数 WEATHERLAB_URL で差し替え可（{model} {kind} {ts} が置換される）。/api/fdiag で取得状況とCSVの列名を確認できる
WL_URL = os.environ.get("WEATHERLAB_URL",
    "https://deepmind.google.com/science/weatherlab/download/cyclones/{model}/{kind}/paired/csv/{model}_{kind}_{ts}_paired.csv")
WL_MODELS = {"WNV3": "WeatherNext3(AI)", "FNV3": "WeatherNext2(AI)", "GENC": "GenCast(AI)"}

# 強さの階級（気象庁・10分間平均風速m/s）。モデルの風速は1分間平均(ATCF)なので 0.88 倍して10分間平均相当にする
CATS = [(54, "猛烈な"), (44, "非常に強い"), (33, "強い"), (17.2, "台風"), (0, "熱帯低気圧")]
W10 = 0.88

def _w10(kt, is10=False):
    """kt → 10分間平均相当のm/s（JMA公式はそのまま）"""
    return None if not kt else kt * KT2MS * (1 if is10 else W10)

def _pct(a, p):
    a = sorted(a); k = (len(a) - 1) * p; f = int(k); c = min(f + 1, len(a) - 1)
    return a[f] + (a[c] - a[f]) * (k - f)

def _r1(v):
    return None if v is None else round(v, 1)

def intensity_rows(tracks, is10=False, probs=False):
    """tracks: [{tau: (風速kt, 気圧hPa)}]（1本=1メンバー or 1モデル）→ 12時間刻みの強さ統計。
    行: [tau, 風速平均, 風速P10, 風速P90, 気圧平均, 気圧P10, 気圧P90, 本数, [強い以上%, 非常に強い以上%, 猛烈な%]|None, 風速最小, 風速最大]（風速はm/s）"""
    rows = []
    for t in sorted({t for tr in tracks for t in tr if t % 12 == 0}):
        ws = [w for tr in tracks if t in tr and (w := _w10(tr[t][0], is10))]
        ps = [tr[t][1] for tr in tracks if t in tr and tr[t][1]]
        if not ws and not ps:
            continue
        pr = [round(100 * sum(w >= th for w in ws) / len(ws)) for th in (33, 44, 54)] if (probs and ws) else None
        rows.append([t,
                     _r1(sum(ws) / len(ws)) if ws else None, _r1(_pct(ws, .1)) if ws else None, _r1(_pct(ws, .9)) if ws else None,
                     round(sum(ps) / len(ps)) if ps else None, round(_pct(ps, .1)) if ps else None, round(_pct(ps, .9)) if ps else None,
                     max(len(ws), len(ps)), pr, _r1(min(ws)) if ws else None, _r1(max(ws)) if ws else None])
    return rows

def mean_track(members):
    """メンバー（[[tau, lat, lon, kt, hPa], ...]）から平均進路を作る。存続メンバーが1/3未満の時刻は捨てる。"""
    by = {}
    for m in members:
        for x in m:
            by.setdefault(x[0], []).append(x)
    out = []
    for t in sorted(by):
        g = by[t]
        if len(g) < max(2, len(members) // 3):
            continue
        lo0 = g[0][2]
        vs = [x[3] for x in g if x[3]]; ps = [x[4] for x in g if x[4]]
        out.append([t, round(sum(x[1] for x in g) / len(g), 1),
                    round(lo0 + sum((x[2] - lo0 + 540) % 360 - 180 for x in g) / len(g), 1),
                    round(sum(vs) / len(vs)) if vs else None, round(sum(ps) / len(ps)) if ps else None])
    return out

def _wl_time(s):
    return datetime.strptime(s.strip().replace("Z", "")[:19].replace("T", " "), F)

def wl_parse(text):
    """Weather Lab の CSV → {track_id: {member: {tau: (lat, lon, kt, hPa)}}}。列名は部分一致で探す（列の順序や細かい名前に依存しない）。
    観測（paired の実況）行はメンバー列が空か source が観測系なので除外する。"""
    rd = csv.reader(io.StringIO(text))
    head = next(rd, None)
    if not head:
        return {}
    h = [c.strip().lower() for c in head]
    def col(*keys, bad=()):
        for k in keys:
            for i, c in enumerate(h):
                if k in c and not any(b in c for b in bad):
                    return i
        return None
    i_tr, i_mem = col("track_id", "storm_id", "track", "sid"), col("sample", "member", "ensemble")
    i_ini, i_val, i_lead = col("init"), col("valid"), col("lead")
    i_lat, i_lon = col("lat"), col("lon")
    i_w = col("wind", bad=("radius", "r34", "r50", "r64", "rmw"))
    i_p = col("pressure", "pres", "slp", bad=("radius",))
    i_src = col("source")
    if None in (i_tr, i_lat, i_lon):
        return {}
    ms_unit = i_w is not None and any(k in h[i_w] for k in ("m_s", "ms", "mps")) and "knot" not in h[i_w] and "kt" not in h[i_w]
    out = {}
    for row in rd:
        try:
            if i_mem is None or not row[i_mem].strip():
                continue
            if i_src is not None and any(k in row[i_src].lower() for k in ("obs", "best", "vital", "truth", "anal")):
                continue
            tau = None
            if i_lead is not None:
                try: tau = int(round(float(row[i_lead])))
                except ValueError: tau = None
            if tau is None and i_ini is not None and i_val is not None:
                tau = int(round((_wl_time(row[i_val]) - _wl_time(row[i_ini])).total_seconds() / 3600))
            if tau is None or tau < 0:
                continue
            la, lo = float(row[i_lat]), float(row[i_lon])
            v = num(row[i_w]) if i_w is not None else None
            p = num(row[i_p]) if i_p is not None else None
            if v is not None and ms_unit:
                v = v * MS2KT
            out.setdefault(row[i_tr].strip().upper(), {}).setdefault(row[i_mem].strip(), {})[tau] = (
                round(la, 1), round(lo, 1), round(v) if v and v > 0 else None, round(p) if p and p > 0 else None)
        except (ValueError, IndexError):
            continue
    return out

def wl_fetch(model, cyc):
    """Weather Lab の1サイクルぶん（全台風）を取得してパース。無ければ None。30分キャッシュ。"""
    def go():
        url = WL_URL.format(model=model, kind="ensemble", ts=cyc.strftime("%Y_%m_%dT%H_00"))
        try:
            r = requests.get(url, headers=UA, timeout=60)
        except requests.RequestException as e:
            print("WeatherLab", model, cyc, repr(e), flush=True)
            return None
        if r.status_code != 200 or r.text.lstrip()[:1] == "<":
            return None
        return wl_parse(r.text)
    if len(_fc_cache) > 40:  # 古いキャッシュを捨てる
        for k, _ in sorted(_fc_cache.items(), key=lambda kv: kv[1][0])[:15]:
            _fc_cache.pop(k, None)
    return _cached(f"wl:{model}:{cyc:%Y%m%d%H}", 1800, go)

def wl_ens(sid):
    """Weather Lab のAIアンサンブル → (ens要素のリスト, 取得状況)。"""
    nn, now = sid[2:], datetime.now(timezone.utc)
    yr = now.year
    base = now.replace(hour=now.hour // 6 * 6, minute=0, second=0, microsecond=0)
    cycles = [base - timedelta(hours=6 * i) for i in range(6)]
    def one(model):
        for cyc in cycles:
            g = wl_fetch(model, cyc)
            if not g:
                continue
            key = next((k for k in g if k in (f"WP{nn}{yr}".upper(), f"{sid}{yr}".upper())), None)
            if key:
                return model, cyc, g[key]
        return model, None, None
    with ThreadPoolExecutor(len(WL_MODELS)) as ex:
        res = list(ex.map(one, WL_MODELS))
    ens, st = [], []
    for model, cyc, tr in res:
        mem = [[[t, *m[t]] for t in sorted(m)] for m in (tr or {}).values()]
        mem = [m for m in mem if len(m) > 1]
        st.append({"name": WL_MODELS[model], "ok": bool(mem), "n": len(mem)})
        if mem:
            ens.append({"key": model, "label": WL_MODELS[model], "init": cyc.strftime("%Y%m%d%H"), "n": len(mem),
                        "members": mem, "mean": mean_track(mem)})
    return ens, st

def _adeck(sid):
    yr = datetime.now(timezone.utc).year
    r = requests.get(f"{RAL}/plots/northwestpacific/{yr}/{sid}{yr}/a{sid}{yr}.dat", headers=UA, timeout=60)
    r.raise_for_status()
    return r.headers.get("Last-Modified", ""), parse_adeck(r.text)

def _spec_steps(spec):
    """specifications.json の各時刻の行（実況・予報）→ {UTC時刻 YYYYMMDDHH: (風速m/s, 気圧hPa)}。
    forecast.json の予報点に風速・気圧が付いていない時の補完用。"""
    out = {}
    for r in spec or []:
        if not isinstance(r, dict) or not isinstance(r.get("part"), dict):
            continue
        t = re.sub(r"\D", "", str((r.get("validtime") or {}).get("UTC", "")))[:10]
        w, p = _wind(r, "sustained"), num(r.get("pressure"))
        if t and (w or p):
            out[t] = (w, p)
    return out

def jma_fc(tc):
    """気象庁の予報円（位置＋風速・気圧）。風速は maximumWind.sustained（m/s）をktに戻して保持。
    forecast.json の予報点に風速・気圧が無い時は、specifications.json の同じ時刻の値で補う。"""
    pts, init, spec = [], "", None
    for r in get_json(f"{BOSAI}/{tc}/forecast.json", []) or []:
        if isinstance(r, dict) and r.get("advancedHours") is not None:
            try:
                w, p = _wind(r, "sustained"), num(r.get("pressure")) or None
                t = re.sub(r"\D", "", str((r.get("validtime") or {}).get("UTC", "")))[:10]
                if not w or not p:
                    if spec is None:
                        spec = _spec_steps(get_json(f"{BOSAI}/{tc}/specifications.json", []))
                    sw, sp = spec.get(t, (None, None))
                    w, p = w or sw, p or sp or None
                pts.append([int(r["advancedHours"]), float(r["center"][0]), float(r["center"][1]),
                            round(w * MS2KT, 1) if w else None, p])
            except (KeyError, TypeError, ValueError, IndexError): continue
            if r["advancedHours"] == 0: init = t
    return {"init": init, "pts": sorted(pts), "is10": True} if len(pts) > 1 else None

def build_forecast(sid, jma=""):
    errs, lm, rows = [], "", []
    try:
        lm, rows = _cached("ad:" + sid, 900, lambda: _adeck(sid))
    except requests.RequestException as e:
        errs.append(f"UCAR RAL: {e}")
    top = datetime.strptime(max(x[0] for x in rows), "%Y%m%d%H") if rows else datetime.now(timezone.utc).replace(tzinfo=None)
    by = {}
    for init, tech, tau, la, lo, v, p in rows:
        if 0 <= tau <= 240: by.setdefault(tech, {}).setdefault(init, {})[tau] = (tau, round(la, 1), round(lo, 1), v, p)
    def last(tech):  # その技術IDの最新の初期時刻の予報（古すぎるものは捨てる）
        d = by.get(tech)
        if not d: return None
        i = max(d)
        if datetime.strptime(i, "%Y%m%d%H") < top - timedelta(hours=48) or len(d[i]) < 2: return None
        return i, [d[i][t] for t in sorted(d[i])]
    models = []
    for t, lb in DET.items():  # 同じモデルが別IDで載っている時（EMX/ECMF など）は、初期時刻が新しい方だけ残す
        if not (g := last(t)): continue
        m = {"key": t, "label": lb, "init": g[0], "pts": [list(x) for x in g[1]], "grp": "off" if lb == "JTWC公式" else "det"}
        old = next((x for x in models if x["label"] == lb), None)
        if old is None: models.append(m)
        elif m["init"] > old["init"]: models[models.index(old)] = m
    # --- DETに無い技術IDを自動検出（合成予報・その他のモデル）。補間版(…I)・アンサンブル・強度のみのガイダンスは除く
    sig = lambda pts: tuple((x[0], x[1], x[2]) for x in pts[:4])
    seen = {sig(m["pts"]) for m in models}
    extra = 0
    for t in sorted(by):
        if extra >= EXTRA_MAX: break
        if t in DET or t in SKIP_TECH or re.fullmatch(r"[A-Z](EMN|EMI|P\d\d|C00|E00)", t): continue
        if t.endswith("I") and (t[:-1] in by or t[:-1] in DET): continue
        if not (g := last(t)): continue
        pts = [list(x) for x in g[1]]
        if any(x[1] == 0 and x[2] == 0 for x in pts) or max(x[0] for x in pts) < 24: continue
        if len({(x[1], x[2]) for x in pts}) < 2 or sig(pts) in seen: continue
        seen.add(sig(pts)); extra += 1
        models.append({"key": t, "label": CON_ID.get(t) or EXTRA_NAME.get(t) or t, "init": g[0], "pts": pts,
                       "grp": "con" if t in CON_ID else "etc"})
    # --- a-deck のアンサンブル（機関ごとに自動検出）
    fam = {}
    for tech in sorted(by):
        m = ENS_ID.fullmatch(tech)
        if m and (g := last(tech)):
            fam.setdefault(m.group(1), []).append(g)
    ens = []
    for L, mem in fam.items():
        if L not in ENS_LET and len(mem) < 5:  # 未知の頭文字は5本以上そろった時だけ採用（誤検出防止）
            continue
        init = max(g[0] for g in mem)
        cur = [[[x[0], x[1], x[2], x[3], x[4]] for x in g[1]] for g in mem if g[0] == init]
        gm = last(L + "EMN")
        name = ENS_LET.get(L, f"ENS-{L}")
        ens.append({"key": name, "label": name, "init": init, "n": len(cur), "members": cur,
                    "mean": [[x[0], x[1], x[2], x[3], x[4]] for x in gm[1]] if gm and gm[0] == init else mean_track(cur)})
    # --- Weather Lab（AIアンサンブル）
    wl_ok = False
    try:
        wens, wst = wl_ens(sid)
        ens += wens
        wl_ok = bool(wens)
    except Exception as e:  # 取得元が変わっても他のモデルは止めない
        wst = []
        errs.append(f"Weather Lab: {e!r}")
    car = by.get("CARQ"); ana = None
    if car:
        i = max(car); x = car[i][min(car[i])]; ana = {"init": i, "lat": x[1], "lon": x[2], "w": x[3], "p": x[4]}
    jm = jma_fc(jma) if jma else None
    cand = [(m["label"], {x[0]: (x[1], x[2]) for x in m["pts"]}) for m in models]
    cand += [(e["label"] + "平均", {x[0]: (x[1], x[2]) for x in e["mean"]}) for e in ens if e["mean"]]
    if jm: cand.append(("JMA公式", {x[0]: (x[1], x[2]) for x in jm["pts"]}))
    taus = [24, 48, 72, 96, 120]; srows = [[lb, []] for lb, _ in cand]; mx, nn = [], []
    for t in taus:  # 各モデルの予報位置が、全モデルの平均位置から何km離れているか
        ps = [(i, tp[t]) for i, (_, tp) in enumerate(cand) if t in tp]
        col = [None] * len(cand)
        if len(ps) >= 2:
            lo0 = ps[0][1][1]
            la_m = sum(p[1][0] for p in ps) / len(ps)
            lo_m = lo0 + sum((p[1][1] - lo0 + 540) % 360 - 180 for p in ps) / len(ps)
            for i, pos in ps: col[i] = ll_dist(pos, (la_m, lo_m))
        for i, v in enumerate(col): srows[i][1].append(v)
        mx.append(max((v for v in col if v is not None), default=None)); nn.append(len(ps))
    # --- 強さの予報（風速m/s＝10分間平均相当、中心気圧hPa）
    ser = []
    def add_int(label, kind, init, tracks, is10=False):
        r = intensity_rows(tracks, is10, probs=(kind == "ens"))
        if r: ser.append({"label": label, "kind": kind, "init": init, "n": len(tracks), "rows": r})
    if jm: add_int("JMA公式", "det", jm["init"], [{x[0]: (x[3], x[4]) for x in jm["pts"]}], True)
    for m in models: add_int(m["label"], "det", m["init"], [{x[0]: (x[3], x[4]) for x in m["pts"]}])
    for e in ens: add_int(e["label"], "ens", e["init"], [{x[0]: (x[3], x[4]) for x in mem} for mem in e["members"]])
    if not (models or ens or jm):
        raise requests.RequestException("; ".join(errs) or "予報データがありません")
    return {"id": sid, "updated": lm, "analysis": ana, "models": models, "ens": ens, "jma": jm,
            "spread": {"taus": taus, "rows": srows, "max": mx, "n": nn},
            "intensity": {"series": ser},
            "sources": {"adeck": bool(rows), "weatherlab": wst, "weatherlab_ok": wl_ok, "errors": errs,
                        "members": sum(e["n"] for e in ens)}}

# ---------------- 似た過去の台風 ----------------
import bisect

def _enu(a, b):
    """a→b の (東向き, 北向き) [km]（近距離用の平面近似）"""
    return ((b[1] - a[1] + 540) % 360 - 180) * math.cos(math.radians((a[0] + b[0]) / 2)) * 111.195, (b[0] - a[0]) * 111.195

def _at(trk, ts, t):
    """時刻順の点列 trk=[(時刻,lat,lon,wind,pres)] の時刻 t での位置を線形補間 → (lat, lon, wind) / 範囲外は None"""
    if t < ts[0] or t > ts[-1]: return None
    i = bisect.bisect_left(ts, t)
    if ts[i] == t: return trk[i][1], trk[i][2], trk[i][3]
    a, b = trk[i - 1], trk[i]
    f = (t - a[0]).total_seconds() / (b[0] - a[0]).total_seconds()
    dlo = (b[2] - a[2] + 540) % 360 - 180
    w = None if a[3] is None or b[3] is None else a[3] + (b[3] - a[3]) * f
    return a[1] + (b[1] - a[1]) * f, a[2] + dlo * f, w

def find_analogs(la, lo, doy, w, fc, n=5):
    """現在位置・今後の動き(予報fc={tau:(lat,lon)})・季節・強さが近い過去の台風。1台風につき最も近い時点を1つ返す。
    スコア = 位置差/150km + 進路差(+24/48/72hの位置の食い違い)/150km + 季節差/40日 + 強さの差/15m/s（小さいほど似ている）"""
    lo = (lo + 180) % 360 - 180
    cut = (datetime.now(timezone.utc) + timedelta(hours=9) - timedelta(days=14)).strftime(F)  # 活動中・直近の台風は除く
    box = q("SELECT p.sid, p.time FROM points p JOIN typhoons t ON t.sid=p.sid WHERE t.end_time < ? "
            "AND p.lat BETWEEN ? AND ? AND p.lon BETWEEN ? AND ?", (cut, la - 5, la + 5, lo - 8, lo + 8))
    hit = {}
    for r in box: hit.setdefault(r["sid"], set()).add(r["time"][:19])
    if not hit: return []
    sids = sorted(hit); trk = {}
    for i in range(0, len(sids), 400):
        ch = sids[i:i + 400]
        for r in q(f"SELECT sid, time, lat, lon, wind, pres FROM points WHERE sid IN ({','.join('?' * len(ch))}) ORDER BY sid, time", ch):
            trk.setdefault(r["sid"], []).append((datetime.strptime(r["time"][:19], F), r["lat"], r["lon"], r["wind"], r["pres"]))
    meta = {r["sid"]: r for r in q("SELECT sid, title, year, max_wind, min_pres FROM typhoons")}
    taus = sorted(t for t in fc if t in (24, 48, 72))
    fv = {t: _enu((la, lo), fc[t]) for t in taus}
    out = []
    for sid in sids:
        pts = trk.get(sid) or []
        if len(pts) < 4 or sid not in meta: continue
        ts = [p[0] for p in pts]; best = None
        for p in pts:
            if p[0].strftime(F) not in hit[sid]: continue
            d0 = ll_dist((la, lo), (p[1], p[2]))
            if d0 > 500: continue
            errs, fut = [], []
            for t in (24, 48, 72):
                x = _at(pts, ts, p[0] + timedelta(hours=t))
                if x is None: continue
                fut.append([t, round(x[0], 1), round(x[1], 1), to_ms(x[2])])
                if t in fv:
                    e = _enu((p[1], p[2]), (x[0], x[1])); errs.append(math.hypot(e[0] - fv[t][0], e[1] - fv[t][1]))
            if taus and (not errs or 24 not in [f[0] for f in fut]): continue
            miss = len([t for t in taus if t not in [f[0] for f in fut]])
            rms = math.sqrt(sum(e * e for e in errs) / len(errs)) + 100 * miss if errs else 0
            dd = abs(p[0].timetuple().tm_yday - doy); dd = min(dd, 365 - dd)
            wi = abs(w - to_ms(p[3])) if (w and p[3]) else 0
            sc = d0 / 150 + rms / 150 + dd / 40 + wi / 15
            if best is None or sc < best[0]: best = (sc, p, d0, rms, fut)
        if best is None: continue
        sc, p, d0, rms, fut = best
        th = [[round(x[1], 2), round(x[2], 2), to_ms(x[3])] for x in pts][::2]
        wi = []  # 起点を0hとした前後の強さ推移 [[時間h, 風速m/s]]
        for h in range(-48, 73, 6):
            x = _at(pts, ts, p[0] + timedelta(hours=h))
            wi.append([h, to_ms(x[2]) if x and x[2] else None])
        mi = min(range(len(th)), key=lambda i: (th[i][0] - p[1]) ** 2 + (th[i][1] - p[2]) ** 2)
        m = meta[sid]
        out.append({"sid": sid, "title": m["title"], "year": m["year"], "time": p[0].strftime(F), "lat": round(p[1], 1), "lon": round(p[2], 1),
                    "w": to_ms(p[3]), "d0": round(d0), "err": round(rms) if taus else None, "score": round(sc, 2),
                    "max_wind": to_ms(m["max_wind"]), "min_pres": m["min_pres"], "fut": fut, "wi": wi, "track": th, "mi": mi})
    return sorted(out, key=lambda r: r["score"])[:n]

@app.get("/api/analogs")
def api_analogs(lat: float, lon: float, doy: int = 0, w: float = 0, fc: str = "", n: int = Query(5, ge=1, le=8)):
    """fc = '24:lat:lon,48:lat:lon,72:lat:lon'（基準にする予報進路）"""
    f = {}
    for x in fc.split(","):
        try:
            t, a, b = x.split(":"); f[int(t)] = (float(a), float(b))
        except ValueError: continue
    key = f"an:{lat:.1f}:{lon:.1f}:{doy}:{w:.0f}:{sorted(f.items())}:{n}"
    try: return _cached(key, 600, lambda: find_analogs(lat, lon, doy, w, f, n))
    except sqlite3.OperationalError: return []

@app.get("/api/forecast/storms")
def fc_storms():
    """予報比較の対象（UCARが公開している活動中の西太平洋の台風）。JMAの台風と名前で対応づける。"""
    def go():
        html = requests.get(f"{RAL}/current/", headers=UA, timeout=30).text
        ids = sorted(set(re.findall(r"northwestpacific/\d{4}/(wp\d{2})\d{4}/", html)))
        jm = []
        for t in get_json(f"{BOSAI}/targetTc.json", []) or []:
            tc = t.get("tropicalCyclone")
            if tc:
                spec = get_json(f"{BOSAI}/{tc}/specifications.json", []) or []
                jm.append({"tc": tc, "num": str(t.get("typhoonNumber", "")), "name": _title_en(spec), "size": jma_size(spec)})
        out = []
        for i in ids:
            m = re.search(r">\s*([^<>]*\(%s\))\s*<" % i.upper(), html)
            label = re.sub(r"\s+", " ", m.group(1)).strip() if m else i.upper()
            out.append({"id": i, "label": label, "jma": next((x for x in jm if x["name"] and x["name"] in label.upper()), None)})
        return sorted(out, key=lambda s: s["id"][2:] >= "90")  # 番号のついた台風を先、invest(90番台)を後ろに
    try: return _cached("fc:storms", 300, go)
    except requests.RequestException as e: raise HTTPException(502, f"台風の一覧を取得できません: {e}")

@app.get("/api/forecast/{sid}")
def fc_detail(sid: str, jma: str = ""):
    if not re.fullmatch(r"wp\d{2}", sid) or not re.fullmatch(r"[A-Za-z0-9]*", jma): raise HTTPException(404, "対象外です")
    try: return _cached(f"fc:{sid}:{jma}", 600, lambda: build_forecast(sid, jma))
    except requests.RequestException as e: raise HTTPException(502, f"予報データを取得できません: {e}")

# 予報は本体ページの「予報」タブに統合した。旧URL(/forecast)は新しいタブへ転送する
@app.get("/forecast")
def forecast_page():
    return RedirectResponse("/#fc")

# ---------------- 実況タブ（防災情報JSON・アメダス・警報注意報）----------------
# 台風が発生していなくても、観測値と警報・注意報は取得できる。3系統は別々のAPIにして、1つの失敗で他を止めない。
AMEDAS = "https://www.jma.go.jp/bosai/amedas"
WARN_URL = "https://www.jma.go.jp/bosai/warning/data/r8/map.json"   # 2026年5月の体系変更後の配信元（旧 warning/data/warning/map.json は5月28日で更新停止）
GEO_BASE = "https://www.jma.go.jp/bosai/common/const"
AREA_URL = "https://www.jma.go.jp/bosai/common/const/area.json"
W_SPECIAL = {"32", "33", "34", "35", "36", "37", "38", "39"}   # 特別警報（レベル5）
W_DANGER = {"42", "43", "44", "45", "46", "47", "48", "49"}    # 危険警報（レベル4）
W_ALERT = {"02", "03", "04", "05", "06", "07", "08", "09"}     # 警報（レベル3）。それ以外の既知コードは注意報（レベル2）
W_NAME = {"32": "暴風雪", "33": "大雨", "34": "氾濫", "35": "暴風", "36": "大雪", "37": "波浪", "38": "高潮", "39": "土砂災害",
          "42": "暴風雪", "43": "大雨", "44": "氾濫", "45": "暴風", "46": "大雪", "47": "波浪", "48": "高潮", "49": "土砂災害",
          "02": "暴風雪", "03": "大雨", "04": "氾濫", "05": "暴風", "06": "大雪", "07": "波浪", "08": "高潮", "09": "土砂災害",
          "10": "大雨", "12": "大雪", "13": "風雪", "14": "雷", "15": "強風", "16": "波浪", "17": "融雪", "18": "氾濫", "19": "高潮",
          "20": "濃霧", "21": "乾燥", "22": "なだれ", "23": "低温", "24": "霜", "25": "着氷", "26": "着雪", "29": "土砂災害"}

def w_level(c):
    """警報コード → 段階（1=注意報 2=警報 3=危険警報 4=特別警報）。画面の l1〜l4 と対応。"""
    return 4 if c in W_SPECIAL else 3 if c in W_DANGER else 2 if c in W_ALERT else 1

def _jget(url):
    """取得に失敗したら例外にする（get_json は握りつぶすため、キャッシュの判定には使わない）。"""
    r = requests.get(url, headers=UA, timeout=30)
    r.raise_for_status()
    return r.json()

def _jp(v):
    """{"jp": "..."} でも文字列でも日本語表記を返す。数値や空は None。"""
    if isinstance(v, dict):
        v = v.get("jp") or v.get("en")
    return v.strip() if isinstance(v, str) and v.strip() else None

def _km(v):
    """半径らしき値 → km。{"km":..} / {"nm":..} / {"radius":{..}} / 数値 / リスト（先頭の有効値）のどれでも読む。"""
    if isinstance(v, dict):
        if "km" in v:
            return num(v["km"])
        if "nm" in v:
            n = num(v["nm"])
            return n * NM if n else None
        for k in ("radius", "range", "circle", "areas"):
            if k in v:
                x = _km(v[k])
                if x:
                    return x
        return None
    if isinstance(v, list):
        return next((x for x in map(_km, v) if x), None)
    return num(v)

def _circle_km(v):
    """予報円・暴風警戒域の半径[km]。辞書・配列の奥まで探し、km があればそれ、無ければ nm を換算する。
    座標（basePoint / center など）は読み飛ばす。単位なしの radius / range は km とみなす。"""
    def walk(x, d=0):
        if d > 5:
            return None
        if isinstance(x, dict):
            for key, f in (("km", 1), ("nm", NM)):
                n = num(x.get(key))
                if n:
                    return n * f
            for k, y in x.items():
                if str(k).lower() in ("basepoint", "center", "position", "deg", "dms", "latlon") or not isinstance(y, (dict, list)):
                    continue
                r = walk(y, d + 1)
                if r:
                    return r
            for k in ("radius", "range"):
                n = num(x.get(k))
                if n:
                    return n
        elif isinstance(x, list):
            for y in x:
                if isinstance(y, (dict, list)):
                    r = walk(y, d + 1)
                    if r:
                        return r
        return None
    return walk(v)

def _sane_km(v, raw=None, tag=""):
    """半径[km]の妥当性チェック。予報円・暴風警戒域は実際には数百km〜千数百km。
    3000kmを超える値は単位がメートルの可能性が高いので1000で割る。それでも範囲外なら読めなかったことにして生データをログに出す。"""
    if v and v > 3000 and v <= 3_000_000:
        v = v / 1000
    if v and not (0 < v <= 3000):
        v = None
    if v is None and raw is not None:
        print(f"live storm: {tag}の半径が範囲外です raw =", json.dumps(raw, ensure_ascii=False)[:400], flush=True)
    return v

def _iso_utc(v):
    s = str(v.get("UTC", "")) if isinstance(v, dict) else ""
    try:
        return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None

def _wind(r, k):
    """maximumWind.{sustained|gust} → m/s。m/s が無く kt だけの時は換算する。"""
    d = (r.get("maximumWind") or {}).get(k) or {}
    if not isinstance(d, dict):
        return num(d)
    v = num(d.get("m/s"))
    if v is None:
        kt = num(d.get("kt") if d.get("kt") is not None else d.get("knot"))
        v = kt * KT2MS if kt else None
    return v

def _speed(r):
    sp = r.get("speed")
    return num(sp.get("km/h")) if isinstance(sp, dict) else num(sp)

def _r6(r):
    """実況の暴風域・強風域 → [向き, 長径km, 短径km, 向き, 長径km, 短径km]（画面の wzShape と同じ形）。無ければ None。"""
    storm = gale = None
    for k, v in r.items():
        kl = str(k).lower()
        if "storm" in kl and "circle" not in kl:
            storm = _warn_area(v)
        elif "gale" in kl:
            gale = _warn_area(v)
    if not storm and not gale:
        return None
    return [round(x, 1) if isinstance(x, float) else x for a in (storm, gale) for x in (a or (None, None, None))]

def jma_size(spec):
    """実況の強風域の最大半径[km]と、気象庁が発表した大きさの階級（無ければ None）。"""
    for r in spec:
        p = r.get("part") if isinstance(r, dict) else None
        if isinstance(p, dict) and p.get("jp") == "実況":
            a = _r6(r)
            return {"km": a[4] if a else None, "label": _jp(r.get("scale"))}
    return None

def live_storm(tc, tn):
    """発生中の台風1つぶんの実況・進路予報（気象庁）。実況の詳細は specifications.json、予報円は forecast.json。"""
    spec = get_json(f"{BOSAI}/{tc}/specifications.json", []) or []
    fc = get_json(f"{BOSAI}/{tc}/forecast.json", []) or []
    out = {"tc": tc, "no": int(tn[2:]), "year": 2000 + int(tn[:2]), "en": _title_en(spec) or _title_en(fc),
           "now": None, "fc": [], "past": []}
    for r in spec:
        p = r.get("part") if isinstance(r, dict) else None
        if isinstance(p, dict) and p.get("jp") == "実況":
            try:
                la, lo = (float(x) for x in r["position"]["deg"][:2])
            except (KeyError, TypeError, ValueError, IndexError):
                break
            lb = {k: _jp(r.get(k)) for k in ("category", "scale", "intensity")   # scale/intensity は文字列（"大型" "-" など）
                  if _jp(r.get(k)) not in (None, "-")}
            out["now"] = {"t": _iso_utc(r.get("validtime")), "lat": la, "lon": lo, "pres": num(r.get("pressure")),
                          "wind": _wind(r, "sustained"), "gust": _wind(r, "gust"), "course": _jp(r.get("course")),
                          "speed": _speed(r), "r": _r6(r), "lb": lb,
                          "sz": _jp(r.get("scale"))}   # 気象庁の階級の原文（"大型" "超大型"。階級なしは "-"。項目が無ければ None）
            break
    # 予報円の半径は specifications.json の各予報行 probabilityCircleRadius {"km":95,"nm":50} が確実（実データで確認済み）
    sp_fc = {}
    for r in spec:
        if isinstance(r, dict) and isinstance(r.get("part"), dict) and r.get("advancedHours") not in (None, 0, "0"):
            try:
                sp_fc[int(r["advancedHours"])] = r
            except (TypeError, ValueError):
                pass
    for r in fc:
        if not isinstance(r, dict) or r.get("advancedHours") is None:
            continue
        try:
            h = int(r["advancedHours"])
            la, lo = float(r["center"][0]), float(r["center"][1])
        except (KeyError, TypeError, ValueError, IndexError):
            continue
        if h == 0:
            out["past"] = [[round(a, 2), round(b, 2)] for a, b in past_track(r.get("track"))]
            if out["now"] is None:  # specifications.json に実況が無い時は、forecast.json の実況点で代用
                out["now"] = {"t": _iso_utc(r.get("validtime")), "lat": la, "lon": lo, "pres": num(r.get("pressure")),
                              "wind": _wind(r, "sustained"), "gust": _wind(r, "gust"), "course": None, "speed": None,
                              "r": None, "lb": {}, "sz": None}
            continue
        ks = sorted(r.keys(), key=lambda k: ("probab" not in str(k).lower(), str(k)))   # probabilityCircle を最優先
        circ = next((r[k] for k in ks if "circle" in str(k).lower()), None)  # 予報円（forecast.json 側。specifications に無い時の予備）
        sr = sp_fc.get(h, {})
        out["fc"].append({"h": h, "t": _iso_utc(r.get("validtime")), "lat": la, "lon": lo, "pres": num(r.get("pressure")),
                          "wind": _wind(r, "sustained"), "gust": _wind(r, "gust"),
                          "circle": (_sane_km(_circle_km(sr.get("probabilityCircleRadius")), sr.get("probabilityCircleRadius"), "予報円(specifications)") if sr.get("probabilityCircleRadius") else None)
                                    or _sane_km(_circle_km(circ), circ, "予報円(forecast)"),
                          "storm": _sane_km(_circle_km(sr.get("stormWarning")), sr.get("stormWarning"), "暴風警戒域(specifications)")
                                   or _sane_km(_circle_km(r.get("stormWarning")), r.get("stormWarning"), "暴風警戒域(forecast)")})  # 暴風警戒域
    out["fc"].sort(key=lambda p: p["h"])
    if out["fc"] and not any(p["circle"] for p in out["fc"]):
        k0 = next((r for r in fc if isinstance(r, dict) and r.get("advancedHours") not in (None, 0, "0")), {})
        print(f"live storm {tc}: 予報円の半径を読み取れません keys =", sorted(k0.keys()), flush=True)
    return out

@app.get("/api/live/storms")
def live_storms():
    def go():
        lst = get_json(f"{BOSAI}/targetTc.json", None)
        if lst is None:
            raise requests.RequestException("targetTc.json を取得できません")
        storms = []
        for t in lst:
            tn, tc = str(t.get("typhoonNumber", "")), t.get("tropicalCyclone")
            if not tc or not re.fullmatch(r"\d{4}", tn):  # 熱帯低気圧(号数なし)は対象外
                continue
            try:
                s = live_storm(tc, tn)
            except Exception as e:
                print(f"live storm {tc} failed:", repr(e), flush=True)
                continue
            if s["now"]:
                storms.append(s)
        return {"storms": sorted(storms, key=lambda s: (s["year"], s["no"]))}
    try:
        return _cached("lv:storms", 180, go)
    except requests.RequestException as e:
        raise HTTPException(502, f"台風の実況を取得できません: {e}")

def _q(d, k):
    """アメダスの値は [値, 品質] の組。品質が正常(0)の時だけ値を返す。"""
    v = d.get(k)
    if isinstance(v, list) and v and v[0] is not None and (len(v) < 2 or v[1] in (0, None)):
        return v[0]
    return None

def _lv1(v):
    return None if v is None else round(float(v), 1)

def live_amedas():
    tbl = _cached("lv:amtbl", 86400, lambda: _jget(f"{AMEDAS}/const/amedastable.json"))
    r = requests.get(f"{AMEDAS}/data/latest_time.txt", headers=UA, timeout=30)
    r.raise_for_status()
    ts = datetime.fromisoformat(r.text.strip())
    data = _jget(f"{AMEDAS}/data/map/{ts.strftime('%Y%m%d%H%M%S')}.json")
    rows = []
    # 列: id, 地点名, 緯度, 経度, 風速m/s, 風向(0=静穏,1=北北東…16=北), 最大瞬間風速m/s, 海面気圧hPa, 1時間雨量, 3時間雨量, 24時間雨量(mm)
    for sid, d in data.items():
        s = tbl.get(sid)
        if not s or not isinstance(d, dict):
            continue
        row = [_q(d, "wind"), _q(d, "windDirection"), _q(d, "gust"), _q(d, "normalPressure"),
               _q(d, "precipitation1h"), _q(d, "precipitation3h"), _q(d, "precipitation24h")]
        if all(x is None for x in row):
            continue
        w, wd, g, p, r1, r3, r24 = row
        rows.append([sid, s.get("kjName") or sid, round(s["lat"][0] + s["lat"][1] / 60, 3), round(s["lon"][0] + s["lon"][1] / 60, 3),
                     _lv1(w), wd, _lv1(g), _lv1(p), _lv1(r1), _lv1(r3), _lv1(r24)])
    return {"t": ts.isoformat(), "rows": rows}

@app.get("/api/live/amedas")
def api_amedas():
    try:
        return _cached("lv:amedas", 240, live_amedas)
    except (requests.RequestException, ValueError, KeyError) as e:
        raise HTTPException(502, f"アメダスの観測値を取得できません: {e!r}")

def _to_office(area, code):
    """地域コード（市町村等・一次細分区域）→ 府県予報区（offices）のコード。
    area.json は階層ごとにコード空間が重なる（小笠原諸島は class10s も class15s も 130040）ので、
    コードで階層を決めず、市町村等(7桁)→class15s→class10s の順に1段ずつ親へ上がる。"""
    code = str(code)
    cur = code
    for lv in (("class20s", "class15s", "class10s") if len(code) == 7 else ("class10s",)):
        a = area.get(lv, {}).get(cur)
        if a and a.get("parent"):
            cur = a["parent"]
    return cur if cur in area.get("offices", {}) else None

def _active_warnings(raw):
    """r8/map.json（発表単位の電文の並び）→ ({地域コード: {警報コード}}, 最新の発表時刻)。
    電文は dataTypeCode（VPWW55 大雨 / 58 暴風 / 59 波浪 …）ごとに、1次細分区域(class10Items)・市町村等(class20Items)の
    kinds を全部並べる。(種類, 地域) ごとに「最新の電文の kinds」で置き換える（解除・「警報から注意報」の降格・
    「発表警報・注意報はなし」で古いコードが残らない）。"""
    items = [it for it in (raw if isinstance(raw, list) else [raw]) if isinstance(it, dict)]
    cur, reported = {}, ""   # (種類, 地域コード) → {警報コード}
    for it in sorted(items, key=lambda x: (str(x.get("reportDatetime") or ""), str(x.get("controlDatetime") or ""))):
        reported = max(reported, str(it.get("reportDatetime") or ""))
        dt = str(it.get("dataTypeCode") or "")
        w = it.get("warning") or {}
        for key in ("class10Items", "class20Items"):
            for a in w.get(key) or []:
                code = str(a.get("areaCode") or "")
                if not code:
                    continue
                cur[(dt, code)] = {str(k["code"]) for k in a.get("kinds") or []
                                   if k.get("code") and k.get("status") != "解除"}
    st = {}
    for (dt, code), cs in cur.items():
        if cs:
            st.setdefault(code, set()).update(cs)
    return st, reported

PREF = {"01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県", "05": "秋田県", "06": "山形県", "07": "福島県", "08": "茨城県",
        "09": "栃木県", "10": "群馬県", "11": "埼玉県", "12": "千葉県", "13": "東京都", "14": "神奈川県", "15": "新潟県", "16": "富山県",
        "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県", "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県",
        "25": "滋賀県", "26": "京都府", "27": "大阪府", "28": "兵庫県", "29": "奈良県", "30": "和歌山県", "31": "鳥取県", "32": "島根県",
        "33": "岡山県", "34": "広島県", "35": "山口県", "36": "徳島県", "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
        "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県", "45": "宮崎県", "46": "鹿児島県", "47": "沖縄県"}

def _to_c10(area, code):
    """地域コード（市町村等・一次細分区域）→ 一次細分区域（class10s）のコード。市町村等(7桁)→class15s→class10s と親へ上がる。"""
    code = str(code)
    c10 = area.get("class10s", {})
    if len(code) == 6 and code in c10:
        return code
    cur = code
    for lv in ("class20s", "class15s"):
        a = area.get(lv, {}).get(cur)
        if a and a.get("parent"):
            cur = a["parent"]
    return cur if cur in c10 else None

def _area_name(area, c10):
    """一次細分区域の表示名。「山梨県中・西部」のように都道府県名を頭に付ける。
    伊豆諸島・小笠原諸島や北海道の「〇〇地方」、名前に県名が入っているもの（大阪府・東京地方・沖縄本島中南部）はそのまま。"""
    cn = area["class10s"][c10].get("name", c10)
    pn = PREF.get(c10[:2], "")
    if not pn or c10[:2] == "01" or "諸島" in cn or cn.startswith(pn[:-1]):
        return cn
    return pn + cn

def live_warnings():
    area = _cached("lv:area", 86400, lambda: _jget(AREA_URL))
    st, reported = _active_warnings(_jget(WARN_URL))
    areas = {}   # 一次細分区域コード → 表示用のまとめ
    for code, ws in st.items():
        c10 = _to_c10(area, code)
        if not c10:
            continue
        o = areas.setdefault(c10, {"code": c10, "name": _area_name(area, c10), "n": 0, "w": {}})
        is20 = code in area.get("class20s", {})
        o["n"] += 1 if is20 else 0
        for c in ws:
            o["w"][c] = o["w"].get(c, 0) + (1 if is20 else 0)   # 警報コード → 発表中の市町村等の数
    out = []
    for o in areas.values():
        ws = [{"c": c, "name": W_NAME.get(c, c), "lv": w_level(c), "k": max(k, 1)} for c, k in o["w"].items()]
        ws.sort(key=lambda x: (-x["lv"], -x["k"]))
        out.append({"code": o["code"], "name": o["name"], "n": o["n"], "w": ws})
    # 地図の塗りつぶし用: 地域コード(6桁=1次細分区域 / 7桁=市町村等) → 発表中の警報コード
    c10 = {k: sorted(v) for k, v in st.items() if len(k) == 6}
    c20 = {k: sorted(v) for k, v in st.items() if len(k) == 7}
    return {"t": reported, "areas": sorted(out, key=lambda o: o["code"]), "c10": c10, "c20": c20}

@app.get("/api/live/geo/{name}")
def api_geo(name: str):
    """警報の塗りつぶし用の地図の形。1次細分区域(class10s)・市町村等(class20s_0〜9)・分割範囲(relm)。気象庁から取って24時間キャッシュする。"""
    if not re.fullmatch(r"class10s|class20s_[0-9]|relm", name):
        raise HTTPException(404, "対象外です")
    url = f"{GEO_BASE}/relm.json" if name == "relm" else f"{GEO_BASE}/geojson/{name}.json"
    def go():
        r = requests.get(url, headers=UA, timeout=60)
        r.raise_for_status()
        return r.content
    try:
        return Response(_cached("geo:" + name, 86400, go), media_type="application/json", headers={"Cache-Control": "public, max-age=86400"})
    except requests.RequestException as e:
        raise HTTPException(502, f"地図の形を取得できません: {e!r}")

@app.get("/api/live/warnings")
def api_warnings():
    try:
        return _cached("lv:warn", 240, live_warnings)
    except (requests.RequestException, ValueError, KeyError) as e:
        raise HTTPException(502, f"警報・注意報を取得できません: {e!r}")

@app.get("/api/live/diag")
def live_diag():
    """実況タブの取得元を確認する用（JSONの形が変わった時にブラウザで見る）。台風1つぶんの生データと、アメダス・警報の先頭を返す。"""
    from fastapi.responses import PlainTextResponse
    out = {}
    try:
        lst = _jget(f"{BOSAI}/targetTc.json")
        out["targetTc"] = lst
        t = next((x for x in lst if re.fullmatch(r"\d{4}", str(x.get("typhoonNumber", "")))), None)
        if t:
            tc = t["tropicalCyclone"]
            out["specifications"] = _jget(f"{BOSAI}/{tc}/specifications.json")
            fcj = _jget(f"{BOSAI}/{tc}/forecast.json") or []
            out["forecast(予報2件)"] = [r for r in fcj if isinstance(r, dict) and r.get("advancedHours") not in (None, 0, "0")][:2]
            out["forecast(先頭2件)"] = fcj[:2]
    except Exception as e:
        out["storm_error"] = repr(e)
    try:
        a = live_amedas()
        out["amedas"] = {"time": a["t"], "stations": len(a["rows"]), "sample": a["rows"][:3],
                         "gustあり": sum(1 for r in a["rows"] if r[6] is not None),
                         "気圧あり": sum(1 for r in a["rows"] if r[7] is not None)}
    except Exception as e:
        out["amedas_error"] = repr(e)
    try:
        w = live_warnings()
        out["warnings"] = {"time": w["t"], "areas": len(w["areas"]), "sample": w["areas"][:3]}
    except Exception as e:
        out["warnings_error"] = repr(e)
    return PlainTextResponse(json.dumps(out, ensure_ascii=False, indent=1)[:60000])

# ---------------- 画面 ----------------
PAGE = r"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0a1120">
<link rel="manifest" href="/manifest.webmanifest"><link rel="icon" href="/icon.svg"><link rel="apple-touch-icon" href="/icon.svg">
<title>台風データベース</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=Zen+Kaku+Gothic+New:wght@400;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#0a1120;--s1:#101a30;--s2:#17233f;--line:#22314f;--ink:#e9eefb;--sub:#8b9bbb;--acc:#38bdf8;--r:14px;--tb:0px}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{overscroll-behavior:none}
[hidden]{display:none!important}
body{margin:0;height:100dvh;display:grid;grid-template-columns:400px 1fr;background:var(--bg);color:var(--ink);font:14px/1.5 "Zen Kaku Gothic New",system-ui,sans-serif;overflow:hidden}
input,select,button{font:inherit;color:var(--ink);touch-action:manipulation}
:focus-visible{outline:2px solid var(--acc);outline-offset:1px}

/* ---- 画面切り替えタブ（PC: 左パネル上部 / スマホ: 画面下部） ---- */
#tabs{position:fixed;left:0;top:0;width:400px;height:56px;z-index:1700;display:grid;grid-template-columns:repeat(4,1fr);gap:6px;padding:7px 10px;background:var(--s1);border-bottom:1px solid var(--line);border-right:1px solid var(--line)}
#tabs button{border:0;border-radius:10px;background:transparent;color:var(--sub);display:flex;flex-direction:column;align-items:center;justify-content:center;line-height:1.25;cursor:pointer}
#tabs b{font-size:14px}#tabs small{font-size:10px;opacity:.85}
#tabs button:hover{background:var(--s2)}
#tabs button[aria-selected=true]{background:var(--acc);color:#04121f}

/* ---- 左パネル（3つの画面が同じ場所に入る） ---- */
aside{display:flex;flex-direction:column;min-height:0;background:var(--s1);border-right:1px solid var(--line);padding-top:56px}
.sec{display:none;flex-direction:column;min-height:0;flex:1}
body[data-tab=fc] #s-fc,body[data-tab=db] #s-db,body[data-tab=st] #s-st,body[data-tab=lv] #s-lv{display:flex}
.shd,.grab{display:none}
.pad{padding:12px 14px 6px;display:grid;gap:8px;flex:none}
.scroll{flex:1;min-height:0;overflow:auto;overscroll-behavior:contain;padding:2px 14px 18px;-webkit-overflow-scrolling:touch}
.sh{margin:16px 0 6px;font-size:12px;font-weight:700;color:var(--sub);letter-spacing:.04em}
.note{color:var(--sub);font-size:11px;line-height:1.6}
.btn{height:38px;padding:0 14px;border:0;border-radius:10px;background:var(--s2);font-size:13px;cursor:pointer;white-space:nowrap}
.btn.sm{height:34px;padding:0 12px}
.btn.on{background:var(--acc);color:#04121f;font-weight:700}
.btn:active{filter:brightness(1.25)}
.btns{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.in,.f input:not([type=checkbox]),.f select,.sel select,#oStep{background:var(--s2);border:1px solid transparent;border-radius:10px;padding:9px 12px}
.f input:not([type=checkbox]):focus,.f select:focus{border-color:var(--acc);outline:0}
.pill{font-size:11px;color:var(--sub);border:1px solid var(--line);border-radius:99px;padding:2px 10px;white-space:nowrap}
.pill b{color:var(--acc);font-weight:700}
.chk{display:flex;align-items:center;gap:8px;font-size:13px;margin-top:10px}

/* ---- 過去の台風（検索・一覧） ---- */
header{padding:12px 14px 6px;display:grid;gap:10px;flex:none}
.f{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.f input:not([type=checkbox]),.f select{width:100%}
.f .wide,.f details{grid-column:span 2}
.chips{display:flex;gap:6px;flex-wrap:wrap}
.chip,#dir,#reset{background:var(--s2);border:1px solid transparent;border-radius:99px;padding:5px 12px;font-size:12px;cursor:pointer}
.chip.on{background:var(--acc);color:#04121f;font-weight:700}
.sort{display:grid;grid-template-columns:1fr auto;gap:8px}
#dir{border-radius:10px;padding:0 14px;font-size:13px}
summary{cursor:pointer;color:var(--sub);font-size:13px;padding:2px 0}
.f2{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}
.f label{font-size:12px;color:var(--sub);display:flex;flex-direction:column;gap:3px}
.f label.chk{flex-direction:row;align-items:center;gap:6px;margin-top:0}
#reset{border-radius:10px;grid-column:span 2}
.pl2{display:grid;gap:8px;margin-top:8px}
.sw{display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer}
#meta{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:2px 16px 8px;flex:none}
#count{color:var(--sub);font-size:12px;transition:opacity .2s}#count.busy{opacity:.5}
#list{flex:1;overflow:auto;overscroll-behavior:contain;margin:0;padding:0 10px 10px;list-style:none;display:grid;gap:6px;align-content:start;-webkit-overflow-scrolling:touch;min-height:0}
#list li{display:flex;gap:12px;align-items:center;padding:10px 12px;border-radius:12px;background:transparent;cursor:pointer;border:1px solid transparent}
#list li:hover{background:var(--s2)}#list li.on{background:var(--s2);border-color:var(--acc)}
.bar{width:4px;align-self:stretch;border-radius:4px;flex:none}
.t{flex:1;min-width:0}.t b{display:block;font-size:14px}
.t span{color:var(--sub);font-size:12px}
.t em{font-style:normal;font-size:10px;margin-left:6px;padding:1px 6px;border-radius:99px;background:#f0803c22;color:#f0a06c;vertical-align:1px}
.m{height:3px;background:var(--line);border-radius:3px;margin-top:6px}.m i{display:block;height:100%;border-radius:3px}
.v{text-align:right;line-height:1.2}.v strong{font:700 20px "Space Grotesk",sans-serif}.v small{display:block;color:var(--sub);font-size:11px}
footer{padding:8px 16px;border-top:1px solid var(--line);color:var(--sub);font-size:11px;flex:none}

/* ---- めずらしい台風・移動距離 ---- */
.k-rev{--tc:#d8b4fe;--tbg:#c084fc26}.k-cin{--tc:#6ee7b7;--tbg:#34d39926}.k-cout{--tc:#5eead4;--tbg:#2dd4bf26}.k-lp{--tc:#fcd34d;--tbg:#fbbf2426}.k-ri{--tc:#fda4af;--tbg:#fb718526}
.t em.tg,.tgp{font-style:normal;margin-left:6px;padding:1px 6px;border-radius:99px;background:var(--tbg);color:var(--tc);font-size:10px;vertical-align:1px}
.sz,.t em.sz{display:inline-block;font-style:normal;font-size:10px;font-weight:700;margin-left:6px;padding:0 6px;border-radius:99px;background:transparent;border:1px solid var(--c);color:var(--c);vertical-align:1px}
.wzs{display:flex;flex-wrap:wrap;align-items:center;gap:6px;margin-top:8px}.wzs .chip{font-size:11px;padding:4px 10px}.wzs .chip i{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px;vertical-align:-1px;background:var(--c)}
.tgs{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}.tgs .tgp{margin:0;padding:2px 9px;font-size:11px;vertical-align:baseline}
.chl{flex:none;align-self:center;color:var(--sub);font-size:11px}
.lg span[data-tg]{cursor:pointer}
.rk{display:grid;gap:4px;margin:0;padding:0;list-style:none}
.rk li{display:flex;justify-content:space-between;gap:10px;align-items:center;background:var(--s2);border-radius:10px;padding:8px 12px;font-size:13px;cursor:pointer}
.rk li b{font:700 14px "Space Grotesk",system-ui,sans-serif;white-space:nowrap}

/* ---- 予報 ---- */
.sel{display:flex;gap:8px}.sel select{flex:1;min-width:0;height:44px;font-size:16px}
.rw{display:flex;align-items:center;gap:10px}.rw .lb{font-size:12px;color:var(--sub);flex:none}
#tau{flex:1;min-width:0;height:32px;accent-color:var(--acc)}#tv{min-width:56px;text-align:right;font:700 18px "Space Grotesk",system-ui,sans-serif}
#pos{display:grid;gap:6px}
.pr{display:grid;grid-template-columns:auto 1fr auto;gap:10px;align-items:center;background:var(--s2);border-radius:10px;padding:6px 12px 6px 8px}
.pn b{display:block;font-size:13px}.pn small{display:block;color:var(--sub);font-size:11px;font-variant-numeric:tabular-nums}
.iv{text-align:right;line-height:1.2}.iv strong{font:700 20px "Space Grotesk",system-ui,sans-serif}.iv small{display:block;color:var(--sub);font-size:11px}
.rg{display:inline-block;width:12px;height:12px;margin:5px;border-radius:50%;background:var(--f);box-shadow:0 0 0 2px var(--s2),0 0 0 5px var(--r)}
.rg.e,.mk.e{border-radius:3px;transform:rotate(45deg) scale(.85)}
.gh{display:flex;justify-content:space-between;align-items:center;width:100%;margin:12px 0 4px;padding:0;border:0;background:none;color:var(--sub);font-size:11px;cursor:pointer}
.srcs{display:flex;flex-wrap:wrap;gap:6px}
.src{border:1px solid var(--c);background:transparent;border-radius:99px;min-height:36px;padding:0 12px;font-size:13px;display:inline-flex;gap:8px;align-items:center;cursor:pointer}
.src small{color:var(--sub);font-size:11px}
.src.off{opacity:.35}.src.off .ln{opacity:.4}
.ln{display:inline-block;width:18px;height:0;border-top:3px solid var(--c)}.ln.d{border-top-style:dashed}
.mkw{background:none;border:0;display:flex;align-items:center;justify-content:center}
.mk{display:block;width:var(--s);height:var(--s);border-radius:50%;background:var(--f);box-shadow:0 0 0 2px #0a1120,0 0 0 5px var(--r)}
.mk.cur{box-shadow:0 0 0 2px #0a1120,0 0 0 5px var(--r),0 0 0 7px #fff}
.mlab2{background:none;border:0}
.sum{background:var(--s2);border-radius:10px;padding:8px 12px;font-size:13px;line-height:1.7}.sum b{display:block;font-size:12px;color:var(--sub);margin-bottom:2px}
.anr{display:block;width:100%;margin-top:6px;text-align:left;background:var(--s2);border:0;border-radius:10px;padding:8px 12px;cursor:pointer}
.anr b{display:block;font-size:13px}.anr small{display:block;color:var(--sub);font-size:11px;line-height:1.5;font-variant-numeric:tabular-nums}.anr.on{outline:2px solid var(--acc)}
.mlab2 span{position:absolute;left:12px;top:-9px;padding:0 5px;border-radius:5px;background:rgba(10,17,32,.88);color:#fff;font:700 11px system-ui,sans-serif;white-space:nowrap}

/* ---- 統計 ---- */
.cond{display:grid;gap:6px;justify-items:start;margin-top:8px}
.kpi{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:12px 0}
.kpi div{background:var(--s2);border-radius:10px;padding:8px 12px}.kpi .w2{grid-column:span 2}
.kpi small{display:block;color:var(--sub);font-size:11px}
.kpi strong{font:700 20px "Space Grotesk",system-ui,sans-serif}.kpi strong span{font-size:11px;font-weight:400;color:var(--sub);margin-left:3px}
.bs{width:100%;height:90px;display:block;background:var(--s2);border-radius:10px}.ax{display:flex;justify-content:space-between;color:var(--sub);font-size:11px;margin-top:2px}
.cb{display:flex;height:14px;border-radius:7px;overflow:hidden;background:var(--s2)}.cb i{display:block}
.lg{display:flex;flex-wrap:wrap;gap:4px 12px;margin-top:6px;font-size:12px}.lg i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:4px}
.segw{position:sticky;top:0;z-index:3;background:var(--s1);padding:10px 0 6px;margin-top:4px}
.seg{display:flex;gap:4px;background:var(--s2);padding:3px;border-radius:12px}
.seg button{flex:1;height:34px;border:0;border-radius:9px;background:none;color:var(--sub);font-size:13px;font-weight:700;cursor:pointer}
.seg button[aria-selected=true]{background:var(--acc);color:#04121f}
.pace{background:var(--s2);border-radius:12px;padding:10px 12px;margin-top:10px}
.pace small{display:block;color:var(--sub);font-size:11px;line-height:1.6}
.pv{display:flex;align-items:baseline;gap:4px 10px;flex-wrap:wrap;margin:2px 0 8px}
.pv strong{font:700 26px "Space Grotesk",system-ui,sans-serif}.pv strong span{font-size:12px;font-weight:400;color:var(--sub);margin-left:3px}
.pv em{font-style:normal;font-size:12px;color:var(--sub)}.pv b.up{color:#ff8a3d}.pv b.dn{color:#4fc3f7}
.pb{position:relative;height:8px;border-radius:4px;background:var(--bg);margin-bottom:8px}.pb i{display:block;height:100%;border-radius:4px;background:var(--acc)}
.pb u{position:absolute;top:-3px;width:2px;height:14px;background:#fff;text-decoration:none}
.ch{display:block;width:100%;height:auto;touch-action:pan-y;-webkit-user-select:none;user-select:none}
.ch .hl{fill:#fff;opacity:0}.ch .hl.on{opacity:.12}
.rd{min-height:46px;margin-top:6px;font-size:12px;line-height:1.8}.rd b{font-size:13px}.rd .btn{margin-left:6px;vertical-align:middle}
.tb{width:100%;border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums}
.tb th{color:var(--sub);font-weight:400;font-size:11px;text-align:right;padding:4px 6px}.tb td{text-align:right;padding:7px 6px;border-top:1px solid var(--line)}
.tb th:first-child,.tb td:first-child{text-align:left}
.hs{display:grid;grid-template-columns:repeat(12,1fr);gap:2px}.hs div{border-radius:6px;padding:4px 0;text-align:center}
.hs small{display:block;font-size:9px;color:var(--sub)}.hs b{font-size:11px}
.lg [data-dr]{cursor:pointer}
.lk{display:inline-block;width:14px;height:0;border-top:2px dashed #fff;margin-right:4px;vertical-align:middle}

/* ---- 地図まわり ---- */
main{position:relative;min-height:0}#map{height:100%;background:#0b1522;z-index:0}
.card{position:absolute;z-index:500;background:rgba(16,26,48,.92);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--line);border-radius:var(--r)}
#info{right:14px;top:14px;padding:12px 14px;width:320px;max-width:calc(100% - 28px)}
body[data-tab=fc] #info,body[data-tab=st] #info,body[data-tab=lv] #info{display:none}
.hd{display:flex;align-items:flex-start;gap:8px}.hd h2{flex:1;margin:0;font-size:17px;min-width:0}
#info .sub{color:var(--sub);font-size:12px}
.g{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:10px}
.g div{background:var(--s2);border-radius:10px;padding:6px 10px}
.g small{display:block;color:var(--sub);font-size:11px}.g strong{font:700 17px "Space Grotesk",sans-serif}.g strong span{font-size:11px;font-weight:400;color:var(--sub)}
.st{display:none;flex-wrap:wrap;align-items:baseline;gap:2px 12px;margin-top:6px;font-size:13px}.st b{font-size:14px}
.pl{display:flex;align-items:center;gap:10px;margin-top:10px}
#play{min-width:64px;background:var(--acc);color:#04121f;font-weight:700}
.chart{flex:1;min-width:0;position:relative}
.chart svg{display:block;width:100%;height:40px}
#scrub{width:100%;margin:0;accent-color:var(--acc);height:20px}
#rd{font-size:12px;color:var(--sub);margin-top:4px;font-variant-numeric:tabular-nums}
.acts{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:10px}
.acts .btn{height:42px;padding:0}.mo{display:none}
#fit{position:absolute;z-index:600;left:14px;top:14px;height:38px;padding:0 14px;border:1px solid var(--line);border-radius:12px;background:rgba(16,26,48,.92);font-size:13px;cursor:pointer}
#legend{left:14px;bottom:24px;padding:8px 12px;font-size:12px}
#legend div{display:flex;gap:8px;align-items:center}#legend i{width:14px;height:4px;border-radius:2px;display:inline-block;flex:none}
#legend i.dot{width:var(--s);height:var(--s);border-radius:50%}
#legend .lgh{color:var(--sub);font-size:11px;margin-top:4px}
#fab{display:none}
dialog{background:var(--s1);color:var(--ink);border:1px solid var(--line);border-radius:18px;width:min(560px,94vw);max-height:86dvh;padding:16px;overflow:auto}
dialog::backdrop{background:#000b}dialog h2{margin:0;font-size:17px}
table{width:100%;border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums}th,td{padding:6px 4px;text-align:right;border-bottom:1px solid var(--line)}th:first-child,td:first-child{text-align:left}
#toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);background:var(--s2);border:1px solid var(--line);padding:8px 16px;border-radius:99px;font-size:13px;z-index:2000;opacity:0;pointer-events:none;transition:opacity .2s}
#toast.on{opacity:1}
#bd{display:none}
.leaflet-tooltip{background:var(--s1);color:var(--ink);border:1px solid var(--line);border-radius:8px}
.leaflet-control-attribution{font-size:9px}

/* ---- 実況 ---- */
.mets{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.mt{display:block;width:100%;text-align:left;border:1px solid transparent;border-radius:10px;background:var(--s2);padding:7px 10px;cursor:pointer;color:var(--ink)}
.mt.on{border-color:var(--acc)}
.mt small{display:block;color:var(--sub);font-size:11px}
.mt strong{font:700 18px "Space Grotesk",system-ui,sans-serif}.mt strong span{font-size:11px;font-weight:400;color:var(--sub);margin-left:2px}
.mt em{display:block;font-style:normal;color:var(--sub);font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tb small{color:var(--sub);font-size:10px}
.lvh{margin:14px 0 4px;font-size:13px;font-weight:700}
.wsum{display:flex;flex-wrap:wrap;gap:6px;margin:2px 0 8px}
.wr{display:flex;align-items:center;gap:6px;flex-wrap:wrap;background:var(--s2);border-radius:10px;padding:7px 10px;border-left:4px solid var(--wc);margin-top:6px}
.wr b{font-size:13px;flex:none;min-width:5.5em}
.wr small{margin-left:auto;color:var(--sub);font-size:11px}
.wr.l1{--wc:#fbbf24}.wr.l2{--wc:#ef4444}.wr.l3{--wc:#a855f7}.wr.l4{--wc:#f1f5f9}
.wb{font-style:normal;font-size:11px;font-weight:700;padding:1px 8px;border-radius:99px;white-space:nowrap}
.wb.l1{background:#fbbf24;color:#1a1200}.wb.l2{background:#ef4444;color:#fff}.wb.l3{background:#a855f7;color:#fff}.wb.l4{background:#f1f5f9;color:#1e1b4b}
.lvk{color:var(--sub);font-size:11px;display:flex;gap:10px;flex-wrap:wrap;margin:4px 0 6px}.lvk .wb{font-size:10px;padding:0 7px}

/* ---- スマホ ---- */
@media(max-width:760px){
 :root{--tb:calc(58px + env(safe-area-inset-bottom))}
 body{display:block}
 #tabs{left:0;right:0;top:auto;bottom:0;width:auto;height:var(--tb);padding:6px 10px calc(6px + env(safe-area-inset-bottom));border-right:0;border-bottom:0;border-top:1px solid var(--line)}
 main{position:fixed;inset:0 0 var(--tb) 0}
 #bd{display:block;position:fixed;inset:0 0 var(--tb) 0;z-index:1400;background:rgba(0,0,0,.5);opacity:0;pointer-events:none;transition:opacity .28s}
 body.lst #bd{opacity:1;pointer-events:auto}
 aside{position:fixed;left:0;right:0;bottom:var(--tb);height:calc(86dvh - var(--tb));z-index:1500;padding-top:0;border:0;border-top:1px solid var(--line);border-radius:20px 20px 0 0;box-shadow:0 -12px 40px #000a;transform:translateY(115%);transition:transform .3s cubic-bezier(.2,.8,.2,1);overscroll-behavior:contain}
 body.lst aside{transform:none}
 body[data-tab=fc] aside,body[data-tab=lv] aside{height:auto;max-height:46dvh;transform:none;z-index:900;box-shadow:0 -8px 30px #0008}
 .grab{display:block;height:20px;position:relative;flex:none}.grab::before{content:"";position:absolute;left:50%;top:9px;width:44px;height:5px;margin-left:-22px;border-radius:3px;background:var(--line)}
 .shd{display:flex;justify-content:space-between;align-items:center;padding:0 14px 8px;flex:none}.shd b{font-size:16px}
 body[data-tab=fc] .grab,body[data-tab=fc] .shd,body[data-tab=lv] .grab,body[data-tab=lv] .shd{display:none}
 #s-fc .pad,#s-lv .pad{padding-top:14px}
 header{max-height:52%;overflow:auto;padding:0 12px 8px;gap:10px;overscroll-behavior:contain}
 .f input:not([type=checkbox]),.f select{font-size:16px;padding:12px 14px;min-height:46px}  /* iOSの自動ズーム防止 */
 .chips{flex-wrap:nowrap;overflow-x:auto;margin:0 -12px;padding:0 12px;scrollbar-width:none}.chips::-webkit-scrollbar{display:none}
 .chip{flex:none;min-height:40px;padding:0 16px;font-size:14px}
 #dir{min-width:56px}#reset{min-height:46px}summary{padding:10px 0;font-size:14px}
 #meta{padding:4px 14px 8px}
 #list{padding:0 8px 16px;gap:4px}#list li{padding:14px 12px;min-height:64px}.t b{font-size:15px}
 #info{left:0;right:0;bottom:0;top:auto;width:auto;max-width:none;border-radius:20px 20px 0 0;border-width:1px 0 0;padding:12px 14px 12px}
 .g{display:none}.st{display:flex}.mo{display:block}.acts{grid-template-columns:repeat(5,1fr)}
 #play{height:44px}.pl{margin-top:8px}.chart svg{height:26px}#scrub{height:28px}
 .leaflet-control-zoom,.leaflet-control-attribution{display:none}
 #legend{left:10px;top:10px;bottom:auto;display:flex;gap:10px;padding:6px 10px;font-size:11px;max-width:calc(100% - 100px);overflow-x:auto;scrollbar-width:none;align-items:center}
 #legend div{flex:none;gap:4px}#legend .lgh{margin-top:0}
 #fit{left:auto;right:10px;top:10px;height:40px}
 #fab{position:absolute;z-index:600;left:50%;bottom:16px;transform:translateX(-50%);padding:0 26px;height:48px;border:0;border-radius:99px;background:var(--acc);color:#04121f;font-weight:700;font-size:15px;box-shadow:0 6px 20px #0008}
 body[data-tab=db] #fab{display:block}
 #info:not([hidden])~#fab{display:none}
 #toast{top:64px;bottom:auto}
}
@media(max-width:760px) and (max-height:520px){.st,.chart svg{display:none}}
</style></head><body data-tab="db">
<div id="bd"></div>
<nav id="tabs" role="tablist" aria-label="画面の切り替え">
 <button type="button" role="tab" data-t="fc" aria-selected="false"><b>予報</b><small>現在の台風</small></button>
 <button type="button" role="tab" data-t="db" aria-selected="true"><b>過去の台風</b><small>検索・進路</small></button>
 <button type="button" role="tab" data-t="st" aria-selected="false"><b>統計</b><small>集計</small></button>
 <button type="button" role="tab" data-t="lv" aria-selected="false"><b>実況</b><small>観測・警報</small></button>
</nav>
<aside>
 <div class="grab" id="grab"></div>
 <div class="shd"><b id="shT">台風を探す</b><button type="button" class="btn sm" id="cl">閉じる</button></div>

 <!-- 画面1: 予報 -->
 <section id="s-fc" class="sec">
  <div class="pad">
   <div class="sel"><select id="fsel" aria-label="予報を見る台風"></select><button type="button" class="btn" id="frf">更新</button></div>
   <div class="rw"><span class="lb">予報時間</span><button type="button" class="btn sm" id="fplay" aria-label="再生" style="padding:0 10px">▶</button><input id="tau" type="range" min="0" max="120" step="6" value="48" aria-label="予報時間"><b id="tv">+48h</b></div>
   <div id="fnote" class="note"></div>
  </div>
  <div class="scroll">
   <div id="fsum" class="sum" hidden></div>
   <h3 class="sh">選んだ時間の予報（位置と強さ）</h3><div id="pos"></div>
   <h3 class="sh">強さの表示</h3>
   <div class="btns"><button type="button" class="btn on" id="oInlay">線を強さで色分け</button><button type="button" class="btn" id="oNum">風速を数字で</button>
    <select id="oStep" aria-label="印の間隔"><option value="12">12時間ごと</option><option value="24" selected>24時間ごと</option></select></div>
   <div class="note" style="margin-top:6px">進路線は、極細の外枠の色が予報の出どころ（モデル）、内側の実線の色が強さです。印も同じで、中の色と大きさが強さ、外側の輪がモデル。円は単独モデル、菱形はアンサンブル平均。ボタンを切ると線はモデルの色だけになります。</div>
   <h3 class="sh">ソース（タップで表示・非表示）</h3><div id="fchips"></div>
   <label class="chk"><input type="checkbox" id="mem" checked>アンサンブルの各メンバーの進路も表示</label>
   <h3 class="sh">似た過去の台風</h3>
   <label class="chk" style="margin-top:0"><input type="checkbox" id="anOn">地図に重ねる（線の色＝当時の強さ ／ 細い線＝それまで、太い線＝その後）</label>
   <div id="an"></div>
   <h3 class="sh">詳しく見る</h3>
   <div class="btns"><button type="button" class="btn" id="tbl">位置のばらつき表</button><button type="button" class="btn" id="int">強さ予報のグラフ</button></div>
   <div id="fsrc" class="note" style="margin-top:16px"></div>
  </div>
 </section>

 <!-- 画面2: 過去の台風 -->
 <section id="s-db" class="sec">
  <header>
   <form class="f" id="f" onsubmit="return false">
    <input class="wide" name="name" type="search" enterkeyhint="search" autocomplete="off" placeholder="名前・号数で検索（例: SURIGAE / 15号）">
    <div class="chips wide">
     <button type="button" class="chip" data-w="">全ての強さ</button><button type="button" class="chip" data-w="33">強い〜</button>
     <button type="button" class="chip" data-w="44">非常に強い〜</button><button type="button" class="chip" data-w="54">猛烈な</button></div>
    <div class="chips wide" id="tgChips" aria-label="特徴で絞り込み"><button type="button" class="chip" data-tg="rev">復活台風</button><button type="button" class="chip" data-tg="cin">越境台風</button><button type="button" class="chip" data-tg="cout">180度の東へ</button><button type="button" class="chip" data-tg="lp">迷走(ループ)</button><button type="button" class="chip" data-tg="ri">急発達</button></div>
    <div class="chips wide" id="szChips" aria-label="大きさで絞り込み（最大時の強風域）"><button type="button" class="chip" data-sz="">全ての大きさ</button><button type="button" class="chip" data-sz="l,xl">大型以上</button><button type="button" class="chip" data-sz="xl">超大型</button><input type="hidden" name="size" value=""></div>
    <div class="note wide" id="tgNote" hidden></div>
    <div class="sort wide"><select name="sort"><option value="number">号数順</option><option value="date">発生日順</option><option value="wind">最大風速順</option><option value="pres">最低気圧順</option><option value="days">継続日数順</option><option value="dist">移動距離順</option><option value="size">強風域の大きさ順</option><option value="name">名前順</option></select>
     <button type="button" id="dir" title="昇順・降順">降順</button><input type="hidden" name="order" value="desc"></div>
    <details id="dPl"><summary>地点から探す（近くを通った台風）</summary><div class="pl2">
     <div class="btns"><button type="button" class="btn" id="tNear">現在地から</button><button type="button" class="btn" id="tPick">地図で指定</button><button type="button" class="btn" id="tClr" hidden>指定を解除</button></div>
     <label>範囲<select name="km"><option value="100">100km以内</option><option value="300" selected>300km以内</option><option value="500">500km以内</option><option value="1000">1000km以内</option><option value="r30">強風域に入った</option><option value="r50">暴風域に入った</option></select></label>
     <div class="note" id="nearTxt">地点を指定すると、その近くを通った台風だけを表示します。</div></div></details>
    <details><summary>詳細条件</summary><div class="f2">
     <label>年（から）<select name="year_from"><option value="">指定なし</option></select></label>
     <label>年（まで）<select name="year_to"><option value="">指定なし</option></select></label>
     <label>発生月<select name="month"><option value="">全て</option></select></label>
     <label>表示件数<select name="limit"><option>100</option><option selected>300</option><option>1000</option></select></label>
     <label>最大風速 m/s 以上<input type="number" inputmode="numeric" name="wind_min" min="0"></label>
     <label>最大風速 m/s 以下<input type="number" inputmode="numeric" name="wind_max" min="0"></label>
     <label>最低気圧 hPa 以下<input type="number" inputmode="numeric" name="pres_max" placeholder="例: 930"></label>
     <label>継続日数 以上<input type="number" inputmode="decimal" name="days_min" min="0" step="0.5"></label>
     <label>移動距離 km 以上<input type="number" inputmode="numeric" name="dist_min" min="0" placeholder="例: 6000"></label>
     <label>移動距離 km 以下<input type="number" inputmode="numeric" name="dist_max" min="0" placeholder="例: 1500"></label>
     <label class="chk"><input type="checkbox" name="named">名前付きのみ</label>
     <button type="button" id="reset">条件をリセット</button>
    </div></details>
   </form>
   <label class="sw"><input type="checkbox" id="ovSw"><span>検索結果の進路を全て地図に重ねる</span></label>
  </header>
  <div id="meta"><span id="count"></span><span class="pill" id="upd"></span></div>
  <ul id="list"></ul>
  <footer>出典: 気象庁（ベストトラック／位置表。IBTrACS経由）。風速は10分平均、時刻は日本時間。「速報」は速報値で後日修正されます。移動距離と、復活・越境・ループ・急発達の印は、各点の位置・風速からの自動判定（参考値）です。地図タイル: Esri。</footer>
 </section>

 <!-- 画面3: 統計 -->
 <section id="s-st" class="sec"><div class="scroll" id="stBody"></div></section>

  <!-- 画面4: 実況 -->
  <section id="s-lv" class="sec">
   <div class="pad">
    <div class="sel"><select id="lsel" aria-label="実況を見る台風"></select><button type="button" class="btn" id="lrf">更新</button></div>
    <div id="lnote" class="note"></div>
   </div>
   <div class="scroll">
    <h3 class="sh">台風の実況（気象庁）</h3><div id="lvNow"></div>
    <h3 class="sh">進路予報（気象庁）</h3><div id="lvFc"></div>
    <h3 class="sh">地図の表示</h3>
    <div class="btns"><button type="button" class="btn on" id="lvTrk">進路・予報円</button><button type="button" class="btn on" id="lvRad">暴風域・強風域</button><button type="button" class="btn on" id="lvObs">観測値</button><button type="button" class="btn on" id="lvWn">警報・注意報の塗り</button></div>
    <div class="note" style="margin-top:6px">白の破線＝これまでの進路、白の実線と円＝予報（円の中に台風の中心が入る確率は70%）、赤い帯＝暴風警戒域。実況の赤い破線＝暴風域、黄色の破線＝強風域。地図の薄い塗りは警報・注意報（黄＝注意報、赤＝警報、紫＝危険警報、白＝特別警報。拡大すると市町村等ごとに細かくなります）。</div>
    <h3 class="sh" id="lvObsT">観測（アメダス）— タップで地図に表示</h3>
    <div class="rw" style="margin-bottom:8px"><span class="lb">範囲</span><select id="lvRng" class="in" style="flex:1;min-width:0" aria-label="観測値の範囲"><option value="auto" id="lvRngA" selected>自動（台風の強さ・大きさに合わせる）</option><option value="0">全国</option><option value="300">台風の中心から300km以内</option><option value="500">台風の中心から500km以内</option><option value="700">台風の中心から700km以内</option><option value="1000">台風の中心から1000km以内</option><option value="1500">台風の中心から1500km以内</option></select></div>
    <div id="lvRngN" class="note" style="margin:-2px 0 8px"></div>
    <div class="mets" id="lvMet"></div>
    <h3 class="sh" id="lvTopT">上位10地点</h3><ul class="rk" id="lvTop"></ul>
    <h3 class="sh">警報・注意報</h3>
    <div id="lvWsum" class="wsum"></div>
    <div class="lvk"><span><i class="wb l4">特別警報</i></span><span><i class="wb l3">危険警報</i></span><span><i class="wb l2">警報</i></span><span><i class="wb l1">注意報</i></span></div>
    <label class="chk" style="margin-top:0"><input type="checkbox" id="lvAll">台風に関係しない種類（雷・濃霧・乾燥など）も表示</label>
    <div id="lvWarn"></div>
    <div id="lvSrc" class="note" style="margin-top:16px">出典: 気象庁（防災情報・アメダス・警報注意報）。台風の風速は10分間平均、時刻は日本時間。観測値は速報で、欠測や品質の低い値は除いています。5分ごとに自動で更新します。</div>
   </div>
  </section>
</aside>
<main><div id="map"></div><button id="fit" type="button">全体を表示</button><div id="info" class="card" hidden></div><button id="fab" type="button">台風を探す</button><div id="legend" class="card"></div></main>
<dialog id="dlg"></dialog>
<div id="toast"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/polygon-clipping@0.15.7/dist/polygon-clipping.umd.min.js"></script>
<script>
// ================= 共通 =================
const TABS={fc:"予報",db:"過去の台風",st:"統計",lv:"実況"};
const $=s=>document.querySelector(s);
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const mobile=()=>matchMedia("(max-width:760px)").matches;
const store={get(){try{return JSON.parse(localStorage.getItem("tf")||"{}")}catch(e){return{}}},set(v){try{localStorage.setItem("tf",JSON.stringify(v))}catch(e){}}};
const tabMem={get(){try{return localStorage.getItem("tf_tab")}catch(e){return null}},set(v){try{localStorage.setItem("tf_tab",v)}catch(e){}}};
function toast(m){const t=$("#toast");t.textContent=m;t.classList.add("on");clearTimeout(toast.t);toast.t=setTimeout(()=>t.classList.remove("on"),1800)}
// 強さの階級（気象庁・10分間平均風速 m/s）。過去の台風・予報で同じ色を使う
const CLS=[[54,"猛烈な","#ff4d7d"],[44,"非常に強い","#ff8a3d"],[33,"強い","#ffd24a"],[17,"台風","#4fc3f7"],[0,"熱帯低気圧","#64789a"]];
const UNK=[null,"不明","#3d4f66"];
const FCLS=[[54,"猛烈な","#ff4d7d",19],[44,"非常に強い","#ff8a3d",16],[33,"強い","#ffd24a",13],[17.2,"台風","#4fc3f7",10],[0,"熱帯低気圧","#64789a",8]];  // 4番目=予報マーカーの大きさ(px)
const UNKF=[null,"不明","#3d4f66",8];
// 台風の大きさ（気象庁）: 強風域(15m/s以上)の最大半径が 500km以上=大型、800km以上=超大型。全タブ共通
const SZ=[[800,"超大型","#f0abfc"],[500,"大型","#c084fc"]];
const szK=km=>km?SZ.find(z=>km>=z[0])||null:null;
// 気象庁の階級（lb）が取れていればそれを優先（"-" は階級なし=大型未満）。取れない時だけ強風域の半径から判定する
const szJ=(km,lb)=>{if(typeof lb==="string"&&lb.trim()){const t=lb.trim();if(t.indexOf("超大型")>=0)return SZ[0];if(t.indexOf("大型")>=0)return SZ[1];if(t==="-")return null}return szK(km)};
const szTag=(km,lb)=>{const z=szJ(km,lb);return z?`<em class="sz" style="--c:${z[2]}">${z[1]}</em>`:""};
const szLg=()=>`<div class="lgh">大きさ（強風域の半径）</div>`+SZ.slice().reverse().map(z=>`<div><i style="background:${z[2]}"></i>${z[1]} ${z[0]}km以上</div>`).join("");
const initHash=location.hash.slice(1);
const initTab=TABS[initHash]?initHash:(initHash?"db":(tabMem.get()||"db"));
let curSid=TABS[initHash]?"":initHash;   // 過去の台風タブで表示中の台風
let tab="";

const map=L.map("map",{worldCopyJump:true,zoomControl:false}).setView([25,135],4);
L.control.zoom({position:"bottomright"}).addTo(map);
const ESRI="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/";
L.tileLayer(ESRI+"World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",{attribution:"Tiles © Esri — Esri, DeLorme, NAVTEQ | 気象庁 / IBTrACS (NOAA) / UCAR RAL / Google DeepMind",maxZoom:12}).addTo(map);
map.createPane("labels").style.zIndex=450;map.getPane("labels").style.pointerEvents="none";
L.tileLayer(ESRI+"World_Dark_Gray_Reference/MapServer/tile/{z}/{y}/{x}",{pane:"labels",maxZoom:12}).addTo(map);
addEventListener("resize",()=>map.invalidateSize());

// ================= 画面の切り替え =================
const views={};
const dbLayerList=[];   // 過去の台風の地図レイヤー（予報タブでは外す）
function dbLayers(on){dbLayerList.forEach(l=>on?l.addTo(map):l.remove())}
function legend(){
  const el=$("#legend");
  if(tab=="lv"){el.innerHTML=LV.legend();return}
  el.innerHTML=(tab=="fc"
    ?`<div class="lgh">強さ（印の中）</div>`+FCLS.map(c=>`<div><i class="dot" style="--s:${c[3]}px;background:${c[2]}"></i>${c[1]}</div>`).join("")+`<div class="lgh">細い外枠・輪 = モデル</div>`
    :[...CLS,UNK].map(c=>`<div><i style="background:${c[2]}"></i>${c[1]}</div>`).join(""))+szLg();
}
function setTab(t){
  if(!TABS[t])t="db";
  const GRP=x=>x=="fc"?"fc":x=="lv"?"lv":"db",pg=tab?GRP(tab):"",g=GRP(t);
  tab=t;document.body.dataset.tab=t;tabMem.set(t);
  document.querySelectorAll("#tabs button").forEach(b=>b.setAttribute("aria-selected",String(b.dataset.t==t)));
  $("#shT").textContent=t=="st"?"統計":"台風を探す";
  if(g!=pg){
    if(pg)views[pg]={c:map.getCenter(),z:map.getZoom()};
    if(g=="fc")FC.show();else FC.hide();
    if(g=="lv")LV.show();else LV.hide();
    dbLayers(g=="db");
    map.invalidateSize();
    const v=views[g];
    if(v)map.setView(v.c,v.z,{animate:false});
    else if(g=="fc")FC.fit();
    else if(g=="lv")LV.fit();
    else if(cur)map.fitBounds(cur.bounds,fitOpt());
  }
  if(mobile()){if(t=="st")openPanel();else closePanel()}
  if(t=="st")loadStats();
  legend();
  history.replaceState(null,"",t=="db"?(curSid?"#"+curSid:location.pathname+location.search):"#"+t);
}
$("#tabs").onclick=e=>{const b=e.target.closest("button");if(!b)return;const t=b.dataset.t;
  if(t==tab&&mobile()&&t!="fc"&&t!="lv"){openPanel();return}
  setTab(t)};
$("#fit").onclick=()=>{if(tab=="fc")FC.fit();else if(tab=="lv")LV.fit();else if(cur)map.fitBounds(cur.bounds,fitOpt())};

// ================= 過去の台風 =================
const K=w=>w==null?UNK:CLS.find(c=>w>=c[0]),col=w=>K(w)[2],cls=w=>K(w)[1];
const spd=w=>w==null?"-":`${w}m/s`;
// めずらしい台風（サーバー側で各台風の点列から自動判定）
const TGN={rev:"復活台風",cin:"越境台風",cout:"180度の東へ抜けた台風",lp:"迷走（ループ）",ri:"急発達"};
const TGS={rev:"復活",cin:"越境",cout:"域外へ",lp:"ループ",ri:"急発達"};
const TGD={rev:"熱帯低気圧に弱まった後、同じ号数のまま再び台風（17m/s以上）になった",cin:"東経180度より東（ハリケーンなど）から北西太平洋に入って台風になった",cout:"東経180度を越えて、その東側（中部太平洋）へ進んだ",lp:"進路がほぼ1周するように回った",ri:"24時間で最大風速が約15m/s以上強まった"};
const tags=new Set();
const dst=n=>Math.round(n).toLocaleString();
const tgTxt=(k,t)=>k=="rev"?`復活台風${t.rev>1?`（${t.rev}回）`:""}`:k=="ri"?`急発達（24時間で+${t.ri}m/s）`:TGN[k];
function tgSync(){document.querySelectorAll("#tgChips [data-tg]").forEach(b=>b.classList.toggle("on",tags.has(b.dataset.tg)));
  const n=$("#tgNote");n.hidden=!tags.size;n.textContent=[...tags].map(k=>TGN[k]+"＝"+TGD[k]).join(" ／ ")+(tags.size>1?"（全てに当てはまるもの）":"")}
$("#tgChips").onclick=e=>{const b=e.target.closest("[data-tg]");if(!b)return;const k=b.dataset.tg;tags.has(k)?tags.delete(k):tags.add(k);tgSync();load()};
const jst=(s,full)=>{const d=new Date(new Date(s.replace(" ","T")+"Z").getTime()+9*3600e3),z=n=>String(n).padStart(2,"0");
  return `${full?d.getUTCFullYear()+"/":""}${d.getUTCMonth()+1}/${d.getUTCDate()} ${z(d.getUTCHours())}時`};
const f=$("#f"),chips=document.querySelectorAll(".chip[data-w]");
const km=(a,b)=>{const r=Math.PI/180,dl=(b[1]-a[1])*r,p1=a[0]*r,p2=b[0]*r;
  return 6371*Math.acos(Math.min(1,Math.sin(p1)*Math.sin(p2)+Math.cos(p1)*Math.cos(p2)*Math.cos(dl)))};
const layer=L.layerGroup(),nearL=L.layerGroup(),ovL=L.layerGroup();dbLayerList.push(layer,nearL,ovL);
map.createPane("ov").style.zIndex=380;
// 強風域・暴風域（気象庁の半径。データがある台風・時刻だけ）。軌跡に沿って1つの帯につなげて描く
map.createPane("wz").style.zIndex=375;
const wzL=L.layerGroup(),wzN=L.layerGroup();dbLayerList.push(wzL,wzN);
const wz={r50:true,r30:true},WZC={r50:"#fb7185",r30:"#fbbf24"},WZN={r50:"暴風域",r30:"強風域"},WZF={r50:.2,r30:.11};   // 既定ON
const BR=[null,45,90,135,180,225,270,315,0];   // 向きコード 1=NE 2=E 3=SE 4=S 5=SW 6=W 7=NW 8=N（0・9=円）
const wzMP=L.Projection.SphericalMercator,
  wzM=p=>{const q=wzMP.project(L.latLng(p[0],p[1]));return [q.x,q.y]},
  wzU=p=>{const q=wzMP.unproject(L.point(p[0],p[1]));return [q.lat,q.lng]};
function wzDest(la,lo,b,km){const R=Math.PI/180,d=km/6371,t=b*R,p1=la*R,p2=Math.asin(Math.sin(p1)*Math.cos(d)+Math.cos(p1)*Math.sin(d)*Math.cos(t)),
  dl=Math.atan2(Math.sin(t)*Math.sin(d)*Math.cos(p1),Math.cos(d)-Math.sin(p1)*Math.sin(p2));return [p2/R,lo+dl/R]}
// 気象庁の定義どおり: 長径方向に(長径-短径)/2ずらした点を中心に、半径(長径+短径)/2の円
function wzShape(la,lo,r,k){const o=k=="r50"?0:3,lg=r[o+1];if(!lg)return null;
  const sh=Math.min(r[o+2]||lg,lg),b0=BR[r[o]];let c=[la,lo],rad=lg;
  if(b0!=null&&sh<lg){c=wzDest(la,lo,b0,(lg-sh)/2);rad=(lg+sh)/2}
  const g=[];for(let a=0;a<360;a+=5)g.push(wzDest(c[0],c[1],a,rad));return g}
function wzHull(P){P=P.slice().sort((a,b)=>a[0]-b[0]||a[1]-b[1]);const cr=(o,a,b)=>(a[0]-o[0])*(b[1]-o[1])-(a[1]-o[1])*(b[0]-o[0]);
  const lo=[],up=[];for(const p of P){while(lo.length>1&&cr(lo[lo.length-2],lo[lo.length-1],p)<=0)lo.pop();lo.push(p)}
  for(const p of P.slice().reverse()){while(up.length>1&&cr(up[up.length-2],up[up.length-1],p)<=0)up.pop();up.push(p)}
  lo.pop();up.pop();return lo.concat(up)}
const wzArea=P=>P.reduce((s,a,i)=>{const b=P[(i+1)%P.length];return s+a[0]*b[1]-b[0]*a[1]},0);
// 帯の外形: 各時刻の形を「円（中心のずれ＋半径）」にし、時刻の並びに沿って軽くなめらかにしてから、
// 隣り合う円どうしの凸包（=外接線でつないだ形）を重ねて1つの外形にする。
// 半径が観測ごとに増減しても、境界が波打たない（描画用の平滑化。今の位置の破線は観測値そのまま）。12時間より空いた所はつなげない
function wzCirc(c,rad,st){const g=[];for(let a=0;a<360;a+=st)g.push(wzDest(c[0],c[1],a,rad));return g}
function wzBand(k){if(cur.wzc[k])return cur.wzc[k];
  const o=k=="r50"?0:3,R=Math.PI/180,
    T=cur.pts.map(p=>new Date(p.time.replace(" ","T")+"Z").getTime()),
    it=cur.pts.map((p,i)=>{const r=p.r,lg=r&&r[o+1];if(!lg)return null;
      const sh=Math.min(r[o+2]||lg,lg),b0=BR[r[o]],d=b0!=null&&sh<lg?(lg-sh)/2:0;
      return {i,ex:d*Math.sin((b0||0)*R),ny:d*Math.cos((b0||0)*R),rad:b0!=null&&sh<lg?(lg+sh)/2:lg}}),
    polys=[];
  for(let s=0;s<it.length;){if(!it[s]){s++;continue}
    let e=s;while(e+1<it.length&&it[e+1]&&T[e+1]-T[e]<=12*36e5)e++;
    const run=it.slice(s,e+1);
    for(let n=0;n<3;n++){const q=run.map(x=>({...x}));   // [1,2,1]/4 を3回（両端は観測値のまま）
      for(let j=1;j<run.length-1;j++)for(const f of["ex","ny","rad"])q[j][f]=(run[j-1][f]+2*run[j][f]+run[j+1][f])/4;
      q.forEach((x,j)=>run[j]=x)}
    const cs=run.map(x=>{const la=cur.ll[x.i][0],lo=cur.ll[x.i][1],d=Math.hypot(x.ex,x.ny),
      c=d>1e-6?wzDest(la,lo,Math.atan2(x.ex,x.ny)/R,d):[la,lo];return wzCirc(c,x.rad,5).map(wzM)});
    cs.forEach((g,j)=>polys.push([j?wzHull(cs[j-1].concat(g)):g]));
    s=e+1}
  let g=null,fb=false;
  if(polys.length){
    try{if(window.polygonClipping)g=polygonClipping.union(...polys).map(pg=>pg.map(rg=>rg.map(wzU)))}catch(e){g=null}
    if(!g){fb=true;g=polys.map(pg=>[(wzArea(pg[0])<0?pg[0].slice().reverse():pg[0]).map(wzU)])}}   // ライブラリが読めない時は枠線なしで重ねる
  return cur.wzc[k]={g:g||[],fb}}
function drawWz(){wzL.clearLayers();if(!cur)return;
  ["r30","r50"].forEach(k=>{if(!wz[k])return;const b=wzBand(k);if(!b.g.length)return;
    L.polygon(b.g,{pane:"wz",stroke:!b.fb,color:WZC[k],weight:1.6,opacity:.9,lineJoin:"round",fillColor:WZC[k],fillOpacity:WZF[k],fillRule:"nonzero",interactive:false}).addTo(wzL)})}
function drawWzNow(){wzN.clearLayers();if(!cur)return;const p=cur.pts[cur.i];if(!p||!p.r)return;
  ["r30","r50"].forEach(k=>{if(!wz[k])return;const g=wzShape(cur.ll[cur.i][0],cur.ll[cur.i][1],p.r,k);
    if(g)L.polygon(g,{pane:"wz",color:WZC[k],weight:2.5,dashArray:"7 5",fillColor:WZC[k],fillOpacity:.16,interactive:false}).addTo(wzN)})}
// 再生バーのグラフに、半径の推移を細い点線で重ねる（強風域・暴風域のチップがONの分だけ。縦軸は共通で、最大の強風半径=グラフの高さ）
function drawRg(){const g=$("#rg");if(!g||!cur)return;
  const P=cur.pts,mx=Math.max(1,...P.map(p=>p.r?Math.max(p.r[1]||0,p.r[4]||0):0));
  g.innerHTML=["r30","r50"].filter(k=>wz[k]).map(k=>{const o=k=="r50"?0:3;let d="",on=false;
    P.forEach((p,i)=>{const v=p.r&&p.r[o+1];if(!v){on=false;return}
      d+=`${on?"L":"M"}${cur.xs[i].toFixed(1)},${(38-v/mx*34).toFixed(1)}`;on=true});
    return d?`<path d="${d}" fill="none" stroke="${WZC[k]}" stroke-width="1.5" stroke-dasharray="4 3" opacity=".9" vector-effect="non-scaling-stroke"><title>${WZN[k]}の半径（最大 ${Math.round(Math.max(0,...P.map(p=>p.r&&p.r[o+1]||0)))}km）</title></path>`:""}).join("")}
let first=true,cur=null,playT=null,near=null,pick=false,ovOn=false,lastP=new URLSearchParams();

// 一覧パネル（スマホ: 下から出るシート）
function openPanel(){document.body.classList.add("lst");setTimeout(()=>{const l=$("#list li.on");l&&reveal(l)},60)}
function closePanel(){document.body.classList.remove("lst")}
$("#cl").onclick=closePanel;$("#fab").onclick=openPanel;$("#bd").onclick=closePanel;
{let sy=null,dy=0;const a=$("aside");   // シートを下へスワイプして閉じる
 for(const el of [$("#grab"),$(".shd")]){
  el.addEventListener("touchstart",e=>{sy=e.touches[0].clientY;dy=0;a.style.transition="none"},{passive:true});
  el.addEventListener("touchmove",e=>{if(sy==null)return;dy=Math.max(0,e.touches[0].clientY-sy);a.style.transform=`translateY(${dy}px)`},{passive:true});
  el.addEventListener("touchend",()=>{if(sy==null)return;sy=null;a.style.transition="";a.style.transform="";if(dy>80)closePanel()})}}
f.name.addEventListener("keydown",e=>{if(e.key=="Enter")e.target.blur()});  // 検索確定でキーボードを閉じる
function reveal(li){const L2=$("#list"),a=li.getBoundingClientRect(),b=L2.getBoundingClientRect();
  if(a.top<b.top)L2.scrollTop-=b.top-a.top+8;else if(a.bottom>b.bottom)L2.scrollTop+=a.bottom-b.bottom+8}
const barH=()=>mobile()&&!$("#info").hidden?$("#info").offsetHeight:0;
const fitOpt=()=>mobile()?{paddingTopLeft:[16,54],paddingBottomRight:[16,barH()+16]}:{paddingTopLeft:[40,40],paddingBottomRight:[360,40]};

let jmaAt="";
async function dbInit(){
  const y=await (await fetch("/api/years")).json();
  for(let i=y.max;i>=y.min;i--){f.year_from.add(new Option(i+"年",i));f.year_to.add(new Option(i+"年",i))}
  for(let m=1;m<=12;m++) f.month.add(new Option(m+"月",m));
  const sv=store.get();for(const k in sv) if(f[k]&&f[k].type!="checkbox") f[k].value=sv[k];  // 前回の絞り込みを復元
  f.named.checked=sv.named==="true";
  (sv.tag||"").split(",").filter(k=>TGN[k]).forEach(k=>tags.add(k));tgSync();
  jmaAt=y.jma_at||"";if(jmaAt) $("#upd").innerHTML=`速報 <b>${jst(jmaAt)}</b> 更新`;
  $("#reset").onclick=()=>{f.reset();f.order.value="desc";near=null;tags.clear();tgSync();load()};
  $("#dir").onclick=()=>{f.order.value=f.order.value=="desc"?"asc":"desc";load()};
  chips.forEach(c=>c.onclick=()=>{f.wind_min.value=c.dataset.w;f.wind_max.value="";load()});
  $("#szChips").onclick=e=>{const b=e.target.closest("[data-sz]");if(!b)return;f.size.value=b.dataset.sz;load()};
  f.addEventListener("input",e=>{if(e.isComposing)return;clearTimeout(dbInit.t);dbInit.t=setTimeout(load,250)});
  f.addEventListener("compositionend",()=>{clearTimeout(dbInit.t);dbInit.t=setTimeout(load,250)});  // 日本語入力の確定後に検索
  load();
}
let ctl;
async function load(){
  const p=new URLSearchParams();
  for(const k of ["name","year_from","year_to","month","wind_min","wind_max","pres_max","days_min","dist_min","dist_max","size","sort","order","limit"]) if(f[k].value) p.set(k,f[k].value);
  if(f.named.checked) p.set("named","true");
  if(tags.size) p.set("tag",[...tags].join(","));
  store.set(Object.fromEntries(p));
  if(near)p.set("near",`${near[0]},${near[1]},${f.km.value}`);
  lastP=p;drawNear();if(ovOn)drawOverlay(p);if(tab=="st")loadStats();
  chips.forEach(c=>c.classList.toggle("on",c.dataset.w===f.wind_min.value&&!f.wind_max.value));
  document.querySelectorAll("#szChips [data-sz]").forEach(c=>c.classList.toggle("on",c.dataset.sz===f.size.value));
  $("#dir").textContent=f.order.value=="desc"?"降順":"昇順";
  ctl&&ctl.abort();ctl=new AbortController();$("#count").classList.add("busy");  // 古いリクエストを破棄
  try{
    const {total,rows}=await (await fetch("/api/typhoons?"+p,{signal:ctl.signal})).json();
    $("#count").classList.remove("busy");
    $("#count").textContent=total?`${total}件`+(total>rows.length?`中 ${rows.length}件を表示（件数を増やすか条件を絞ってください）`:""):"該当なし。条件を変えてください";
    $("#list").innerHTML=rows.map(r=>`<li data-sid="${esc(r.sid)}"><span class="bar" style="background:${col(r.max_wind)}"></span>
     <div class="t"><b>${esc(r.title)}${r.prov?"<em>速報</em>":""}${(r.tg||[]).map(k=>`<em class="tg k-${k}">${TGS[k]}</em>`).join("")}${szTag(r.r30)}</b><span>${jst(r.start_time,1)}〜 ／ ${r.days}日${f.sort.value=="size"?(r.r30?` ／ 強風域 ${dst(r.r30)}km`:" ／ 半径データなし"):(r.dist!=null?` ／ ${dst(r.dist)}km`:"")}</span>
     <div class="m"><i style="width:${Math.min(100,(r.max_wind||0)/0.67)}%;background:${col(r.max_wind)}"></i></div></div>
     <div class="v"><strong>${r.max_wind??"-"}</strong><small>m/s</small><small>${r.min_pres??"-"}hPa</small></div></li>`).join("");
    const on=curSid&&$(`#list li[data-sid="${CSS.escape(curSid)}"]`);if(on)on.classList.add("on");
    if(first){first=false;const s=curSid||(rows[0]&&rows[0].sid);if(s)show(s)}  // 共有リンク or 最新の台風を自動表示
  }catch(e){if(e.name!="AbortError"){$("#count").classList.remove("busy");$("#count").textContent="読み込みに失敗しました。再読み込みしてください"}}
}
$("#list").addEventListener("click",e=>{const li=e.target.closest("li");if(li){if(mobile())closePanel();show(li.dataset.sid,li)}});
function nav(d){const a=[...document.querySelectorAll("#list li")],i=a.findIndex(x=>x.classList.contains("on")),n=a[i+d];if(n)show(n.dataset.sid,n)}
addEventListener("keydown",e=>{if(tab!="db"||/INPUT|SELECT|TEXTAREA/.test(e.target.tagName))return;
  if(e.key=="ArrowDown"||e.key=="j"){e.preventDefault();nav(1)}else if(e.key=="ArrowUp"||e.key=="k"){e.preventDefault();nav(-1)}else if(e.key==" "&&cur){e.preventDefault();play()}});
// 進行アニメーション
function setPos(i){if(!cur)return;cur.i=i;const p=cur.pts[i];cur.mk.setLatLng(cur.ll[i]);drawWzNow();
  $("#scrub").value=i;$("#rd").textContent=`${jst(p.time,1)} ・ ${spd(p.wind)} ・ ${p.pres??"-"}hPa`+["r50","r30"].filter(k=>wz[k]&&p.r&&p.r[(k=="r50"?0:3)+1]).map(k=>` ・ ${WZN[k]}${p.r[(k=="r50"?0:3)+1]}km`).join("")+(p.r&&szK(p.r[4])?` ・ ${szK(p.r[4])[1]}`:"");
  const c=$("#cur");if(c){c.setAttribute("x1",cur.xs[i]);c.setAttribute("x2",cur.xs[i])}
  if(tab!="db")return;
  const pt=map.latLngToContainerPoint(cur.ll[i]),s=map.getSize(),bh=barH(),rx=mobile()?0:350,top=mobile()?50:20;
  if(pt.x<24||pt.x>s.x-rx-24||pt.y<top||pt.y>s.y-bh-24)map.panBy([pt.x-(s.x-rx)/2,pt.y-(top+(s.y-bh-top)/2)])}
function stop(){clearInterval(playT);playT=null;const b=$("#play");if(b)b.textContent="再生"}
function play(){if(!cur)return;if(playT)return stop();$("#play").textContent="停止";if(cur.i>=cur.pts.length-1)setPos(0);
  playT=setInterval(()=>{if(cur.i>=cur.pts.length-1)return stop();setPos(cur.i+1)},Math.max(60,Math.min(250,6000/cur.pts.length)))}
async function share(){const u=location.origin+location.pathname+"#"+curSid,t=$("#info h2").textContent;
  try{if(navigator.share)await navigator.share({title:t,url:u});else{await navigator.clipboard.writeText(u);toast("リンクをコピーしました")}}catch(e){}}
function csv(){if(!cur)return toast("台風を選択してください");
  const nm=($("#info h2").firstChild.textContent||"typhoon").replace(/[\\/:*?"<>|\s]+/g,"_");
  const rows=["time_utc,lat,lon,wind_ms,pres_hpa",...cur.pts.map(p=>[p.time,p.lat,p.lon,p.wind??"",p.pres??""].join(","))];
  const a=document.createElement("a");a.href=URL.createObjectURL(new Blob(["\ufeff"+rows.join("\n")],{type:"text/csv"}));a.download=nm+".csv";a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1e3)}
$("#info").addEventListener("click",e=>{const b=e.target.closest("[data-a]");if(!b)return;
  ({prev:()=>nav(-1),next:()=>nav(1),share,csv,play,list:openPanel,close:()=>{$("#info").hidden=true;stop()}})[b.dataset.a]()});
$("#info").addEventListener("click",e=>{const b=e.target.closest("[data-wz]");if(!b)return;
  const k=b.dataset.wz;wz[k]=!wz[k];b.classList.toggle("on",wz[k]);drawWz();drawWzNow();drawRg();setPos(cur?cur.i:0)});
let seq=0;
async function show(sid,li,keep){
  const my=++seq;stop();curSid=sid;
  document.querySelectorAll("#list li.on").forEach(x=>x.classList.remove("on"));
  li=li||document.querySelector(`#list li[data-sid="${CSS.escape(sid)}"]`);
  if(li){li.classList.add("on");if(!keep)reveal(li)}
  if(tab=="db")history.replaceState(null,"","#"+sid);
  const r=await fetch("/api/typhoons/"+encodeURIComponent(sid));if(!r.ok||my!=seq)return;
  const t=await r.json();if(my!=seq)return;
  layer.clearLayers();
  const pts=t.track;
  // 日付変更線をまたぐ場合に備えて経度を連続化
  let prev=null;const ll=pts.map(p=>{let lo=p.lon;if(prev!==null){while(lo-prev>180)lo-=360;while(lo-prev<-180)lo+=360}prev=lo;return [p.lat,lo]});
  L.polyline(ll,{color:"#fff",weight:9,opacity:.08}).addTo(layer);
  for(let i=1;i<pts.length;i++) L.polyline([ll[i-1],ll[i]],{color:col(pts[i].wind),weight:4,lineCap:"round"}).addTo(layer);
  pts.forEach((p,i)=>{const e=i==0||i==pts.length-1;
    L.circleMarker(ll[i],{radius:e?6:3,color:"#fff",weight:1,fillColor:col(p.wind),fillOpacity:1,interactive:false}).addTo(layer);
    L.circleMarker(ll[i],{radius:14,stroke:false,fillOpacity:0,bubblingMouseEvents:false}).on("click",()=>{stop();setPos(i)})
     .bindTooltip(`${i==0?"発生 ":e?"終了 ":""}${jst(p.time,1)}<br>${spd(p.wind)} / ${p.pres??"-"}hPa`).addTo(layer)});
  if(t.rev_t){const ia=pts.findIndex(p=>p.time===t.rev_a),ib=pts.findIndex(p=>p.time===t.rev_t);
    if(ia>=0&&ib>ia){L.polyline(ll.slice(ia,ib+1),{color:"#c084fc",weight:6,opacity:.55,dashArray:"2 8",lineCap:"round",interactive:false}).addTo(layer);
      L.circleMarker(ll[ib],{radius:11,color:"#c084fc",weight:3,fillOpacity:0,interactive:false}).addTo(layer);
      L.circleMarker(ll[ib],{radius:14,stroke:false,fillOpacity:0,bubblingMouseEvents:false}).bindTooltip(`再び台風に発達 ${jst(pts[ib].time,1)}`).addTo(layer)}}
  if(t.cin||t.cout){const dl=180+360*Math.round((ll[0][1]-180)/360);
    L.polyline([[-50,dl],[60,dl]],{color:"#c084fc",weight:1,opacity:.6,dashArray:"4 6",interactive:false}).addTo(layer)}
  const n=pts.length,W=300,xs=pts.map((_,i)=>n>1?i/(n-1)*W:0),mw=Math.max(1,...pts.map(p=>p.wind||0));
  const wl=pts.map((p,i)=>p.wind==null?null:`${xs[i].toFixed(1)},${(38-p.wind/mw*34).toFixed(1)}`).filter(Boolean).join(" ");
  const dist=Math.round((t.dist!=null?t.dist:ll.slice(1).reduce((s,p,i)=>s+km(ll[i],p),0))/10)*10;
  const spdv=dist&&t.days>0?Math.round(dist/(t.days*24)):"-";
  const wzh=t.has_r?`<div class="wzs"><button type="button" class="chip${wz.r50?" on":""}" data-wz="r50" style="--c:${WZC.r50}"><i></i>暴風域（25m/s以上）</button><button type="button" class="chip${wz.r30?" on":""}" data-wz="r30" style="--c:${WZC.r30}"><i></i>強風域（15m/s以上）</button></div>
   <div class="note" style="margin-top:4px">気象庁の半径から作図（データのある時刻のみ）。軌跡に沿って影響範囲を1つの帯につなげて表示し、今の位置は破線で強調します。</div>`:"";
  const tgl=(t.tg||[]).length?`<div class="tgs">${t.tg.map(k=>`<span class="tgp k-${k}" title="${esc(TGD[k])}">${esc(tgTxt(k,t))}</span>`).join("")}</div>`:"";
  cur={pts,ll,xs,i:0,wzc:{},bounds:L.latLngBounds(ll),mk:L.circleMarker(ll[0],{radius:9,color:"#fff",weight:2,fillColor:"#38bdf8",fillOpacity:.9,interactive:false}).addTo(layer)};
  const i=$("#info");i.hidden=false;
  i.innerHTML=`<div class="hd"><h2>${esc(t.title)}${t.prov?'<span class="pill" style="margin-left:8px">速報値</span>':""}</h2><button type="button" class="btn sm" data-a="close">閉じる</button></div>
   <div class="sub">${jst(t.start_time,1)} 〜 ${jst(t.end_time,1)}（日本時間）${dist?` ・ 約${dist.toLocaleString()}km`:""}</div>
   ${tgl}${wzh}
   <div class="g"><div><small>最大風速</small><strong>${t.max_wind??"-"}<span> m/s</span></strong></div>
   <div><small>強さ</small><strong style="color:${col(t.max_wind)}">${cls(t.max_wind)}</strong></div>
   <div><small>最低気圧</small><strong>${t.min_pres??"-"}<span> hPa</span></strong></div>
   <div><small>継続</small><strong>${t.days}<span> 日</span></strong></div>
   <div><small>移動距離</small><strong>${dist?dist.toLocaleString():"-"}<span> km</span></strong></div>
   <div><small>平均の速さ</small><strong>${spdv}<span> km/h</span></strong></div>
   <div><small>大きさ（最大時）</small><strong style="color:${szK(t.r30)?szK(t.r30)[2]:"var(--ink)"}">${szK(t.r30)?szK(t.r30)[1]:t.r30?"大型未満":"-"}</strong></div>
   <div><small>強風域の最大半径</small><strong>${t.r30?dst(t.r30):"-"}<span> km</span></strong></div></div>
   <div class="st"><b style="color:${col(t.max_wind)}">${cls(t.max_wind)}</b><span>最大 ${spd(t.max_wind)}</span><span>${t.min_pres??"-"}hPa</span><span>${t.days}日</span>${dist?`<span>${dist.toLocaleString()}km</span>`:""}${t.r30?`<span>強風域 最大${dst(t.r30)}km${szTag(t.r30)}${t.r50?` ／ 暴風域 最大${dst(t.r50)}km`:""}</span>`:""}</div>
   <div class="pl"><button type="button" class="btn" id="play" data-a="play">再生</button><div class="chart"><div id="rd"></div>
    <svg viewBox="0 0 ${W} 40" preserveAspectRatio="none"><polyline points="${wl}" fill="none" stroke="${col(t.max_wind)}" stroke-width="2" vector-effect="non-scaling-stroke"/><g id="rg"></g>
    <line id="cur" x1="0" x2="0" y1="0" y2="40" stroke="#fff" stroke-width="1" vector-effect="non-scaling-stroke"/></svg>
    <input id="scrub" type="range" min="0" max="${Math.max(0,n-1)}" value="0" aria-label="時刻"></div></div>
   <div class="acts"><button type="button" class="btn mo" data-a="list">一覧</button><button type="button" class="btn" data-a="prev">前へ</button><button type="button" class="btn" data-a="next">次へ</button><button type="button" class="btn" data-a="share">共有</button><button type="button" class="btn" data-a="csv">CSV</button></div>`;
  $("#scrub").oninput=e=>{stop();setPos(+e.target.value)};
  if(!keep&&tab=="db")map.fitBounds(cur.bounds,fitOpt());
  drawWz();drawRg();setPos(0);
}
// ---- 地点から探す / 重ね表示 ----
function drawNear(){nearL.clearLayers();$("#tNear").classList.toggle("on",!!near);$("#tClr").hidden=!near;
  const zk=/^r\d+$/.test(f.km.value);   // r30/r50 = 強風域・暴風域に入った台風
  $("#nearTxt").textContent=near?(zk?`北緯${near[0].toFixed(1)}度・東経${near[1].toFixed(1)}度が${WZN[f.km.value]}に入った台風を表示中`:`北緯${near[0].toFixed(1)}度・東経${near[1].toFixed(1)}度から ${f.km.value}km 以内を通った台風を表示中`):"地点を指定すると、その近くを通った台風だけを表示します。";
  if(near){$("#dPl").open=true;if(!zk)L.circle(near,{radius:f.km.value*1000,color:"#38bdf8",weight:1,fillOpacity:.07,interactive:false}).addTo(nearL);
    L.circleMarker(near,{radius:5,color:"#fff",weight:2,fillColor:"#38bdf8",fillOpacity:1,interactive:false}).addTo(nearL)}}
function setNear(la,lo){near=la==null?null:[+la.toFixed(3),+lo.toFixed(3)];load();
  if(near)map.fitBounds(L.circle(near,{radius:(/^r\d+$/.test(f.km.value)?500:f.km.value)*1000}).getBounds(),fitOpt())}
$("#tNear").onclick=()=>{if(near)return setNear(null);
  if(!navigator.geolocation)return toast("位置情報を使えません");toast("現在地を取得中…");
  navigator.geolocation.getCurrentPosition(p=>setNear(p.coords.latitude,p.coords.longitude),()=>toast("現在地を取得できません。「地図で指定」をお試しください"),{timeout:10000})};
$("#tClr").onclick=()=>setNear(null);
$("#tPick").onclick=()=>{pick=true;toast("地図をタップして地点を指定");if(mobile())closePanel()};
map.on("click",e=>{if(!pick)return;pick=false;const l=e.latlng.wrap();setNear(l.lat,l.lng);if(mobile())openPanel()});
async function drawOverlay(p){const q=new URLSearchParams(p);["limit","sort","order"].forEach(k=>q.delete(k));
  try{const r=await (await fetch("/api/tracks?"+q)).json();ovL.clearLayers();
    r.forEach(t=>{let pv=null;const ll=t.p.map(([la,lo])=>{if(pv!==null){while(lo-pv>180)lo-=360;while(lo-pv<-180)lo+=360}pv=lo;return [la,lo]});
      L.polyline(ll,{pane:"ov",color:col(t.w),weight:2,opacity:.5}).bindTooltip(esc(t.title)).on("click",()=>{if(mobile())closePanel();show(t.sid)}).addTo(ovL)});
    return r.length}catch(e){}}
$("#ovSw").onchange=async e=>{ovOn=e.target.checked;
  if(!ovOn){ovL.clearLayers();return}
  const n=await drawOverlay(lastP);toast(n?`${n}本を重ねて表示（タップで選択）`:"該当なし");if(n&&mobile())closePanel()};

// ================= 統計 =================
function condText(p){const a=[],g=k=>p.get(k);
  if(g("name"))a.push(`名前「${g("name")}」`);
  if(g("year_from")||g("year_to"))a.push(`${g("year_from")||"最初"}年〜${g("year_to")||"最新"}年`);
  if(g("month"))a.push(`${g("month")}月に発生`);
  if(g("wind_min"))a.push(`最大風速${g("wind_min")}m/s以上`);
  if(g("wind_max"))a.push(`最大風速${g("wind_max")}m/s以下`);
  if(g("pres_max"))a.push(`最低気圧${g("pres_max")}hPa以下`);
  if(g("days_min"))a.push(`継続${g("days_min")}日以上`);
  if(g("named"))a.push("名前付きのみ");
  if(g("dist_min"))a.push(`移動距離${g("dist_min")}km以上`);
  if(g("dist_max"))a.push(`移動距離${g("dist_max")}km以下`);
  if(g("size"))a.push(g("size").split(",").map(k=>k=="xl"?"超大型":"大型").join("・")+"（最大時の強風域が"+(g("size")=="xl"?"800":"500")+"km以上"+(g("size")=="l"?"800km未満":"")+"）");
  if(g("tag"))a.push(g("tag").split(",").map(k=>TGN[k]||k).join("かつ"));
  if(g("near")){const[la,lo,k]=g("near").split(",");a.push(/^r\d+$/.test(k)?`地点（北緯${(+la).toFixed(1)}度・東経${(+lo).toFixed(1)}度）が${WZN[k]}に入った`:`地点（北緯${(+la).toFixed(1)}度・東経${(+lo).toFixed(1)}度）から${k}km以内`)}
  return a.length?a.join(" ／ "):"条件なし（全ての台風）"}
let stSeq=0,stD=null,stSub="ov",stRec="wind",stCh={},stSel={};
const STS=[["ov","概要"],["tr","推移"],["se","季節"],["rc","記録"]];
const RECS=[["wind","風速"],["pres","気圧"],["days","継続"],["dist","距離"],["size","強風域"]];
const RECN={wind:"最大風速（10分平均）。同じ風速なら気圧が低い順",pres:"最低中心気圧",days:"発生から消滅までの日数",dist:"各点を結んだ道のり",size:"強風域（15m/s以上）の最大半径。半径のデータがある台風のみ"};
const CW=[{wind_min:54,wind_max:""},{wind_min:44,wind_max:53},{wind_min:33,wind_max:43},{wind_min:17,wind_max:32},{wind_min:"",wind_max:16}];  // 強さ階級ごとの風速の範囲
const SC=CLS.map(c=>c[2]);
const pct=(a,b)=>b?Math.round(a/b*100):0;
function drill(o){for(const k in o)f[k].value=o[k];setTab("db");load();if(mobile())openPanel()}
// 積み上げ棒グラフ。data=[{x:軸ラベル,p:[値…(下から)],c:[色…]}]、o={c:既定の色,line:[折れ線の値…],h:高さ}
function stChart(id,data,o){
  const W=320,H=o.h||150,ml=28,mr=4,mt=8,mb=18,iw=W-ml-mr,ih=H-mt-mb,n=data.length,bw=iw/n,
    m0=Math.max(1,...data.map(d=>d.p.reduce((a,b)=>a+b,0)),...(o.line||[]).map(v=>v||0)),
    st=[1,2,5,10,20,50,100,200,500,1000].find(s=>m0/s<=4)||1000,mx=Math.ceil(m0/st)*st,Y=v=>mt+ih-v/mx*ih;
  let g="";
  for(let v=0;v<=mx;v+=st)g+=`<line x1="${ml}" x2="${W-mr}" y1="${Y(v).toFixed(1)}" y2="${Y(v).toFixed(1)}" stroke="#22314f"/><text x="${ml-4}" y="${(Y(v)+3).toFixed(1)}" text-anchor="end" font-size="9" fill="#8b9bbb">${v}</text>`;
  data.forEach((d,i)=>{const x=ml+i*bw,gap=Math.min(1.5,bw*.12);let b=0;
    g+=`<rect class="hl" x="${x.toFixed(2)}" y="${mt}" width="${bw.toFixed(2)}" height="${ih}"/>`;
    d.p.forEach((v,k)=>{if(!v)return;const y0=Y(b+v),h=Y(b)-y0;b+=v;
      g+=`<rect x="${(x+gap).toFixed(2)}" y="${y0.toFixed(2)}" width="${Math.max(.5,bw-gap*2).toFixed(2)}" height="${h.toFixed(2)}" fill="${(d.c||o.c)[k]}"/>`});
    if(d.x!=="")g+=`<text x="${(x+bw/2).toFixed(1)}" y="${H-5}" text-anchor="middle" font-size="9" fill="#8b9bbb">${d.x}</text>`});
  if(o.line){const pts=o.line.map((v,i)=>v==null?null:`${(ml+i*bw+bw/2).toFixed(1)},${Y(v).toFixed(1)}`).filter(Boolean);
    if(pts.length>1)g+=`<polyline points="${pts.join(" ")}" fill="none" stroke="#fff" stroke-width="1.6" stroke-dasharray="4 3" stroke-linejoin="round"/>`}
  return `<svg class="ch" data-ch="${id}" data-n="${n}" viewBox="0 0 ${W} ${H}">${g}</svg><div class="rd" id="rd-${id}"></div>`}
function stPick(e){const sv=e.target.closest&&e.target.closest("svg.ch");if(!sv)return;
  const r=sv.getBoundingClientRect(),n=+sv.dataset.n,i=Math.floor(((e.clientX-r.left)/r.width*320-28)/((320-32)/n)),id=sv.dataset.ch,c=stCh[id];
  if(!c||i<0||i>=n||stSel[id]===i)return;
  stSel[id]=i;sv.querySelectorAll(".hl").forEach((h,k)=>h.classList.toggle("on",k==i));$("#rd-"+id).innerHTML=c.rd(i)}
// ---- 概要 ----
function stOv(s){
  const tot=s.total||1,p=s.pace,tg=s.tags||{},
    cl=[...CLS.map((c,i)=>[c[1],s.classes[c[1]]||0,c[2],i]),[UNK[1],s.classes[UNK[1]]||0,UNK[2],-1]].filter(c=>c[1]),
    sz=s.sizes||{},zl=[["超大型",sz.xl||0,SZ[0][2],"xl"],["大型",sz.l||0,SZ[1][2],"l"],["大型未満",sz.n||0,"#475a7a",null],["半径データなし",sz.x||0,"#2a3850",null]].filter(c=>c[1]),
    tgh=Object.keys(TGN).map(k=>`<span class="k-${k}" data-tg="${k}"><i style="background:var(--tc)"></i>${TGN[k]} ${tg[k]||0}</span>`).join("");
  let pace="";
  if(p){const d=p.n-p.normal,mx=Math.max(p.n,p.normal,1)*1.15;
    pace=`<div class="pace"><small>${p.year}年の発生ペース（${p.md}まで・全期間の集計）</small>
     <div class="pv"><strong>${p.n}<span>個</span></strong><em>平年（1991〜2020年）は${p.normal}個 ／ <b class="${d>0?"up":d<0?"dn":""}">${d>0?"+":""}${d.toFixed(1)}</b></em></div>
     <div class="pb"><i style="width:${p.n/mx*100}%"></i><u style="left:${p.normal/mx*100}%"></u></div>
     <small>同じ時期までの発生数は、記録のある${p.of}年のうち多い方から${p.rank}位。平年の年間発生数は${p.full_normal}個です。</small></div>`}
  const wh=s.wind_hist,wn=wh.reduce((a,x)=>a+x[1],0),wi=wh.reduce((m,x,i)=>x[1]>wh[m][1]?i:m,0);
  stCh.wh={init:wi,rd:i=>{const lo=wh[i][0],a=lo==0?"14m/s以下":lo==70?"70m/s以上":`${lo}〜${lo+4}m/s`;
    return `<b>${a}</b> ${wh[i][1]}個（${pct(wh[i][1],wn)}%）<button type="button" class="btn sm" data-dr="w:${lo}">この範囲を一覧へ</button>`}};
  const hd=wh.map(([lo,n])=>({x:lo==0?"〜14":lo==70?"70〜":lo%10==0?String(lo):"",p:[n],c:[col(lo==0?10:lo+2)]}));
  return `${pace}
   <div class="kpi"><div><small>台風の数</small><strong>${s.total}<span>個</span></strong></div><div><small>年平均の発生数</small><strong>${s.avg_per_year??"-"}<span>個</span></strong></div>
    <div><small>平均の継続日数</small><strong>${s.avg_days??"-"}<span>日</span></strong></div><div><small>平均の移動距離</small><strong>${s.avg_dist!=null?dst(s.avg_dist):"-"}<span>km</span></strong></div></div>
   <h3 class="sh">強さ別（タップでその強さだけを一覧に表示）</h3><div class="cb">${cl.map(c=>`<i style="width:${c[1]/tot*100}%;background:${c[2]}"></i>`).join("")}</div>
   <div class="lg">${cl.map(c=>`<span${c[3]>=0?` data-dr="c:${c[3]}"`:""}><i style="background:${c[2]}"></i>${c[0]} ${c[1]}（${pct(c[1],tot)}%）</span>`).join("")}</div>
   <h3 class="sh">大きさ別（最大時の強風域・タップでその大きさだけを一覧に表示）</h3><div class="cb">${zl.map(c=>`<i style="width:${c[1]/tot*100}%;background:${c[2]}"></i>`).join("")}</div>
   <div class="lg">${zl.map(c=>`<span${c[3]?` data-dr="s:${c[3]}"`:""}><i style="background:${c[2]}"></i>${c[0]} ${c[1]}（${pct(c[1],tot)}%）</span>`).join("")}</div>
   <h3 class="sh">最大風速の分布（5m/sごと）</h3>${stChart("wh",hd,{c:[],h:130})}
   <h3 class="sh">めずらしい台風（タップでその台風だけを一覧に表示）</h3><div class="lg">${tgh}</div>
   <p class="note" style="margin-top:12px">年平均は、まだ終わっていない今年を除いて計算しています。</p>`}
// ---- 推移 ----
function stTr(s){
  const ys=s.years,cy=s.cur_year;
  if(ys.length<2)return '<p class="note" style="margin-top:12px">期間が1年だけなので推移は表示できません。「条件を変更する」で年の範囲を広げてください。</p>';
  const ma=ys.map((r,i)=>{if(r[0]==cy)return null;const a=ys.slice(Math.max(0,i-9),i+1).filter(x=>x[0]!=cy);return a.length>=5?a.reduce((t,x)=>t+x[1],0)/a.length:null});
  stCh.yr={init:ys.length-1,rd:i=>{const r=ys[i],aw=r[5]?(r[4]/r[5]).toFixed(1):"-";
    return `<b>${r[0]}年${r[0]==cy?"（途中）":""}</b> ${r[1]}個 ／ 強い以上 ${r[2]}個・非常に強い以上 ${r[3]}個 ／ 最大風速の平均 ${aw}m/s<button type="button" class="btn sm" data-dr="y:${r[0]}">この年を一覧へ</button>`}};
  const dec={};ys.filter(r=>r[0]!=cy).forEach(r=>{const k=Math.floor(r[0]/10)*10,d=dec[k]||(dec[k]=[0,0,0,0,0]);d[0]++;d[1]+=r[1];d[2]+=r[2];d[3]+=r[4];d[4]+=r[5]});
  const tr=Object.keys(dec).map(k=>{const d=dec[k];return `<tr><td>${k}年代</td><td>${(d[1]/d[0]).toFixed(1)}</td><td>${pct(d[2],d[1])}%</td><td>${d[4]?(d[3]/d[4]).toFixed(1):"-"}</td></tr>`}).join("");
  const data=ys.map(r=>({x:r[0]%10==0?String(r[0]):"",p:[r[2],r[1]-r[2]],c:["#ff8a3d","#38bdf8"]}));
  return `<h3 class="sh">年別の発生数</h3>${stChart("yr",data,{c:[],line:ma})}
   <div class="lg"><span><i style="background:#ff8a3d"></i>強い以上（33m/s〜）</span><span><i style="background:#38bdf8"></i>それ以外</span><span><span class="lk"></span>10年移動平均</span></div>
   ${tr?`<h3 class="sh">年代別のようす</h3><table class="tb"><tr><th></th><th>年平均の発生数</th><th>強い以上の割合</th><th>最大風速の平均(m/s)</th></tr>${tr}</table>`:""}
   <p class="note" style="margin-top:10px">今年のように終わっていない年は、移動平均と年代別から除いています。</p>`}
// ---- 季節 ----
function stSe(s){
  const M=s.mon_cls,tt=M.map(a=>a.reduce((x,y)=>x+y,0)),T=tt.reduce((x,y)=>x+y,0),pk=tt.indexOf(Math.max(...tt));
  if(!T)return '<p class="note" style="margin-top:12px">該当する台風がありません。</p>';
  stCh.mo={init:pk,rd:i=>{const a=M[i];return `<b>${i+1}月</b> ${tt[i]}個（全体の${pct(tt[i],T)}%）／ 猛烈な ${a[0]}・非常に強い ${a[1]}・強い ${a[2]}<button type="button" class="btn sm" data-dr="m:${i+1}">この月を一覧へ</button>`}};
  const data=M.map((a,i)=>({x:`${i+1}月`,p:[a[4],a[3],a[2],a[1],a[0]]})),hs=M.map((a,i)=>{const t=tt[i],r=t?(a[0]+a[1]+a[2])/t:0;
    return `<div style="background:rgba(255,138,61,${(t?.1+r*.8:.05).toFixed(2)})"><small>${i+1}月</small><b>${t?Math.round(r*100):"-"}</b></div>`}).join("");
  return `<h3 class="sh">月別の発生数</h3>${stChart("mo",data,{c:[...SC].reverse()})}
   <div class="lg">${CLS.map(c=>`<span><i style="background:${c[2]}"></i>${c[1]}</span>`).join("")}</div>
   <p style="font-size:13px;margin:10px 0 0">最も多いのは<b>${pk+1}月</b>（${tt[pk]}個）。7〜10月で全体の<b>${pct(tt[6]+tt[7]+tt[8]+tt[9],T)}%</b>です。</p>
   <h3 class="sh">月別の「強い以上」の割合（%）</h3><div class="hs">${hs}</div>
   <p class="note" style="margin-top:6px">その月に発生した台風のうち、最大風速33m/s以上になったものの割合。色が濃いほど強まりやすい時期です。</p>`}
// ---- 記録 ----
function stRc(s){
  const L=(s.rec||{})[stRec]||[],u={wind:r=>`${r.v}m/s${r.p?` ・ ${r.p}hPa`:""}`,pres:r=>`${r.v}hPa${r.w!=null?` ・ ${r.w}m/s`:""}`,days:r=>`${r.v}日`,dist:r=>`${dst(r.v)}km`,size:r=>`${dst(r.v)}km${szTag(r.v)}`}[stRec];
  return `<div class="chips" style="margin-top:10px">${RECS.map(([k,n])=>`<button type="button" class="chip${k==stRec?" on":""}" data-rec="${k}">${n}</button>`).join("")}</div>
   ${L.length?`<ul class="rk" style="margin-top:8px">${L.map((r,i)=>`<li data-sid="${esc(r.sid)}"><span>${i+1}. ${esc(r.title)}</span><b>${u(r)}</b></li>`).join("")}</ul>`:'<p class="note" style="margin-top:8px">該当する台風がありません。</p>'}
   <p class="note" style="margin-top:8px">${RECN[stRec]}（上位10）。タップで地図に表示します。</p>`}
function stRender(){
  if(!stD)return;stCh={};stSel={};
  document.querySelectorAll("#stSeg button").forEach(b=>b.setAttribute("aria-selected",String(b.dataset.sub==stSub)));
  $("#stPane").innerHTML=({ov:stOv,tr:stTr,se:stSe,rc:stRc}[stSub]||stOv)(stD);
  for(const id in stCh){const sv=$(`svg.ch[data-ch="${id}"]`),i=stCh[id].init||0,h=sv&&sv.querySelectorAll(".hl")[i];
    if(h){h.classList.add("on");stSel[id]=i;$("#rd-"+id).innerHTML=stCh[id].rd(i)}}}
async function loadStats(){
  const my=++stSeq,b=$("#stBody");b.innerHTML='<p class="note">集計中…</p>';
  const q=new URLSearchParams(lastP);["limit","sort","order"].forEach(k=>q.delete(k));
  try{const r=await fetch("/api/stats?"+q);if(!r.ok)throw 0;const s=await r.json();if(my!=stSeq)return;stD=s;
    b.innerHTML=`<div class="cond"><div class="note">集計の対象（「過去の台風」で絞り込んだ条件）</div><b>${esc(condText(lastP))}</b><button type="button" class="btn sm" data-a="gotodb">条件を変更する</button></div>
     <div class="segw"><div class="seg" id="stSeg" role="tablist">${STS.map(([k,n])=>`<button type="button" role="tab" data-sub="${k}">${n}</button>`).join("")}</div></div><div id="stPane"></div>`;
    stRender()}
  catch(e){if(my==stSeq)b.innerHTML='<p class="note">集計に失敗しました。時間をおいて再度お試しください</p>'}}
$("#stBody").addEventListener("pointerdown",stPick);
$("#stBody").addEventListener("pointermove",e=>{if(e.pointerType=="mouse"||e.buttons)stPick(e)});
$("#stBody").addEventListener("click",e=>{
  if(e.target.closest("[data-a=gotodb]"))return setTab("db");
  const sb=e.target.closest("[data-sub]");if(sb){stSub=sb.dataset.sub;stRender();return}
  const rc=e.target.closest("[data-rec]");if(rc){stRec=rc.dataset.rec;stRender();return}
  const dr=e.target.closest("[data-dr]");
  if(dr){const[k,v]=dr.dataset.dr.split(":"),n=+v;
    drill(k=="y"?{year_from:n,year_to:n}:k=="m"?{month:n}:k=="s"?{size:v}:k=="c"?CW[n]:{wind_min:n?n:"",wind_max:n==70?"":n==0?14:n+4});return}
  const g=e.target.closest("[data-tg]");if(g){tags.clear();tags.add(g.dataset.tg);tgSync();setTab("db");load();if(mobile())openPanel();return}
  const r=e.target.closest(".rk li");if(r){setTab("db");show(r.dataset.sid)}});

// ================= 予報 =================
// 表現のルール:  線と外側の輪の色 = 予報の出どころ(モデル) / 印の中の色と大きさ = 強さ / 円 = 単独モデル、菱形 = アンサンブル平均
const FC=(()=>{
const COL={"JMA公式":"#ffffff",UKMET:"#ff8a3d",NAVGEM:"#a78bfa",CMC:"#34d399",GEFS:"#38bdf8","CMC-EPS":"#10b981","NAVGEM-EPS":"#c4b5fd",ECMWF:"#f43f5e",GFS:"#facc15","JMA-GSM":"#f9a8d4","JTWC公式":"#ef4444","ECMWF-ENS":"#fb7185","UKMET-ENS":"#fb923c","JMA-GEPS":"#e879f9","WeatherNext3(AI)":"#22d3ee","WeatherNext2(AI)":"#2dd4bf","GenCast(AI)":"#a3e635"};
const PAL=["#f59e0b","#14b8a6","#8b5cf6","#ec4899","#84cc16","#0ea5e9","#f97316","#22c55e","#e11d48","#06b6d4"];
const colr=k=>COL[k]||PAL[[...String(k)].reduce((a,c)=>a+c.charCodeAt(0),0)%PAL.length];
const GN={off:"公式予報",det:"単独モデル",con:"合成予報（複数モデルの平均）",ens:"アンサンブル平均",ai:"AIモデル（アンサンブル）",etc:"その他（取得元で検出したモデル）"};
const KT=0.514444;
const w10=(kt,is10)=>kt?kt*KT*(is10?1:.88):null;   // ATCFの1分間平均を気象庁の10分間平均相当(×0.88)
const kls=(kt,is10)=>{const v=w10(kt,is10);return v==null?UNKF:FCLS.find(c=>v>=c[0])};
const itxt=(kt,hp,is10)=>{const v=w10(kt,is10),k=kls(kt,is10);return [v!=null?`${v.toFixed(0)}m/s`:"",hp?`${Math.round(hp)}hPa`:"",v!=null?k[1]:""].filter(Boolean).join(" ")};
const tm=L.layerGroup(),ana=L.layerGroup(),anL=L.layerGroup();
let an=[],anSel=-1,anSeq=0,pt=null;
let items=[],d=null,ref=140,ready=false,active=false,met="w",fitFor="";
const opt={step:24,inlay:true,num:false};
const w=lo=>lo+360*Math.round((ref-lo)/360);   // 日付変更線をまたいでも線が飛ばないよう経度を連続化
// 予報の時刻は全て日本時間(JST)で表示する。初期時刻は YYYYMMDDHH（UTC）で届く
const iso=i=>Date.UTC(+i.slice(0,4),+i.slice(4,6)-1,+i.slice(6,8),+i.slice(8,10));
const J=ms=>{const x=new Date(ms+9*3600e3);return `${x.getUTCMonth()+1}/${x.getUTCDate()} ${String(x.getUTCHours()).padStart(2,"0")}時`};
const fmt=i=>i?J(iso(i)):"";                      // 初期時刻（日本時間）
const vt=(i,h)=>i?J(iso(i)+h*3600e3):"";          // 初期時刻のh時間後（日本時間）
const vd=(i,h)=>{if(!i)return"";const x=new Date(iso(i)+h*3600e3+9*3600e3);return `${x.getUTCMonth()+1}/${x.getUTCDate()}`};
const thh=t=>`<th>+${t}h${base?`<br><small>${vd(base,t)}</small>`:""}</th>`;
let base="";   // 基準の初期時刻（実況の時刻。無ければ最新の初期時刻）
const avg=(a,b)=>a&&b?(a+b)/2:(a||b||null);
const at=(p,t)=>{for(let i=0;i<p.length;i++){if(p[i][0]==t)return[p[i][1],w(p[i][2])];
  if(p[i][0]>t){if(!i)return null;const a=p[i-1],b=p[i],f=(t-a[0])/(b[0]-a[0]);return[a[1]+(b[1]-a[1])*f,w(a[2])+(w(b[2])-w(a[2]))*f]}}return null};
const atv=(p,t)=>{for(let i=0;i<p.length;i++){if(p[i][0]==t)return[p[i][3],p[i][4]];
  if(p[i][0]>t){if(!i)return[null,null];const a=p[i-1],b=p[i],f=(t-a[0])/(b[0]-a[0]),l=(x,y)=>x==null||y==null?null:x+(y-x)*f;return[l(a[3],b[3]),l(a[4],b[4])]}}return[null,null]};
const mrange=(it,t)=>{if(!it.members)return"";const v=it.members.map(m=>w10(atv(m,t)[0],false)).filter(x=>x!=null);
  return v.length<2?"":` ・ メンバー${Math.min(...v).toFixed(0)}〜${Math.max(...v).toFixed(0)}m/s（${v.length}本）`};
// 予報の印: 中の色と大きさ=強さ、外側の輪=モデル、形=円(単独)/菱形(アンサンブル平均)
function mk(lat,lon,c,kt,is10,o){const k=kls(kt,is10),s=k[3]+(o.cur?4:0),S=s+(o.cur?22:16);
  return L.marker([lat,lon],{icon:L.divIcon({className:"mkw",html:`<span class="mk${o.e?" e":""}${o.cur?" cur":""}" style="--s:${s}px;--f:${k[2]};--r:${c}"></span>`,iconSize:[S,S]}),interactive:!o.cur,keyboard:false})}
function draw(it){
  const g=it.g;g.clearLayers();
  const ll=it.pts.map(p=>[p[1],w(p[2])]);
  if(opt.inlay){   // 極細の外枠=モデルの色(片側1px)、内側の実線=その区間の強さの色
    const iw=it.big?6:5;
    L.polyline(ll,{color:it.c,weight:iw+2,opacity:1,lineCap:"round",lineJoin:"round",interactive:false}).addTo(g);
    for(let i=1;i<it.pts.length;i++)L.polyline([ll[i-1],ll[i]],{color:kls(avg(it.pts[i-1][3],it.pts[i][3]),it.is10)[2],weight:iw,opacity:1,lineCap:"round",interactive:false}).addTo(g);
  }else L.polyline(ll,{color:it.c,weight:it.big?4:3,dashArray:it.dash,opacity:.95,interactive:false}).addTo(g);
  it.pts.forEach(p=>{if(!p[0]||p[0]%opt.step)return;
    mk(p[1],w(p[2]),it.c,p[3],it.is10,{e:it.ens}).bindTooltip(`${esc(it.label)} +${p[0]}h（${vt(it.init,p[0])}） ${itxt(p[3],p[4],it.is10)}`).addTo(g);
    if(opt.num){const v=w10(p[3],it.is10);if(v!=null)L.marker([p[1],w(p[2])],{icon:L.divIcon({className:"mlab2",html:`<span>${Math.round(v)}</span>`,iconSize:[0,0]}),interactive:false,keyboard:false}).addTo(g)}});
}
function redraw(){items.forEach(draw);apply()}
function setNote(t){$("#fnote").textContent=t}
function build(){
  items.forEach(i=>{i.g.remove();i.mem&&i.mem.remove()});items=[];ana.clearLayers();
  ref=d.analysis?d.analysis.lon:140;
  base=(d.analysis&&d.analysis.init)||(d.jma&&d.jma.init)||"";
  const add=(label,init,pts,o={})=>{const it={label,init,pts,c:colr(label),g:L.layerGroup(),visible:true,is10:!!o.is10,ens:!!o.ens,dash:o.dash,big:o.big,grp:o.grp,tag:o.tag||""};items.push(it);return it};
  if(d.jma)add("JMA公式",d.jma.init,d.jma.pts,{big:1,is10:true,grp:"off"});
  d.models.forEach(m=>add(m.label,m.init,m.pts,{grp:m.grp||(m.label=="JTWC公式"?"off":"det")}));
  d.ens.forEach(e=>{const it=add(e.label,e.init,e.mean,{dash:"6 5",ens:true,grp:/\(AI\)$/.test(e.label)?"ai":"ens",tag:`${e.n}メンバー`});it.members=e.members;
    it.mem=L.layerGroup();e.members.forEach(m=>L.polyline(m.map(x=>[x[1],w(x[2])]),{color:it.c,weight:1,opacity:.3,interactive:false}).addTo(it.mem))});
  if(!base)base=items.map(i=>i.init).filter(Boolean).sort().pop()||"";
  items.forEach(draw);
  if(d.analysis)L.circleMarker([d.analysis.lat,w(d.analysis.lon)],{radius:6,color:"#fff",weight:2,fillColor:"#0a1120",fillOpacity:1}).bindTooltip("解析位置 "+fmt(d.analysis.init)+"（日本時間）").addTo(ana);
  $("#fchips").innerHTML=["off","det","con","ens","ai","etc"].map(g=>{const a=items.map((it,i)=>[it,i]).filter(([it])=>it.grp==g);
    return a.length?`<button type="button" class="gh" data-g="${g}"><span>${GN[g]}</span><span>まとめて切り替え</span></button><div class="srcs">${a.map(([it,i])=>`<button type="button" class="src" data-i="${i}" style="--c:${it.c}"><i class="ln${it.ens?" d":""}"></i>${esc(it.label)}<small>${fmt(it.init)}${it.tag?" "+it.tag:""}</small></button>`).join("")}</div>`:""}).join("");
  const mx=Math.min(240,Math.max(24,...items.map(i=>i.pts.length?i.pts[i.pts.length-1][0]:0)));
  $("#tau").max=mx;if(+$("#tau").value>mx)$("#tau").value=48;
  const have=items.map(i=>i.label),miss=["ECMWF","GFS","JMA-GSM","ECMWF-ENS","UKMET-ENS","JMA-GEPS","GEFS"].filter(x=>!have.includes(x)),u=new Date(d.updated),s=d.sources||{};
  $("#fsrc").textContent=`時刻はすべて日本時間（JST）。各ソース名の下の時刻は予報の初期時刻で、「+○h」はそこからの経過時間です。取得元: UCAR RAL（ATCF a-deck）${isNaN(u)?"":" 更新 "+u.toLocaleString("ja-JP",{timeZone:"Asia/Tokyo",month:"numeric",day:"numeric",hour:"2-digit",minute:"2-digit"})}`+
    (s.weatherlab_ok?" ＋ Google DeepMind Weather Lab（AI）":"")+"。"+(d.jma?"公式予報は気象庁。":"")+`アンサンブル計${s.members||0}メンバー。`+
    (miss.length?`この取得元に無いモデル: ${miss.join("・")}。`:"")+"風速は10分間平均相当（モデルは1分間平均×0.88）。予報は参考値です。防災には気象庁の情報を確認してください。";
  setNote(`${items.length}種類のソースを表示中`);
  apply();summary();loadAn();
  if(active&&fitFor!=d.id){fitFor=d.id;fit()}
}
function apply(){items.forEach(it=>{const on=active&&it.visible;on?it.g.addTo(map):it.g.remove();
  if(it.mem)(on&&$("#mem").checked)?it.mem.addTo(map):it.mem.remove()});
  document.querySelectorAll("#fchips .src").forEach(b=>b.classList.toggle("off",!items[+b.dataset.i].visible));anShow();marks()}
function marks(){tm.clearLayers();const t=+$("#tau").value,out=[];$("#tv").innerHTML=`+${t}h${base?`<small style="display:block;font-weight:400;font-size:11px;color:var(--sub)">${vt(base,t)}（日本時間）</small>`:""}`;
  items.forEach(it=>{if(!it.visible)return;const p=at(it.pts,t);if(!p)return;const q=atv(it.pts,t);
    mk(p[0],p[1],it.c,q[0],it.is10,{e:it.ens,cur:true}).addTo(tm);
    const k=kls(q[0],it.is10),v=w10(q[0],it.is10);
    out.push(`<div class="pr"><i class="rg${it.ens?" e":""}" style="--r:${it.c};--f:${k[2]}"></i><div class="pn"><b>${esc(it.label)}</b><small>${vt(it.init,t)} ・ ${p[0].toFixed(1)}N ${((p[1]%360+360)%360).toFixed(1)}E${mrange(it,t)}</small></div>
     <div class="iv"><strong style="color:${k[2]}">${v!=null?v.toFixed(0):"-"}</strong><small>m/s${v!=null?" "+k[1]:""}${q[1]?" "+Math.round(q[1])+"hPa":""}</small></div></div>`)});
  $("#pos").innerHTML=out.join("")||'<p class="note">この時間の予報はありません</p>'}
const pad=()=>mobile()?{paddingTopLeft:[16,60],paddingBottomRight:[16,$("aside").offsetHeight+16]}:{paddingTopLeft:[40,40],paddingBottomRight:[40,40]};
function fit(){const ll=[];items.forEach(it=>it.pts.forEach(p=>ll.push([p[1],w(p[2])])));if(d&&d.analysis)ll.push([d.analysis.lat,w(d.analysis.lon)]);
  if(ll.length)map.fitBounds(L.latLngBounds(ll),pad())}
function show(){active=true;ana.addTo(map);tm.addTo(map);if(!ready){ready=true;init()}else apply()}
function hide(){active=false;stopPlay();anL.remove();items.forEach(i=>{i.g.remove();i.mem&&i.mem.remove()});ana.remove();tm.remove()}
// 操作
$("#fchips").onclick=e=>{
  const g=e.target.closest(".gh");
  if(g){const a=items.filter(i=>i.grp==g.dataset.g),on=!a.some(i=>i.visible);a.forEach(i=>i.visible=on);apply();return}
  const b=e.target.closest(".src");if(!b)return;const it=items[+b.dataset.i];it.visible=!it.visible;apply()};
$("#mem").onchange=apply;$("#anOn").onchange=apply;$("#tau").oninput=()=>{stopPlay();marks()};
function stopPlay(){if(pt){clearInterval(pt);pt=null}$("#fplay").textContent="▶"}
$("#fplay").onclick=()=>{if(pt)return stopPlay();const s=$("#tau");if(+s.value>=+s.max)s.value=0;$("#fplay").textContent="⏸";marks();
  pt=setInterval(()=>{const n=+s.value+6;if(n>+s.max)return stopPlay();s.value=n;marks()},700)};
const tog=(id,k)=>$(id).onclick=e=>{opt[k]=!opt[k];e.currentTarget.classList.toggle("on",opt[k]);redraw()};
tog("#oInlay","inlay");tog("#oNum","num");
$("#oStep").onchange=e=>{opt.step=+e.target.value;redraw()};
// ばらつき表・強さ予報
$("#tbl").onclick=()=>{if(!d)return;const s=d.spread,dl=$("#dlg"),n=v=>v==null?"-":Math.round(v);
  dl.innerHTML=`<div style="display:flex;justify-content:space-between;align-items:center"><b>各時刻の位置のばらつき</b><button type="button" class="btn sm" data-close>閉じる</button></div>
   <p style="color:var(--sub);font-size:12px">各モデルの予報位置が、全モデルの平均位置から何km離れているか。大きいほどモデル間で意見が割れています。</p>
   <table><tr><th></th>${s.taus.map(thh).join("")}</tr>${s.rows.map(r=>`<tr><td>${esc(r[0])}</td>${r[1].map(v=>`<td>${n(v)}</td>`).join("")}</tr>`).join("")}
   <tr><td>平均からの最大</td>${s.max.map(v=>`<td><b>${n(v)}</b></td>`).join("")}</tr><tr><td>モデル数</td>${s.n.map(v=>`<td>${v}</td>`).join("")}</tr></table>`;dl.showModal()};
function chart(){
 const S=(d.intensity||{series:[]}).series,W=520,H=250,L0=42,R0=10,T0=10,B0=38,isW=met=="w",ix=isW?[1,2,3]:[4,5,6],vs=[];
 S.forEach(s=>s.rows.forEach(r=>ix.forEach(i=>{if(r[i]!=null)vs.push(r[i])})));
 if(!vs.length)return"<p style='color:var(--sub)'>この項目の予報値がありません</p>";
 let lo,hi;
 if(isW){lo=0;hi=Math.max(60,Math.ceil(Math.max(...vs)/10)*10)}else{lo=Math.floor((Math.min(...vs)-4)/10)*10;hi=Math.ceil((Math.max(...vs)+4)/10)*10}
 const tmax=Math.max(24,...S.map(s=>s.rows[s.rows.length-1][0])),
  X=t=>L0+(W-L0-R0)*t/tmax,Y=v=>isW?T0+(H-T0-B0)*(1-(v-lo)/(hi-lo)):T0+(H-T0-B0)*(v-lo)/(hi-lo),st=isW?10:20;
 let g="";
 for(let v=Math.ceil(lo/st)*st;v<=hi;v+=st)g+=`<line x1="${L0}" x2="${W-R0}" y1="${Y(v)}" y2="${Y(v)}" stroke="#22314f"/><text x="${L0-5}" y="${Y(v)+4}" fill="#8b9bbb" font-size="10" text-anchor="end">${v}</text>`;
 for(let t=0;t<=tmax;t+=24)g+=`<line x1="${X(t)}" x2="${X(t)}" y1="${T0}" y2="${H-B0}" stroke="#22314f"/><text x="${X(t)}" y="${H-(base?20:8)}" fill="#8b9bbb" font-size="10" text-anchor="middle">+${t}h</text>${base?`<text x="${X(t)}" y="${H-8}" fill="#8b9bbb" font-size="9" text-anchor="middle">${vd(base,t)}</text>`:""}`;
 if(isW)[[33,"強い"],[44,"非常に強い"],[54,"猛烈な"]].forEach(([v,n])=>{if(v<hi)g+=`<line x1="${L0}" x2="${W-R0}" y1="${Y(v)}" y2="${Y(v)}" stroke="#8b9bbb" stroke-dasharray="4 4"/><text x="${W-R0-2}" y="${Y(v)-3}" fill="#8b9bbb" font-size="10" text-anchor="end">${n}</text>`});
 S.forEach(s=>{const c=colr(s.label),rs=s.rows.filter(r=>r[ix[0]]!=null);if(!rs.length)return;
  if(s.kind=="ens"&&rs.length>1){const a=rs.map(r=>`${X(r[0])},${Y(r[ix[2]])}`),b=rs.map(r=>`${X(r[0])},${Y(r[ix[1]])}`).reverse();
   g+=`<polygon points="${a.concat(b).join(" ")}" fill="${c}" opacity=".16"/>`}
  g+=`<polyline points="${rs.map(r=>`${X(r[0])},${Y(r[ix[0]])}`).join(" ")}" fill="none" stroke="${c}" stroke-width="${s.kind=="ens"?2:2.5}"${s.kind=="ens"?' stroke-dasharray="6 4"':""}/>`});
 return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:auto">${g}</svg>`}
function openInt(){if(!d)return;const S=(d.intensity||{series:[]}).series,dl=$("#dlg"),fv=v=>v==null?"-":Math.round(v),T=[24,48,72,96,120],ens=S.filter(s=>s.kind=="ens");
 const legendH=S.map(s=>`<span class="src" style="--c:${colr(s.label)};min-height:26px;font-size:11px;cursor:default"><i class="ln${s.kind=="ens"?" d":""}"></i>${esc(s.label)}${s.kind=="ens"?`<small>${s.n}本</small>`:""}</span>`).join("");
 const cell=(s,t)=>{const r=s.rows.find(x=>x[0]==t);if(!r)return"<td>-</td>";
  return`<td><b>${fv(r[1])}</b><br><small style="color:var(--sub)">${s.kind=="ens"?`${fv(r[2])}〜${fv(r[3])}`:(r[4]?r[4]+"hPa":"")}</small></td>`};
 const pr=ens.length?`<p style="color:var(--sub);font-size:12px;margin-top:14px">各階級以上になるメンバーの割合（強い以上／非常に強い以上／猛烈な, %）</p>
  <table><tr><th></th>${T.map(thh).join("")}</tr>${ens.map(s=>`<tr><td>${esc(s.label)}</td>${T.map(t=>{const r=s.rows.find(x=>x[0]==t);return`<td>${r&&r[8]?r[8].join("/"):"-"}</td>`}).join("")}</tr>`).join("")}</table>`:"";
 dl.innerHTML=`<div style="display:flex;justify-content:space-between;align-items:center"><b>強さの予報</b><button type="button" class="btn sm" data-close>閉じる</button></div>
  <div class="btns" style="margin:8px 0"><button type="button" class="btn sm${met=="w"?" on":""}" data-met="w">風速(m/s)</button><button type="button" class="btn sm${met=="p"?" on":""}" data-met="p">中心気圧(hPa)</button></div>
  ${chart()}<div style="display:flex;flex-wrap:wrap;gap:4px;margin:6px 0">${legendH}</div>
  <p style="color:var(--sub);font-size:12px">実線=単独モデル、破線＋帯=アンサンブル平均と10〜90%範囲。横軸の下段は日付（日本時間）です。風速は10分間平均相当（モデルは1分間平均×0.88）。全球モデルは分解能の都合で猛烈な台風を弱めに予報しがちです。</p>
  <table><tr><th></th>${T.map(thh).join("")}</tr>${S.map(s=>`<tr><td style="color:${colr(s.label)}">${esc(s.label)}</td>${T.map(t=>cell(s,t)).join("")}</tr>`).join("")}</table>${pr}`;
 if(!dl.open)dl.showModal()}
$("#int").onclick=openInt;
$("#dlg").addEventListener("click",e=>{const dl=$("#dlg");
  if(e.target===dl||e.target.closest("[data-close]"))return dl.close();
  const b=e.target.closest("[data-met]");if(b){met=b.dataset.met;openInt()}});
// 見通しのまとめ・似た過去の台風
const refItem=()=>items.find(i=>i.label=="JMA公式")||items.find(i=>i.ens&&i.pts.length)||items.find(i=>i.pts.length)||null;
const km=(a,b)=>{const r=Math.PI/180,x=Math.sin((b[0]-a[0])*r/2)**2+Math.cos(a[0]*r)*Math.cos(b[0]*r)*Math.sin((b[1]-a[1])*r/2)**2;return 12742*Math.asin(Math.sqrt(x))};
const brg=(a,b)=>{const r=Math.PI/180,dl=(b[1]-a[1])*r,y=Math.sin(dl)*Math.cos(b[0]*r),x=Math.cos(a[0]*r)*Math.sin(b[0]*r)-Math.sin(a[0]*r)*Math.cos(b[0]*r)*Math.cos(dl);return(Math.atan2(y,x)/r+360)%360};
const dirj=a=>["北","北東","東","南東","南","南西","西","北西"][Math.round(a/45)%8];
const r10=v=>Math.round(v/10)*10;
function summary(){const el=$("#fsum"),r=refItem();if(!r||!d){el.hidden=true;return}
  const p0=at(r.pts,0)||[r.pts[0][1],w(r.pts[0][2])],T=[72,48,24].find(t=>at(r.pts,t)),bt=t=>base?`（${vt(base,t)}）`:"";
  let h=`<b>${esc(r.label)}の見通し</b>`;
  if(T){const p=at(r.pts,T);h+=`<div>${T}時間後${bt(T)}は${dirj(brg(p0,p))}へ約${r10(km(p0,p))}km（${p[0].toFixed(1)}N ${((p[1]%360+360)%360).toFixed(1)}E）</div>`}
  let pk=null;r.pts.forEach(p=>{const v=w10(p[3],r.is10);if(v!=null&&(!pk||v>pk[0]))pk=[v,p[0],p[3]]});
  if(pk)h+=`<div>ピーク ${Math.round(pk[0])}m/s（${kls(pk[2],r.is10)[1]}）・${pk[1]?`+${pk[1]}h${bt(pk[1])}`:"現在"}</div>`;
  const fo=$("#fsel").selectedOptions[0];
  if(fo&&fo.dataset.jma){const zk=+fo.dataset.szk||null,z=szJ(zk,fo.dataset.szl);
    h+=`<div>大きさ: <b>${z?z[1]:zk?"大型未満":"不明"}</b>${zk?`（強風域の最大半径 約${r10(zk)}km・気象庁の現況）`:""}${z?szTag(zk,fo.dataset.szl):""}　<small style="color:var(--sub)">モデルの予報には大きさの予測は含まれません</small></div>`}
  const sp=d.spread,i=sp.taus.indexOf(72),m=sp.max[i];
  if(m!=null&&sp.n[i]>=2)h+=`<div>72時間後のモデル間のばらつき: <b>${m<150?"小（ほぼ一致）":m<300?"中":"大（意見が割れています）"}</b>（最大約${r10(m)}km・${sp.n[i]}ソース）</div>`;
  el.innerHTML=h;el.hidden=false}
async function loadAn(){const my=++anSeq,r=refItem();an=[];anSel=-1;anL.clearLayers();
  if(!d||!r){$("#an").innerHTML="";return}
  $("#an").innerHTML='<p class="note">似た台風を探しています…</p>';
  const a0=d.analysis?[d.analysis.lat,d.analysis.lon]:[r.pts[0][1],r.pts[0][2]],wd=d.analysis&&d.analysis.w?w10(d.analysis.w,false):w10(r.pts[0][3],r.is10)||0;
  const t0=base?iso(base):Date.now(),doy=Math.floor((t0-Date.UTC(new Date(t0).getUTCFullYear(),0,0))/864e5);
  const fc=[24,48,72].map(t=>{const p=at(r.pts,t);return p?`${t}:${p[0].toFixed(1)}:${p[1].toFixed(1)}`:""}).filter(Boolean).join(",");
  try{const x=await fetch(`/api/analogs?lat=${a0[0]}&lon=${a0[1]}&doy=${doy}&w=${(wd||0).toFixed(0)}&fc=${fc}&n=5`);if(!x.ok)throw 0;const j=await x.json();if(my!=anSeq)return;an=j;anList();drawAn()}
  catch(e){if(my==anSeq)$("#an").innerHTML='<p class="note">似た台風を取得できませんでした</p>'}}
const wc=v=>v==null?UNKF[2]:(FCLS.find(c=>v>=c[0])||FCLS[4])[2];
function anChart(a,rs){  // 当時の強さの推移（細線=当時、破線=今回の予報）。起点=0h
  const X=h=>(h+48)/120*200,mx=Math.max(60,...a.wi.map(x=>x[1]||0),...rs.map(x=>x[1])),Y=v=>(44-v/mx*40).toFixed(1);
  const g=[33,44,54].filter(v=>v<mx).map(v=>`<line x1="0" x2="200" y1="${Y(v)}" y2="${Y(v)}" stroke="${wc(v)}" stroke-opacity=".35" stroke-width="1" vector-effect="non-scaling-stroke"/>`).join("");
  let seg="";for(let i=1;i<a.wi.length;i++){const p=a.wi[i-1],q=a.wi[i];if(p[1]==null||q[1]==null)continue;
    seg+=`<line x1="${X(p[0]).toFixed(1)}" y1="${Y(p[1])}" x2="${X(q[0]).toFixed(1)}" y2="${Y(q[1])}" stroke="${wc(q[1])}" stroke-width="3" stroke-linecap="round" vector-effect="non-scaling-stroke"/>`}
  const fl=rs.length>1?`<polyline points="${rs.map(x=>X(x[0]).toFixed(1)+","+Y(x[1])).join(" ")}" fill="none" stroke="#fff" stroke-width="1.5" stroke-dasharray="4 3" vector-effect="non-scaling-stroke"/>`:"";
  return `<svg viewBox="0 0 200 48" preserveAspectRatio="none" style="width:100%;height:46px;margin-top:4px;display:block"><line x1="${X(0)}" x2="${X(0)}" y1="0" y2="48" stroke="#8b9bbb" stroke-width="1" vector-effect="non-scaling-stroke"/>${g}${seg}${fl}</svg>
   <div class="note" style="display:flex;justify-content:space-between"><span>-48h</span><span>起点</span><span>+72h</span></div>`}
function anList(){
  if(!an.length){$("#an").innerHTML='<p class="note">近い動きをした過去の台風は見つかりませんでした</p>';return}
  const r=refItem(),rs=r?r.pts.filter(p=>p[0]<=72&&p[3]).map(p=>[p[0],w10(p[3],r.is10)]):[];
  $("#an").innerHTML=an.map((a,i)=>{const f=a.fut.find(x=>x[0]==72)||a.fut[a.fut.length-1],p=[a.lat,a.lon];
    const fu=f?`その後${f[0]}h: ${dirj(brg(p,[f[1],f[2]]))}へ約${r10(km(p,[f[1],f[2]]))}km`:"";
    const after=a.wi.filter(x=>x[0]>=0&&x[1]!=null),pk=after.reduce((m,x)=>!m||x[1]>m[1]?x:m,null);
    const it=`起点 ${a.w??"-"}m/s${pk?` → その後のピーク ${pk[1]}m/s（${cls(pk[1])}・+${pk[0]}h）`:""}`;
    return `<button type="button" class="anr${i==anSel?" on":""}" data-i="${i}"><b>${esc(a.title)}</b>
     <small>${a.time.slice(0,4)}年${+a.time.slice(5,7)}/${+a.time.slice(8,10)}時点 ${a.lat}N ${a.lon}E ・ 位置差${a.d0}km${a.err!=null?` ・ 進路差${a.err}km`:""}</small>
     <small>${it}</small><small>${fu}${fu?" ・ ":""}生涯の最大 ${a.max_wind??"-"}m/s${a.min_pres?` ${Math.round(a.min_pres)}hPa`:""}</small>${anChart(a,rs)}</button>`}).join("")+
    '<div class="note" style="margin-top:6px">現在位置・予報の動き・季節・強さが近い過去の台風です。グラフの細い線＝当時の風速、白い破線＝今回の予報（'+esc(r?r.label:"")+'）。同じ経過をたどるとは限らない参考表示です（気象庁ベストトラック準拠）。</div>'}
function anLL(a){const sh=360*Math.round((ref-a.lon)/360);let pv=null;return a.track.map(([la,lo])=>{lo+=sh;if(pv!==null){while(lo-pv>180)lo-=360;while(lo-pv<-180)lo+=360}pv=lo;return[la,lo]})}
function anShow(){(active&&$("#anOn").checked)?anL.addTo(map):anL.remove()}
function drawAn(){anL.clearLayers();an.forEach((a,i)=>{const on=i==anSel,ll=anLL(a),tip=`${a.title}（${a.time.slice(0,4)}年）`,sh=360*Math.round((ref-a.lon)/360),go=()=>selAn(i);
  for(let k=1;k<ll.length;k++){const aft=k>a.mi,v=a.track[k][2];  // 線の色=当時の強さ。起点より前は細く薄く、その後は太く
    L.polyline([ll[k-1],ll[k]],{color:wc(v),weight:aft?(on?5:3):(on?2.5:1.5),opacity:aft?(on?1:.8):(on?.6:.3),lineCap:"round"}).bindTooltip(esc(tip)+(v!=null?` ${v}m/s`:"")).on("click",go).addTo(anL)}
  L.circleMarker([a.lat,a.lon+sh],{radius:on?7:5,color:"#fff",weight:2,fillColor:wc(a.w),fillOpacity:1}).bindTooltip(esc(tip)+" 比較の起点"+(a.w!=null?` ${a.w}m/s`:"")).on("click",go).addTo(anL)})}
function selAn(i){anSel=i;$("#anOn").checked=true;anShow();anList();drawAn();const a=an[i];if(a)map.fitBounds(L.latLngBounds(anLL(a)),pad())}
$("#an").onclick=e=>{const b=e.target.closest(".anr");if(b)selAn(+b.dataset.i)};
// 読み込み
async function load(){const o=$("#fsel").selectedOptions[0];if(!o)return;setNote("読み込み中…");
  try{const r=await fetch(`/api/forecast/${o.value}?jma=${o.dataset.jma||""}`);if(!r.ok)throw 0;d=await r.json();build()}
  catch(e){setNote("予報データを取得できませんでした。少し待ってから「更新」を押してください")}}
async function init(){setNote("台風の一覧を取得中…");
  try{const r=await fetch("/api/forecast/storms");if(!r.ok)throw 0;const s=await r.json(),keep=$("#fsel").value;
    $("#fsel").innerHTML=s.map(x=>`<option value="${esc(x.id)}" data-jma="${esc(x.jma?x.jma.tc:"")}" data-szk="${x.jma&&x.jma.size&&x.jma.size.km||""}" data-szl="${esc(x.jma&&x.jma.size&&x.jma.size.label||"")}">${esc(x.label)}${x.jma?` ／ 台風${+x.jma.num.slice(2)}号`:""}${x.jma&&x.jma.size&&szJ(x.jma.size.km,x.jma.size.label)?` ／ ${szJ(x.jma.size.km,x.jma.size.label)[1]}`:""}</option>`).join("");
    if(!s.length){setNote("現在、活動中の西太平洋の台風はありません");return}
    if(keep&&s.some(x=>x.id==keep))$("#fsel").value=keep;load()}
  catch(e){setNote("台風の一覧を取得できませんでした。「更新」で再試行してください")}}
$("#fsel").onchange=load;$("#frf").onclick=init;
return{show,hide,fit};
})();

// ================= 実況 =================
// 台風の実況・進路予報（気象庁 防災情報）／アメダスの観測値／警報・注意報。台風が発生していなくても、観測と警報は見られる
const LV=(()=>{
const COLS=["#3d4f66","#4fc3f7","#a3e635","#ffd24a","#ff8a3d","#ff4d7d","#d946ef","#a855f7"];
const D16=["静穏","北北東","北東","東北東","東","東南東","南東","南南東","南","南南西","南西","西南西","西","西北西","北西","北北西","北"];
const DN=[null,"北東","東","南東","南","南西","西","北西","北"];
const RN=(lim)=>lim.map((v,i)=>i?`${v}〜`:`〜${lim[1]}`);
// i=観測データの列番号。lo=小さいほど強い（気圧）。all=0やゼロ付近も描く
const MET={
 wind:{n:"風速",s:"最大風速",u:"m/s",i:4,all:1,lim:[0,5,10,15,20,25,30,40]},
 gust:{n:"最大瞬間風速",s:"最大瞬間風速",u:"m/s",i:6,all:1,lim:[0,10,15,20,25,30,40,50]},
 pres:{n:"気圧（海面）",s:"最低気圧",u:"hPa",i:7,all:1,lo:1,lim:[-9999,-1010,-1000,-990,-980,-970,-960,-950],lb:["1010超","〜1010","〜1000","〜990","〜980","〜970","〜960","〜950"]},
 r1:{n:"1時間雨量",s:"1時間雨量",u:"mm",i:8,lim:[0,1,5,10,20,30,50,80]},
 r3:{n:"3時間雨量",s:"3時間雨量",u:"mm",i:9,lim:[0,10,30,50,100,150,200,300]},
 r24:{n:"24時間雨量",s:"24時間雨量",u:"mm",i:10,lim:[0,10,50,100,200,300,400,500]}};
const REL=new Set(["03","04","05","07","08","09","10","15","16","18","19","29","33","34","35","37","38","39","43","44","45","47","48","49"]);   // 台風に関係する種類（暴風・大雨・氾濫・土砂災害・波浪・高潮・強風）
// 警報コード → 段階（1=注意報 2=警報 3=危険警報 4=特別警報）。サーバーの w_level と同じ
const wlv=c=>{const n=+c;return n>=32&&n<=39?4:n>=42&&n<=49?3:n>=2&&n<=9?2:1};
// 塗りの色（薄く）。段階: [色, 塗りの濃さ]
const WNC=[null,["#fbbf24",.2],["#ef4444",.28],["#a855f7",.34],["#f1f5f9",.5]];
const trkL=L.layerGroup(),radL=L.layerGroup(),obsL=L.layerGroup(),wnL=L.layerGroup();
map.createPane("lvwn").style.zIndex=410;map.getPane("lvwn").style.pointerEvents="none";
const cvw=L.canvas({pane:"lvwn",padding:.3});
map.createPane("lvobs").style.zIndex=420;map.createPane("lvtk").style.zIndex=430;
const cv=L.canvas({pane:"lvobs",padding:.3});
let active=false,ready=false,S=[],sel=-1,obs=null,wrn=null,met="wind",timer=null,seq=0,fitted=false,loadedAt=0,mk={};
const vis={trk:true,rad:true,obs:true,wrn:true};
const cur=()=>sel<0?S:(S[sel]?[S[sel]]:[]);   // 表示中の台風（sel=-1 は すべて）
const fv=v=>v==null?"-":Number.isInteger(v)?String(v):v.toFixed(1);
const T=iso=>{if(!iso)return"-";const d=new Date(new Date(iso).getTime()+9*3600e3),z=n=>String(n).padStart(2,"0");return `${d.getUTCMonth()+1}/${d.getUTCDate()} ${z(d.getUTCHours())}:${z(d.getUTCMinutes())}`};
const DH=iso=>{if(!iso)return"-";const d=new Date(new Date(iso).getTime()+9*3600e3);return `${d.getUTCMonth()+1}/${d.getUTCDate()} ${d.getUTCHours()}時`};
const nm=en=>en?en.charAt(0)+en.slice(1).toLowerCase():"";
const pd=()=>mobile()?{paddingTopLeft:[16,60],paddingBottomRight:[16,$("aside").offsetHeight+16]}:{paddingTopLeft:[40,40],paddingBottomRight:[40,40]};
async function jget(u){const r=await fetch(u);if(!r.ok)throw new Error(r.status);return r.json()}
const xv=(v,m)=>m.lo?-v:v;
const bin=(v,m)=>{const x=xv(v,m);let b=0;for(let i=0;i<m.lim.length;i++)if(x>=m.lim[i])b=i;return b};

// ---- 台風 ----
function rt(r,o){if(!r||!r[o+1])return"なし";const d=r[o],lg=Math.round(r[o+1]),sh=Math.round(r[o+2]||r[o+1]);
  return d>=1&&d<=8&&sh<lg?`${DN[d]}側${lg}km（その他${sh}km）`:`${lg}km`}
function nowHTML(){const c=cur();if(!c.length)return `<p class="note">現在、発生中の台風はありません（気象庁が台風情報を発表している間だけ表示されます）。観測値と警報・注意報は下に表示します。</p>`;
  return c.map(nowOne).join("")}
function nowOne(s){
  const n=s.now,lb=n.lb||{},w=n.wind==null?null:Math.round(n.wind),
    kind=[lb.category,lb.intensity].filter(Boolean).join(" ")||(w!=null?cls(w):""),
    zg=n.r&&n.r[4]?n.r[4]:null,z=szJ(zg,n.sz);
  const t=(a,b,c,x)=>`<div${x?` class="${x}"`:""}><small>${a}</small><strong>${b}${c?`<span>${c}</span>`:""}</strong></div>`;
  return `<div class="kpi">
   <div class="w2"><small>${T(n.t)} 現在（日本時間）</small><strong style="color:${w!=null?col(w):"var(--ink)"}">台風${s.no}号 ${esc(nm(s.en))}</strong><span style="display:block;color:var(--sub);font-size:12px">${esc(kind)}${z?szTag(zg,n.sz):""}</span></div>
   ${t("中心位置",`${n.lat.toFixed(1)}°N ${n.lon.toFixed(1)}°E`,"")}
   ${t("中心気圧",n.pres?Math.round(n.pres):"-","hPa")}
   ${t("最大風速（中心付近）",w!=null?w:"-","m/s")}
   ${t("最大瞬間風速",n.gust!=null?Math.round(n.gust):"-","m/s")}
   ${t("進行方向・速度",esc(n.course||"-"),n.speed!=null?` ${Math.round(n.speed)}km/h`:"")}
   <div class="w2"><small>大きさ（気象庁の階級。無い時は強風域の最大半径で判定）</small><strong style="font-size:14px">${z?z[1]:zg?"大型未満":"不明"}${zg?`（強風域 最大${Math.round(zg)}km）`:""}</strong></div>
   <div class="w2"><small>暴風域（風速25m/s以上）</small><strong style="font-size:14px">${rt(n.r,0)}</strong></div>
   <div class="w2"><small>強風域（風速15m/s以上）</small><strong style="font-size:14px">${rt(n.r,3)}</strong></div></div>`}
function fcHTML(){const c=cur();if(!c.length)return`<p class="note">台風が発生していません</p>`;
  return c.map(s=>(c.length>1?`<h4 class="lvh">台風${s.no}号 ${esc(nm(s.en))}</h4>`:"")+fcOne(s,S.indexOf(s))).join("")+`<div class="note" style="margin-top:6px">気圧 hPa／風速・瞬間風速 m/s／予報円・暴風警戒域の半径 km。行をタップすると地図がその位置へ移ります。</div>`}
function fcOne(s,si){
  if(!s.fc.length)return`<p class="note">この台風の進路予報はまだ発表されていません</p>`;
  return `<table class="tb"><tr><th>日時</th><th>中心位置</th><th>気圧</th><th>風速</th><th>予報円</th></tr>`+
   s.fc.map((p,i)=>`<tr data-si="${si}" data-fi="${i}" style="cursor:pointer"><td>${DH(p.t)}<br><small>+${p.h}時間</small></td><td>${p.lat.toFixed(1)}N<br>${p.lon.toFixed(1)}E</td><td>${p.pres?Math.round(p.pres):"-"}</td>
    <td>${p.wind!=null?Math.round(p.wind):"-"}${p.gust!=null?`<br><small>瞬間${Math.round(p.gust)}</small>`:""}</td>
    <td>${p.circle?Math.round(p.circle):"-"}${p.storm?`<br><small>警戒${Math.round(p.storm)}</small>`:""}</td></tr>`).join("")+
   `</table>`}
// 予報円どうしを外接線でつないだ帯（強風域・暴風域の帯と同じ作り）
// 外形線は塗りと別の折れ線で描く。多角形に枠線を付けると、画面端で切られた所に偽の直線（経線・緯線のような線）が出るため
function ringsOf(g){const o=[];(function f(x){if(!x.length)return;if(typeof x[0][0]==="number")o.push(x.concat([x[0]]));else x.forEach(f)})(g);return o}
function outline(g,o){return L.polyline(ringsOf(g),Object.assign({pane:"lvtk",interactive:false,lineJoin:"round"},o))}
// 予報円どうしを外接線でつないだ帯（強風域・暴風域の帯と同じ作り）。[塗り, 外形線] を返す
function band(cs,fill,line){if(cs.length<2)return null;
  const P=cs.map(x=>wzCirc(x.c,x.r,6).map(wzM)),polys=P.slice(1).map((g,j)=>[wzHull(P[j].concat(g))]);let g=null;
  try{if(window.polygonClipping)g=polygonClipping.union(...polys).map(pg=>pg.map(rg=>rg.map(wzU)))}catch(e){g=null}
  if(!g)g=polys.map(pg=>[(wzArea(pg[0])<0?pg[0].slice().reverse():pg[0]).map(wzU)]);
  const r=[L.polygon(g,Object.assign({pane:"lvtk",interactive:false,stroke:false},fill))];
  if(line&&window.polygonClipping)r.push(outline(g,line));
  return r}
// 日付変更線をまたぐ軌跡で、地図を横切る長い直線にならないよう、経度を連続させる
function unwrapLL(a){const o=[];let k=0;a.forEach((p,i)=>{if(i){const d=p[1]+k-o[i-1][1];if(d>180)k-=360;else if(d<-180)k+=360}o.push([p[0],p[1]+k])});return o}
function drawStorm(){trkL.clearLayers();radL.clearLayers();cur().forEach(drawOne)}
function drawOne(s){
  const n=s.now,c0=[n.lat,n.lon],F=s.fc.filter(p=>p.lat!=null);
  if(s.past.length>1)L.polyline(unwrapLL(s.past.concat([c0])),{pane:"lvtk",color:"#cbd5e1",weight:2.5,dashArray:"2 6",opacity:.9,interactive:false}).addTo(trkL);
  const b1=band([{c:c0,r:1}].concat(F.filter(p=>p.circle).map(p=>({c:[p.lat,p.lon],r:p.circle}))),{fillColor:"#fff",fillOpacity:.1});if(b1)b1.forEach(l=>l.addTo(trkL));
  // 暴風域・暴風警戒域: 実況→予報の順に並べ、半径のある点が続く区間ごとに帯にする（途中から発生・途中で消える場合に対応）。
  // 1点だけの区間は円で描く。半径の無い点をまたいでつながない
  {const seq=[{c:c0,r:(n.r&&n.r[1])||0}].concat(F.map(p=>({c:[p.lat,p.lon],r:p.storm||0}))),runs=[];let run=[];
   seq.forEach(x=>{if(x.r>0)run.push(x);else{if(run.length)runs.push(run);run=[]}});if(run.length)runs.push(run);
   runs.forEach(rn=>{
     if(rn.length>1){const b=band(rn,{fillColor:"#ef4444",fillOpacity:.14},{color:"#fb7185",weight:1.5});if(b)b.forEach(l=>l.addTo(trkL))}
     else if(rn[0].c!==c0)L.circle(rn[0].c,{pane:"lvtk",radius:rn[0].r*1000,color:"#fb7185",weight:1.5,fillColor:"#ef4444",fillOpacity:.14,interactive:false}).addTo(trkL)})}
  L.polyline(unwrapLL([c0].concat(F.map(p=>[p.lat,p.lon]))),{pane:"lvtk",color:"#fff",weight:2,opacity:.95,interactive:false}).addTo(trkL);
  F.forEach(p=>{
    if(p.circle&&p.circle<=3000)L.circle([p.lat,p.lon],{pane:"lvtk",radius:p.circle*1000,color:"#fff",weight:2,opacity:.95,fill:false,interactive:false}).addTo(trkL);
    L.circleMarker([p.lat,p.lon],{pane:"lvtk",radius:4,color:"#0a1120",weight:1,fillColor:p.wind!=null?col(Math.round(p.wind)):"#fff",fillOpacity:1})
     .bindTooltip(`${cur().length>1?`台風${s.no}号 `:""}+${p.h}時間（${DH(p.t)}）<br>${p.pres?Math.round(p.pres)+"hPa ":""}${p.wind!=null?Math.round(p.wind)+"m/s":""}${p.circle?`<br>予報円 半径${Math.round(p.circle)}km`:""}`).addTo(trkL);
    L.marker([p.lat,p.lon],{pane:"lvtk",icon:L.divIcon({className:"mlab2",html:`<span>${DH(p.t)}</span>`,iconSize:[0,0]}),interactive:false,keyboard:false}).addTo(trkL)});
  const w=n.wind==null?null:Math.round(n.wind);
  L.marker(c0,{pane:"lvtk",icon:L.divIcon({className:"mkw",html:`<span class="mk cur" style="--s:18px;--f:${w!=null?col(w):"#fff"};--r:#fff"></span>`,iconSize:[34,34]}),keyboard:false})
   .bindTooltip(`台風${s.no}号 ${esc(nm(s.en))}<br>${T(n.t)}現在 ${n.pres?Math.round(n.pres)+"hPa ":""}${w!=null?w+"m/s":""}`).addTo(trkL);
  if(cur().length>1)L.marker(c0,{pane:"lvtk",icon:L.divIcon({className:"mlab2",html:`<span>台風${s.no}号</span>`,iconSize:[0,0]}),interactive:false,keyboard:false}).addTo(trkL);
  if(n.r)["r30","r50"].forEach(k=>{const g=wzShape(n.lat,n.lon,n.r,k);
    if(g){L.polygon(g,{pane:"wz",stroke:false,fillColor:WZC[k],fillOpacity:.16,interactive:false}).addTo(radL);
      L.polyline(g.concat([g[0]]),{pane:"wz",color:WZC[k],weight:2.5,dashArray:"7 5",interactive:false}).addTo(radL)}});
}
// ---- 観測 ----
function tip(r){return `<b>${esc(r[1])}</b>`+[r[4]!=null?`風 ${fv(r[4])}m/s${r[5]!=null?"（"+D16[r[5]]+"）":""}`:"",r[6]!=null?`最大瞬間 ${fv(r[6])}m/s`:"",
  r[7]!=null?`海面気圧 ${fv(r[7])}hPa`:"",r[8]!=null?`1時間雨量 ${fv(r[8])}mm`:"",r[9]!=null?`3時間雨量 ${fv(r[9])}mm`:"",r[10]!=null?`24時間雨量 ${fv(r[10])}mm`:""].filter(Boolean).map(x=>"<br>"+x).join("")}
// 範囲: 台風が発生中で距離を選んだ時だけ、台風の中心からその距離内の地点に絞る（0=全国）
// 観測値の範囲「自動」: 強風域の最大半径（無ければ強さから推定）に、強いほど大きい係数をかけて、近い段階(km)へ切り上げる。超えたら上限の1500km
const RSTEP=[300,500,700,1000,1500];
function autoR0(s){const n=s.now,w=n.wind==null?null:Math.round(n.wind),g=n.r&&n.r[4]?n.r[4]:null,
  base=g??(w==null||w<33?300:w<44?350:w<54?400:450),fac=w==null||w<33?1.2:w<44?1.3:w<54?1.4:1.5,need=Math.max(300,base*fac);
  return RSTEP.find(x=>x>=need)||RSTEP[RSTEP.length-1]}   // 台風の強さ・大きさだけで決めた範囲（自動は最大でも1500km。全国にはしない）
// 自動の範囲は台風の強さ・大きさだけで決める（警報・注意報の発表状況では変えない）
const autoR=autoR0;
const rv=s=>{const v=$("#lvRng").value;return v=="auto"?autoR(s):+v};
const rngAll=()=>{const r=cur().map(s=>[s,rv(s)]);return r.some(x=>!x[1])?[]:r};   // [[台風, 半径km]]。空なら全国
const rng=()=>{const r=rngAll();return r.length?Math.max(...r.map(x=>x[1])):0};
function rngInfo(){const c=cur(),s=c[0],A=$("#lvRngA"),N=$("#lvRngN");
  if(!s){A.textContent="自動（台風の強さ・大きさに合わせる）";N.textContent="";return}
  if(c.length>1){A.textContent=`自動（${c.map(t=>{const r=autoR(t);return `${t.no}号 ${r?r+"km":"全国"}`}).join("・")}）`;N.textContent=$("#lvRng").value=="auto"?"各台風の強さ・大きさから範囲を決め、どれかの台風の範囲に入る地点の最大値を見ます。":"";return}
  const R=autoR(s),n=s.now,w=n.wind==null?null:Math.round(n.wind),g=n.r&&n.r[4]?n.r[4]:null,z=szJ(g,n.sz);
  A.textContent=`自動（${R?`台風の中心から${R}km以内`:"全国"}）`;
  N.textContent=$("#lvRng").value=="auto"?`強さ「${w!=null?cls(w):"不明"}」・大きさ「${z?z[1]:g?"大型未満":"不明"}」${g?`（強風域 最大${Math.round(g)}km）`:""}から、初期の範囲を${R?R+"km以内":"全国"}にしています。強いほど、大きいほど広く見ます。`:""}
function rowsOf(m){if(!obs)return[];const rs=rngAll();
  return obs.rows.filter(r=>r[m.i]!=null&&(m.all||r[m.i]>0)&&(!rs.length||rs.some(([s,R])=>km([s.now.lat,s.now.lon],[r[2],r[3]])<=R))).sort((a,b)=>xv(b[m.i],m)-xv(a[m.i],m))}   // 強い順
function drawObs(){
  obsL.clearLayers();mk={};if(!obs)return;const m=MET[met];
  rngAll().forEach(([s,R])=>L.circle([s.now.lat,s.now.lon],{pane:"lvobs",radius:R*1000,color:"#94a3b8",weight:1.2,dashArray:"4 6",fill:false,interactive:false}).addTo(obsL));
  rowsOf(m).slice().reverse().forEach(r=>{const b=bin(r[m.i],m),c=L.circleMarker([r[2],r[3]],{renderer:cv,radius:3+b*1.3,color:"#0a1120",weight:1,fillColor:COLS[b],fillOpacity:.92}).bindTooltip(tip(r),{direction:"top"});
    c.addTo(obsL);mk[r[0]]=c})}
function metHTML(){
  rngInfo();const R=rng(),multi=rngAll().length>1;$("#lvObsT").textContent=R?(multi?`観測（アメダス）— 各台風の中心から${$("#lvRng").value=="auto"?"自動の範囲":R+"km"}以内の最大値`:`観測（アメダス）— 台風の中心から${R}km以内の最大値`):"観測（アメダス）— 全国の最大値";$("#lvRng").disabled=!cur().length;
  if(!obs)return`<p class="note" style="grid-column:span 2">観測値を取得できませんでした。「更新」で再試行してください</p>`;
  return Object.entries(MET).map(([k,m])=>{const r=rowsOf(m)[0];
    return `<button type="button" class="mt${k==met?" on":""}" data-m="${k}"><small>${m.s}</small><strong>${r?fv(r[m.i]):"-"}<span>${r?m.u:""}</span></strong><em>${r?esc(r[1]):"観測なし"}</em></button>`}).join("")}
function topHTML(){
  const m=MET[met],R=rowsOf(m).slice(0,10);$("#lvTopT").textContent=`${m.n}（${m.lo?"低い":"高い"}順 上位10地点）`;
  if(!R.length)return`<li style="cursor:default"><span>観測値がありません</span></li>`;
  return R.map((r,i)=>`<li data-st="${r[0]}"><span>${i+1}. ${esc(r[1])}${met=="wind"&&r[5]!=null?`<small style="color:var(--sub)"> ${D16[r[5]]}</small>`:""}</span><b style="color:${COLS[bin(r[m.i],m)]}">${fv(r[m.i])}${m.u}</b></li>`).join("")}
// ---- 警報・注意報 ----
function warnHTML(){
  if(!wrn){$("#lvWsum").innerHTML="";return`<p class="note">警報・注意報を取得できませんでした。「更新」で再試行してください</p>`}
  const all=$("#lvAll").checked,
    rows=wrn.areas.map(o=>{const w=o.w.filter(x=>all||REL.has(x.c));return w.length?{...o,w,lv:Math.max(...w.map(x=>x.lv))}:null}).filter(Boolean).sort((a,b)=>b.lv-a.lv||b.n-a.n||(a.code<b.code?-1:1));
  const cnt=l=>rows.filter(o=>o.lv==l).length;
  $("#lvWsum").innerHTML=[[4,"特別警報"],[3,"危険警報"],[2,"警報"],[1,"注意報"]].map(([l,t])=>`<span class="pill"><b>${cnt(l)}</b> ${t}（地域数）</span>`).join("");
  if(!rows.length)return`<p class="note">現在、発表中の${all?"":"台風に関係する"}警報・注意報はありません</p>`;
  return rows.map(o=>`<div class="wr l${o.lv}"><b>${esc(o.name)}</b>${o.w.map(x=>`<em class="wb l${x.lv}">${esc(x.name)}</em>`).join("")}<small>${o.n?o.n+"市町村等":""}</small></div>`).join("")}
// ---- 描画まとめ ----
function renderStorm(){$("#lvNow").innerHTML=nowHTML();$("#lvFc").innerHTML=fcHTML();drawStorm();apply()}
function renderObs(){$("#lvMet").innerHTML=metHTML();$("#lvTop").innerHTML=topHTML();drawObs();apply();if(active)legend2()}
// ---- 警報・注意報の塗りつぶし（1次細分区域。拡大すると市町村等）----
const geo={c10:null,rel:null,c20:{}},geoP={};
const gj=n=>geoP[n]||(geoP[n]=jget("/api/live/geo/"+n).catch(e=>{delete geoP[n];throw e}));
let wnSeq=0;
function wnLevel(codes,all){let m=0;for(const c of codes||[])if(all||REL.has(c))m=Math.max(m,wlv(c));return m}
function wnLayer(data,tbl,all){
  return L.geoJSON(data,{filter:f=>wnLevel(tbl[(f.properties||{}).code],all)>0,style:f=>{
    const l=wnLevel(tbl[f.properties.code],all),[c,a]=WNC[l];
    return {pane:"lvwn",renderer:cvw,interactive:false,noClip:true,color:c,weight:f.properties.islandBold?5:.8,opacity:l==4?.9:.55,fillColor:c,fillOpacity:a,lineJoin:"round"}}})}
async function drawWarn(){
  const my=++wnSeq;wnL.clearLayers();if(!wrn||!wrn.c10)return;
  const all=$("#lvAll").checked;
  try{
    if(!geo.c10)geo.c10=await gj("class10s");
    if(my!=wnSeq)return;
    let use20=null;
    if(map.getZoom()>=8){   // 拡大時: 画面に入る範囲の市町村等の形を読み込む（読み込めるまでは1次細分区域のまま）
      if(!geo.rel)geo.rel=await gj("relm");
      if(my!=wnSeq)return;
      const b=map.getBounds(),need=geo.rel.map((r,i)=>b.intersects(L.latLngBounds(r.sw,r.ne))?i:-1).filter(i=>i>=0);
      await Promise.all(need.filter(i=>!geo.c20[i]).map(async i=>{geo.c20[i]=await gj("class20s_"+i)}));
      if(my!=wnSeq)return;
      use20=need;
    }
    wnL.clearLayers();
    if(use20)use20.forEach(i=>wnLayer(geo.c20[i],wrn.c20||{},all).addTo(wnL));
    else wnLayer(geo.c10,wrn.c10,all).addTo(wnL);
  }catch(e){if(my==wnSeq)wnL.clearLayers()}}
let wnT=null;
map.on("moveend",()=>{if(!active||!vis.wrn)return;clearTimeout(wnT);wnT=setTimeout(drawWarn,250)});
function renderWarn(){$("#lvWarn").innerHTML=warnHTML();drawWarn()}
function apply(){[[trkL,vis.trk],[radL,vis.rad],[obsL,vis.obs],[wnL,vis.wrn]].forEach(([g,on])=>(active&&on)?g.addTo(map):g.remove())}
function legendHTML(){const m=MET[met],lb=m.lb||RN(m.lim);
  return `<div class="lgh">${m.n}（${m.u}）</div>`+COLS.map((c,i)=>`<div><i class="dot" style="--s:${6+i*1.3}px;background:${c}"></i>${lb[i]}</div>`).join("")+
   `<div class="lgh">台風</div><div><i style="background:${WZC.r50}"></i>暴風域</div><div><i style="background:${WZC.r30}"></i>強風域</div>`+szLg()+
   (vis.wrn?`<div class="lgh">警報・注意報（薄い塗り）</div>`+[[1,"注意報"],[2,"警報"],[3,"危険警報"],[4,"特別警報"]].map(([l,t])=>`<div><i style="background:${WNC[l][0]};opacity:${Math.min(1,WNC[l][1]*2.4)}"></i>${t}</div>`).join(""):"")}
function legend2(){if(tab=="lv")$("#legend").innerHTML=legendHTML()}
function setNote(t){$("#lnote").textContent=t}
function noteTimes(){const a=[];if(cur().length)a.push(`台風 ${[...new Set(cur().map(x=>T(x.now.t)))].join("・")}`);if(obs)a.push(`アメダス ${T(obs.t)}`);if(wrn&&wrn.t)a.push(`警報 ${T(wrn.t)}`);
  setNote(a.length?`${a.join(" ／ ")}（日本時間）`:"データを取得できませんでした。「更新」で再試行してください")}
function fit(){const ll=[];
  cur().forEach(s=>{ll.push([s.now.lat,s.now.lon]);s.past.forEach(p=>ll.push(p));s.fc.forEach(p=>{ll.push([p.lat,p.lon]);if(p.circle){const d=p.circle/111;ll.push([p.lat+d,p.lon+d],[p.lat-d,p.lon-d])}})})
  if(ll.length)map.fitBounds(L.latLngBounds(ll),pd());else map.fitBounds([[22,122],[46,148]],pd())}
async function loadAll(manual){
  const my=++seq;if(manual)setNote("更新中…");
  const [a,b,c]=await Promise.allSettled([jget("/api/live/storms"),jget("/api/live/amedas"),jget("/api/live/warnings")]);
  if(my!=seq)return;
  const keep=sel<0?"":((S[sel]&&S[sel].tc)||"");
  S=a.status=="fulfilled"?(a.value.storms||[]):[];sel=S.length>1?S.findIndex(s=>s.tc==keep):0;   // 複数ある時の既定は「すべて」(-1)
  $("#lsel").innerHTML=S.length?(S.length>1?`<option value="-1">すべての台風（${S.length}個）</option>`:"")+S.map((s,i)=>{const z=szJ(s.now.r&&s.now.r[4],s.now.sz);return `<option value="${i}">台風${s.no}号 ${esc(nm(s.en))}${z?`（${z[1]}）`:""}</option>`}).join(""):`<option>${a.status=="fulfilled"?"発生中の台風はありません":"台風情報を取得できません"}</option>`;
  $("#lsel").disabled=!S.length;$("#lsel").value=String(sel);
  obs=b.status=="fulfilled"?b.value:null;wrn=c.status=="fulfilled"?c.value:null;loadedAt=Date.now();
  renderStorm();renderObs();renderWarn();noteTimes();
  if(active&&!fitted){fitted=true;fit()}}
// ---- 操作 ----
const tg=(id,k)=>$(id).onclick=e=>{vis[k]=!vis[k];e.currentTarget.classList.toggle("on",vis[k]);apply();if(k=="wrn"){if(vis.wrn)drawWarn();legend2()}};
tg("#lvTrk","trk");tg("#lvRad","rad");tg("#lvObs","obs");tg("#lvWn","wrn");
$("#lsel").onchange=e=>{sel=+e.target.value;renderStorm();renderObs();noteTimes();fit()};
$("#lrf").onclick=()=>loadAll(true);
$("#lvAll").onchange=renderWarn;
$("#lvRng").onchange=()=>{renderObs();if(rng()&&active&&vis.obs){const b=L.latLngBounds([]);rngAll().forEach(([s,R])=>{const dl=R/111,dn=dl/Math.max(.2,Math.cos(s.now.lat*Math.PI/180));b.extend([[s.now.lat-dl,s.now.lon-dn],[s.now.lat+dl,s.now.lon+dn]])});if(b.isValid())map.fitBounds(b,pd())}};
$("#lvMet").onclick=e=>{const b=e.target.closest("[data-m]");if(!b)return;met=b.dataset.m;renderObs()};
$("#lvTop").onclick=e=>{const li=e.target.closest("[data-st]");if(!li)return;const c=mk[li.dataset.st];if(!c)return;
  vis.obs=true;$("#lvObs").classList.add("on");apply();map.setView(c.getLatLng(),Math.max(map.getZoom(),8));c.openTooltip()};
$("#lvFc").onclick=e=>{const tr=e.target.closest("[data-fi]");if(!tr)return;const p=S[+tr.dataset.si].fc[+tr.dataset.fi];map.setView([p.lat,p.lon],Math.max(map.getZoom(),5))};
function show(){active=true;apply();
  if(!ready){ready=true;loadAll()}else if(Date.now()-loadedAt>4*60e3)loadAll();
  clearInterval(timer);timer=setInterval(()=>{if(active&&!document.hidden)loadAll()},5*60e3)}
function hide(){active=false;clearInterval(timer);apply()}
document.addEventListener("visibilitychange",()=>{if(active&&!document.hidden&&Date.now()-loadedAt>4*60e3)loadAll()});
return{show,hide,fit,legend:legendHTML};
})();

// ================= 起動 =================
$("#dlg").addEventListener("click",e=>{if(e.target.id=="dlg")e.target.close()});
if("serviceWorker" in navigator)addEventListener("load",()=>navigator.serviceWorker.register("/sw.js").catch(()=>{}));
legend();
setTab(initTab);
dbInit();
// 開いたままでも、サーバー側で速報が更新されたら一覧と表示中の台風を自動で更新する
async function poll(){
  try{
    const y=await (await fetch("/api/years")).json();
    if(y.jma_at&&y.jma_at!==jmaAt){
      jmaAt=y.jma_at;$("#upd").innerHTML=`速報 <b>${jst(jmaAt)}</b> 更新`;
      await load();if(curSid)show(curSid,null,true);
    }
  }catch(e){}
}
setInterval(poll,5*60*1000);
document.addEventListener("visibilitychange",()=>{if(!document.hidden)poll()});  // スマホでアプリに戻った時にも確認
</script></body></html>
"""

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "build":
        build()
    elif len(sys.argv) > 1 and sys.argv[1] == "diag":
        diag()
    elif len(sys.argv) > 1 and sys.argv[1] == "fdiag":
        fdiag()
    elif len(sys.argv) > 1 and sys.argv[1] == "featdiag":
        featdiag()
    else:
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
