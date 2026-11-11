"""
TechSeal - Pipeline de extracción de datos de YouTube (v3)
===========================================================

Extrae datos PÚBLICOS de una lista de canales con la YouTube Data API v3.

Genera 3 archivos:
    channels_raw.csv      una fila por canal (datos brutos de la API)
    videos_raw.csv        una fila por vídeo (datos brutos, para Power Query)
    channels_metrics.csv  métricas agregadas por canal (referencia / control
                          cruzado con lo que calcules en Power Query/DAX)

REQUISITOS:
    pip install google-api-python-client python-dotenv

CONFIGURACIÓN:
    1. Archivo .env en la misma carpeta con:  YOUTUBE_API_KEY=tu_api_key
    2. Archivo channels_input.csv con columnas:  handle,source
       (handle = @usuario o ID UC..., source = top_socialblade / api_discovery / manual)

USO:
    python techseal_data_pipeline.py
    python techseal_data_pipeline.py --input otra_lista.csv --videos 50

CUOTA (límite gratuito: 10.000 unidades/día):
    ~3-4 unidades por canal (resolver handle + canal + playlist + vídeos).
    ~8 unidades por canal con 150 vídeos.
    ~130 canales con 150 vídeos  ->  ~1.100 unidades. Sin problema.

CAMBIOS EN v3:
    - Descarga 150 vídeos por canal (antes 50): en tech de consumo ~50% de las
      subidas son Shorts, y con 50 vídeos muchos canales tenían <10 largos.
    - Métricas SEPARADAS por formato (prefijos long_ y short_), cada una sobre
      los 30 vídeos más recientes de ese formato. Desde 2025 YouTube cuenta las
      vistas de Shorts de otra forma, así que mezclarlas no tiene sentido.
    - enough_long_videos: True si hay >=10 vídeos largos (mínimo para puntuar).
    - es_title_ratio y consumer_title_ratio: control automático de idioma y
      de nicho a partir de los títulos (para detectar canales que no encajan).
    - days_since_last_long_video: detecta canales que solo sobreviven a base
      de Shorts (posible señal de estancamiento en formato largo).

CAMBIOS EN v2:
    - Lee los canales desde un CSV (no hay que editar el código).
    - 50 vídeos por canal en vez de 20 (mismo coste de cuota, más fiabilidad).
    - Excluye de las métricas los vídeos con menos de 7 días (aún no han
      acumulado vistas y hunden la media).
    - Separa Shorts y vídeos largos: las métricas de rendimiento se calculan
      solo sobre vídeos largos (los Shorts tienen otra lógica de vistas).
    - CORREGIDO: coeficiente de variación de vistas (antes no existía / salía vacío).
    - CORREGIDO: el CSV usaba las columnas de la PRIMERA fila; si a un canal le
      faltaba una métrica, el resto se perdían o fallaba la escritura.
    - Suscriptores ocultos -> vacío (no 0), para no dividir por cero ni sesgar.
    - views_per_subscriber se calcula con vistas RECIENTES (mediana últimos
      vídeos / suscriptores), no con el histórico total, que premia a canales
      viejos con mucho catálogo.
    - Guarda topicCategories de la API (clasificación tech objetiva).
    - Reanudable: si se corta, no vuelve a pedir los canales ya procesados.
"""

import argparse
import csv
import os
import re
import statistics
from datetime import datetime, timezone

from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

MIN_VIDEO_AGE_DAYS = 7          # vídeos más nuevos no cuentan para métricas
SHORT_MAX_SECONDS = 180         # desde oct-2024 los Shorts pueden durar hasta 3 min
AFFILIATE_PATTERNS = re.compile(
    r"amzn\.to|amazon\.[a-z.]+/.*tag=|aliexpress|s\.click\.aliexpress|bit\.ly|"
    r"geni\.us|tidd\.ly|awin|linktr\.ee|codigo|código|descuento|coupon",
    re.IGNORECASE,
)

CHANNEL_RAW_FIELDS = [
    "input_handle", "source", "channel_id", "title", "custom_url", "country",
    "channel_published_at", "subscribers", "hidden_subscribers", "total_views",
    "video_count", "topic_categories", "is_tech_topic", "extracted_at",
]
VIDEO_RAW_FIELDS = [
    "channel_id", "video_id", "title", "published_at", "age_days",
    "duration_seconds", "is_short", "is_live", "views", "likes", "comments",
    "likes_hidden", "comments_disabled", "tags_count", "title_length",
    "description_length", "has_affiliate_link",
]
METRIC_FIELDS = [
    "channel_id",
    "title",
    "source",
    "country",
    "subscribers",
    "tier",
    "video_count",
    "channel_age_days",
    "is_tech_topic",
    "es_title_ratio",
    "consumer_title_ratio",
    "n_videos_sampled",
    "enough_long_videos",
    "long_n_videos",
    "long_avg_comment_rate",
    "long_avg_like_rate",
    "long_avg_engagement_rate",
    "long_median_views",
    "long_views_cv",
    "long_avg_views_per_day",
    "long_views_per_subscriber",
    "short_n_videos",
    "short_avg_comment_rate",
    "short_avg_like_rate",
    "short_avg_engagement_rate",
    "short_median_views",
    "short_views_cv",
    "short_avg_views_per_day",
    "short_views_per_subscriber",
    "lifetime_views_per_subscriber",
    "avg_days_between_uploads",
    "stdev_days_between_uploads",
    "days_since_last_upload",
    "days_since_last_long_video",
    "shorts_ratio",
    "live_videos_ratio",
    "avg_tags_count",
    "avg_description_length",
    "avg_title_length",
    "stdev_title_length",
    "affiliate_link_ratio",
]


# ----------------------------------------------------------------- utilidades
def parse_iso_duration(d):
    """PT1H2M3S -> segundos."""
    m = re.fullmatch(r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", d or "")
    if not m:
        return None
    days, h, mi, s = (int(x) if x else 0 for x in m.groups())
    return days * 86400 + h * 3600 + mi * 60 + s


def parse_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def safe_mean(xs):
    xs = [x for x in xs if x is not None]
    return statistics.mean(xs) if xs else None


def safe_stdev(xs):
    xs = [x for x in xs if x is not None]
    return statistics.stdev(xs) if len(xs) >= 2 else None


def rnd(x, n=4):
    return round(x, n) if isinstance(x, (int, float)) else x


def tier_from_subs(subs):
    if subs is None:
        return "desconocido"
    if subs < 10_000:
        return "pequeño"
    if subs < 100_000:
        return "micro"
    if subs < 390_000:
        return "mid"
    return "macro"


# ----------------------------------------------------------------- API
def get_service():
    if not API_KEY:
        raise RuntimeError("No se encontró YOUTUBE_API_KEY. Revisa tu archivo .env")
    return build("youtube", "v3", developerKey=API_KEY)


def resolve_channel_id(yt, handle_or_id):
    h = handle_or_id.strip()
    if h.startswith("UC") and len(h) == 24:
        return h
    resp = yt.channels().list(part="id", forHandle=h.lstrip("@")).execute()
    items = resp.get("items", [])
    if not items:
        raise ValueError(f"No se pudo resolver el canal: {handle_or_id}")
    return items[0]["id"]


def get_channel(yt, channel_id):
    resp = yt.channels().list(
        part="snippet,statistics,contentDetails,topicDetails", id=channel_id
    ).execute()
    it = resp["items"][0]
    st = it["statistics"]
    hidden = st.get("hiddenSubscriberCount", False)
    topics = it.get("topicDetails", {}).get("topicCategories", [])
    topics_clean = [t.rsplit("/", 1)[-1] for t in topics]
    return {
        "channel_id": channel_id,
        "title": it["snippet"]["title"],
        "custom_url": it["snippet"].get("customUrl", ""),
        "country": it["snippet"].get("country", ""),
        "channel_published_at": it["snippet"]["publishedAt"],
        "subscribers": None if hidden else int(st.get("subscriberCount", 0)),
        "hidden_subscribers": hidden,
        "total_views": int(st.get("viewCount", 0)),
        "video_count": int(st.get("videoCount", 0)),
        "topic_categories": "|".join(topics_clean),
        "is_tech_topic": "Technology" in topics_clean,
        "_uploads": it["contentDetails"]["relatedPlaylists"]["uploads"],
    }


def get_recent_video_ids(yt, playlist_id, n):
    ids, token = [], None
    while len(ids) < n:
        resp = yt.playlistItems().list(
            part="contentDetails", playlistId=playlist_id,
            maxResults=min(50, n - len(ids)), pageToken=token,
        ).execute()
        ids += [i["contentDetails"]["videoId"] for i in resp.get("items", [])]
        token = resp.get("nextPageToken")
        if not token:
            break
    return ids[:n]


def get_videos(yt, ids, channel_id, now):
    out = []
    for i in range(0, len(ids), 50):
        resp = yt.videos().list(
            part="snippet,statistics,contentDetails,liveStreamingDetails",
            id=",".join(ids[i:i + 50]),
        ).execute()
        for it in resp.get("items", []):
            sn, st, cd = it["snippet"], it.get("statistics", {}), it["contentDetails"]
            pub = parse_dt(sn["publishedAt"])
            dur = parse_iso_duration(cd.get("duration"))
            desc = sn.get("description", "") or ""
            out.append({
                "channel_id": channel_id,
                "video_id": it["id"],
                "title": sn["title"],
                "published_at": sn["publishedAt"],
                "age_days": rnd((now - pub).total_seconds() / 86400, 2),
                "duration_seconds": dur,
                # Aproximación: la API no marca los Shorts. Duración <= 3 min.
                "is_short": dur is not None and dur <= SHORT_MAX_SECONDS,
                "is_live": "liveStreamingDetails" in it,
                "views": int(st["viewCount"]) if "viewCount" in st else None,
                "likes": int(st["likeCount"]) if "likeCount" in st else None,
                "comments": int(st["commentCount"]) if "commentCount" in st else None,
                "likes_hidden": "likeCount" not in st,
                "comments_disabled": "commentCount" not in st,
                "tags_count": len(sn.get("tags", [])),
                "title_length": len(sn["title"]),
                "description_length": len(desc),
                "has_affiliate_link": bool(AFFILIATE_PATTERNS.search(desc)),
            })
    return out


# ----------------------------------------------------------------- métricas
ES_WORDS = set("de la el en y que los las del un una por con para es se lo mi mejor nuevo nueva "
               "este esta cómo como qué precio análisis móvil movil reseña vale pena así más ya al su sus".split())
EN_WORDS = set("the and of to in for with is on my best this new how it you your worth after "
               "should buy what why from".split())
CONSUMER_RE = re.compile(
    r"m[óo]vil|celular|smartphone|tel[ée]fono|iphone|samsung|galaxy|xiaomi|redmi|poco|pixel|oppo|"
    r"realme|honor|huawei|oneplus|motorola|tablet|ipad|auricular|aud[íi]fono|airpods|buds|smartwatch|"
    r"reloj|watch|power ?bank|bater[íi]a|cargador|port[áa]til|macbook|monitor|altavoz|parlante|"
    r"c[áa]mara|gopro|insta360|dji|dron|aspirador|robot|router|wifi|tv|televisor|proyector|teclado|"
    r"rat[óo]n|gadget|unboxing|apple|android|consola|ps5|switch|xbox|gpu|rtx|ssd|kindle|patinete|"
    r"alexa|dom[óo]tica|enchufe|nas|micr[óo]fono|review|an[áa]lisis",
    re.IGNORECASE,
)
METRIC_WINDOW = 30   # nº máx. de vídeos recientes por formato usados para métricas
MIN_LONG_FOR_SCORE = 10


def spanish_ratio(titles):
    es = en = 0
    for t in titles:
        words = re.findall(r"[a-záéíóúñü]+", t.lower())
        es += sum(w in ES_WORDS for w in words)
        en += sum(w in EN_WORDS for w in words)
    return es / (es + en) if (es + en) else None


def perf_metrics(vids, subs, prefix):
    """Métricas de rendimiento para UN formato (largos o Shorts)."""
    vids = vids[-METRIC_WINDOW:]                       # los N más recientes
    views = [v["views"] for v in vids]
    cr = [v["comments"] / v["views"] for v in vids if v["views"] and v["comments"] is not None]
    lr = [v["likes"] / v["views"] for v in vids if v["views"] and v["likes"] is not None]
    er = [(v["likes"] + (v["comments"] or 0)) / v["views"]
          for v in vids if v["views"] and v["likes"] is not None]
    vpd = [v["views"] / v["age_days"] for v in vids if v["age_days"] > 0]
    mean_v, sd_v = safe_mean(views), safe_stdev(views)
    med = statistics.median(views) if views else None
    pct = lambda xs: rnd(safe_mean(xs) * 100) if safe_mean(xs) is not None else None
    return {
        f"{prefix}_n_videos": len(vids),
        f"{prefix}_avg_comment_rate": pct(cr),
        f"{prefix}_avg_like_rate": pct(lr),
        f"{prefix}_avg_engagement_rate": pct(er),
        f"{prefix}_median_views": med,
        f"{prefix}_views_cv": rnd(sd_v / mean_v) if (sd_v is not None and mean_v) else None,
        f"{prefix}_avg_views_per_day": rnd(safe_mean(vpd), 2),
        f"{prefix}_views_per_subscriber": rnd(med / subs) if (med is not None and subs) else None,
    }


def compute_metrics(ch, videos, source, now):
    subs = ch["subscribers"]
    all_v = sorted(videos, key=lambda v: v["published_at"])
    mature = [v for v in all_v if v["age_days"] >= MIN_VIDEO_AGE_DAYS and v["views"] is not None]
    long_v = [v for v in mature if not v["is_short"]]
    short_v = [v for v in mature if v["is_short"]]

    # Cadencia y estilo: últimos 50 vídeos de cualquier formato (es actividad del canal)
    recent = all_v[-50:]
    dates = [parse_dt(v["published_at"]) for v in recent]
    gaps = [(dates[i + 1] - dates[i]).total_seconds() / 86400 for i in range(len(dates) - 1)]
    last_upload = max(dates) if dates else None
    last_long = max((parse_dt(v["published_at"]) for v in all_v if not v["is_short"]), default=None)
    titles = [v["title"] for v in recent]
    tl = [len(t) for t in titles]
    n = len(recent) or None

    return {
        "channel_id": ch["channel_id"],
        "title": ch["title"],
        "source": source,
        "country": ch["country"],
        "subscribers": subs,
        "tier": tier_from_subs(subs),
        "video_count": ch["video_count"],
        "channel_age_days": (now - parse_dt(ch["channel_published_at"])).days,
        "is_tech_topic": ch["is_tech_topic"],
        "es_title_ratio": rnd(spanish_ratio(titles), 3),
        "consumer_title_ratio": rnd(sum(bool(CONSUMER_RE.search(t)) for t in titles) / n, 3) if n else None,
        "n_videos_sampled": len(all_v),
        "enough_long_videos": len(long_v) >= MIN_LONG_FOR_SCORE,
        **perf_metrics(long_v, subs, "long"),
        **perf_metrics(short_v, subs, "short"),
        "lifetime_views_per_subscriber": rnd(ch["total_views"] / subs, 2) if subs else None,
        "avg_days_between_uploads": rnd(safe_mean(gaps), 2),
        "stdev_days_between_uploads": rnd(safe_stdev(gaps), 2),
        "days_since_last_upload": rnd((now - last_upload).total_seconds() / 86400, 1) if last_upload else None,
        "days_since_last_long_video": rnd((now - last_long).total_seconds() / 86400, 1) if last_long else None,
        "shorts_ratio": rnd(sum(v["is_short"] for v in recent) / n) if n else None,
        "live_videos_ratio": rnd(sum(v["is_live"] for v in recent) / n) if n else None,
        "avg_tags_count": rnd(safe_mean([v["tags_count"] for v in recent]), 2),
        "avg_description_length": rnd(safe_mean([v["description_length"] for v in recent]), 1),
        "avg_title_length": rnd(safe_mean(tl), 1),
        "stdev_title_length": rnd(safe_stdev(tl), 2),
        "affiliate_link_ratio": rnd(sum(v["has_affiliate_link"] for v in recent) / n) if n else None,
    }


# ----------------------------------------------------------------- E/S
def read_input(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return [(r["handle"].strip(), (r.get("source") or "").strip()) for r in rows if r.get("handle")]


def already_done(path):
    if not os.path.exists(path):
        return set()
    with open(path, newline="", encoding="utf-8") as f:
        return {r["input_handle"] for r in csv.DictReader(f)}


def append_rows(path, fields, rows):
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="channels_input.csv")
    ap.add_argument("--videos", type=int, default=150)
    ap.add_argument("--outdir", default=".")
    args = ap.parse_args()

    p_ch = os.path.join(args.outdir, "channels_raw.csv")
    p_vid = os.path.join(args.outdir, "videos_raw.csv")
    p_met = os.path.join(args.outdir, "channels_metrics.csv")
    p_err = os.path.join(args.outdir, "errors.csv")

    yt = get_service()
    todo = read_input(args.input)
    done = already_done(p_ch)
    print(f"{len(todo)} canales en la lista, {len(done)} ya procesados.")

    now = datetime.now(timezone.utc)
    for i, (handle, source) in enumerate(todo, 1):
        if handle in done:
            continue
        print(f"[{i}/{len(todo)}] {handle} ...", end=" ", flush=True)
        try:
            cid = resolve_channel_id(yt, handle)
            ch = get_channel(yt, cid)
            ids = get_recent_video_ids(yt, ch["_uploads"], args.videos)
            vids = get_videos(yt, ids, cid, now)
            met = compute_metrics(ch, vids, source, now)

            ch_row = {**ch, "input_handle": handle, "source": source,
                      "extracted_at": now.isoformat()}
            append_rows(p_vid, VIDEO_RAW_FIELDS, vids)
            append_rows(p_met, METRIC_FIELDS, [met])
            append_rows(p_ch, CHANNEL_RAW_FIELDS, [ch_row])   # el último: marca "hecho"
            print(f"OK ({len(vids)} vídeos, tier {met['tier']})")
        except Exception as e:
            print(f"ERROR: {e}")
            append_rows(p_err, ["handle", "error"], [{"handle": handle, "error": str(e)}])

    print("\nListo. Archivos en", os.path.abspath(args.outdir))


if __name__ == "__main__":
    main()
