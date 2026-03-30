import os
import json
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for
from apscheduler.schedulers.background import BackgroundScheduler
from models import init_db, get_db
from scrapers import sync_drazby_sk, sync_datahub_ov
from scraper_playwright import sync_drazby_playwright, sync_ov_playwright, DISTRICT_TO_REGION, _region_from_district

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "drazby-dev-key-change-in-prod")


@app.context_processor
def inject_globals():
    return {"now_str": datetime.now().strftime("%Y-%m-%d")}

# Initialize database
init_db()

# Scheduler for daily sync
scheduler = BackgroundScheduler()
scheduler.add_job(sync_drazby_sk, "cron", hour=6, minute=0, id="sync_drazby",
                  misfire_grace_time=3600, replace_existing=True)
scheduler.add_job(sync_datahub_ov, "cron", hour=6, minute=30, id="sync_ov",
                  misfire_grace_time=3600, replace_existing=True)
# Playwright-based scraping runs at 7:00 to get more comprehensive data
scheduler.add_job(sync_drazby_playwright, "cron", hour=7, minute=0, id="sync_playwright_drazby",
                  misfire_grace_time=3600, replace_existing=True)
scheduler.add_job(sync_ov_playwright, "cron", hour=7, minute=30, id="sync_playwright_ov",
                  misfire_grace_time=3600, replace_existing=True)


REGIONS = [
    ("BA", "Bratislavský"), ("TT", "Trnavský"), ("TN", "Trenčiansky"),
    ("NR", "Nitriansky"), ("ZA", "Žilinský"), ("BB", "Banskobystrický"),
    ("PO", "Prešovský"), ("KE", "Košický")
]

SUBJECT_TYPES = ["Byt", "Dom", "Rodinný dom", "Pozemok", "Nebytový priestor",
                  "Komerčný objekt", "Garáž", "Podnik", "Súbor vecí", "Neurčené", "Iný"]


@app.route("/")
def index():
    conn = get_db()
    try:
        # Get filter params
        subject_type = request.args.get("subject_type", "")
        region = request.args.get("region", "")
        district = request.args.get("district", "")
        price_min = request.args.get("price_min", "")
        price_max = request.args.get("price_max", "")
        status = request.args.get("status", "")
        source = request.args.get("source", "")
        keyword = request.args.get("keyword", "")
        sort = request.args.get("sort", "auction_date")
        order = request.args.get("order", "asc")
        page = int(request.args.get("page", 1))
        per_page = 50

        # Build query
        where = ["1=1"]
        params = []

        if subject_type:
            where.append("subject_type = ?")
            params.append(subject_type)
        if region:
            where.append("region = ?")
            params.append(region)
        if district:
            where.append("district LIKE ?")
            params.append(f"%{district}%")
        if price_min:
            where.append("price >= ?")
            params.append(float(price_min))
        if price_max:
            where.append("price <= ?")
            params.append(float(price_max))
        if status:
            where.append("status = ?")
            params.append(status)
        if source:
            where.append("source = ?")
            params.append(source)
        if keyword:
            where.append("(title LIKE ? OR description LIKE ? OR city LIKE ?)")
            kw = f"%{keyword}%"
            params.extend([kw, kw, kw])

        where_clause = " AND ".join(where)

        # Allowed sort columns
        allowed_sorts = {"auction_date", "price", "created_at", "subject_type", "region", "city"}
        if sort not in allowed_sorts:
            sort = "auction_date"
        order_dir = "DESC" if order == "desc" else "ASC"

        # Count
        count = conn.execute(
            f"SELECT COUNT(*) FROM auctions WHERE {where_clause}", params
        ).fetchone()[0]

        # Fetch
        offset = (page - 1) * per_page
        auctions = conn.execute(
            f"""SELECT * FROM auctions WHERE {where_clause}
                ORDER BY {sort} {order_dir}
                LIMIT ? OFFSET ?""",
            params + [per_page, offset]
        ).fetchall()

        total_pages = max(1, (count + per_page - 1) // per_page)

        # Stats
        stats = {
            "total": conn.execute("SELECT COUNT(*) FROM auctions").fetchone()[0],
            "planned": conn.execute("SELECT COUNT(*) FROM auctions WHERE status='planned'").fetchone()[0],
            "current": conn.execute("SELECT COUNT(*) FROM auctions WHERE status='current'").fetchone()[0],
        }

        # Last sync
        last_sync = conn.execute("""
            SELECT source, finished_at, records_new, status
            FROM sync_log ORDER BY id DESC LIMIT 5
        """).fetchall()

        # Districts for selected region
        districts = []
        if region:
            districts = [r[0] for r in conn.execute(
                "SELECT DISTINCT district FROM auctions WHERE region=? AND district!='' ORDER BY district",
                (region,)
            ).fetchall()]

        return render_template("index.html",
            auctions=auctions, count=count, page=page, total_pages=total_pages,
            stats=stats, last_sync=last_sync, regions=REGIONS,
            subject_types=SUBJECT_TYPES, districts=districts,
            filters={
                "subject_type": subject_type, "region": region,
                "district": district, "price_min": price_min,
                "price_max": price_max, "status": status,
                "source": source, "keyword": keyword,
                "sort": sort, "order": order
            }
        )
    finally:
        conn.close()


@app.route("/auction/<auction_id>")
def auction_detail(auction_id):
    conn = get_db()
    try:
        auction = conn.execute("SELECT * FROM auctions WHERE id=?", (auction_id,)).fetchone()
        if not auction:
            return "Dražba nenájdená", 404

        raw = {}
        if auction["raw_data"]:
            try:
                raw = json.loads(auction["raw_data"])
            except json.JSONDecodeError:
                pass

        return render_template("detail.html", auction=auction, raw=raw)
    finally:
        conn.close()


_sync_running = False

@app.route("/sync", methods=["POST"])
def manual_sync():
    global _sync_running
    if _sync_running:
        return redirect(url_for("index"))
    _sync_running = True
    source = request.form.get("source", "all")
    try:
        if source in ("all", "drazby"):
            sync_drazby_sk()
        if source in ("all", "ov"):
            sync_datahub_ov()
        if source in ("all", "playwright_drazby", "playwright"):
            try:
                sync_drazby_playwright()
            except Exception as e:
                logger.error(f"Playwright drazby sync error: {e}")
        if source in ("all", "playwright_ov", "playwright"):
            try:
                sync_ov_playwright()
            except Exception as e:
                logger.error(f"Playwright OV sync error: {e}")
    except Exception as e:
        logger.error(f"Sync error: {e}")
    finally:
        _sync_running = False
    return redirect(url_for("index"))


@app.route("/cleanup", methods=["POST"])
def cleanup_data():
    """Re-process existing records: fill missing regions from districts, normalize names, remove junk."""
    conn = get_db()
    try:
        fixed = 0

        # 1. Fill missing region from district using the mapping
        rows = conn.execute(
            "SELECT id, district, region FROM auctions WHERE district != '' AND (region = '' OR region IS NULL)"
        ).fetchall()
        for row in rows:
            region = _region_from_district(row["district"])
            if region:
                conn.execute("UPDATE auctions SET region=?, updated_at=datetime('now') WHERE id=?",
                             (region, row["id"]))
                fixed += 1

        # 2. Normalize region names (fix "X kraj" → canonical names)
        region_fixes = {
            "Trenčiansky kraj": "Trenčiansky", "Nitriansky kraj": "Nitriansky",
            "Košický kraj": "Košický", "Prešovský kraj": "Prešovský",
            "Bratislavský kraj": "Bratislavský", "Trnavský kraj": "Trnavský",
            "Žilinský kraj": "Žilinský", "Banskobystrický kraj": "Banskobystrický",
        }
        for wrong, correct in region_fixes.items():
            result = conn.execute("UPDATE auctions SET region=? WHERE region=?", (correct, wrong))
            fixed += result.rowcount

        # 3. Clean up city/district: trim whitespace, remove trailing garbage
        rows = conn.execute(
            "SELECT id, city, district FROM auctions WHERE city LIKE '% ' OR city LIKE ' %' OR district LIKE '% ' OR district LIKE ' %'"
        ).fetchall()
        for row in rows:
            city = (row["city"] or "").strip()
            district = (row["district"] or "").strip()
            conn.execute("UPDATE auctions SET city=?, district=? WHERE id=?",
                         (city, district, row["id"]))
            fixed += 1

        # 4. Remove non-real-estate entries (pohľadávky that slipped through)
        result = conn.execute("""
            DELETE FROM auctions WHERE
            (description LIKE '%pohľadávk%' OR description LIKE '%peňažn%' OR title LIKE '%pohľadávk%')
            AND description NOT LIKE '%nehnuteľnost%'
            AND description NOT LIKE '%pozemok%'
            AND description NOT LIKE '%parcela%'
            AND description NOT LIKE '%dom%'
            AND description NOT LIKE '%byt%'
            AND description NOT LIKE '%stavba%'
        """)
        removed = result.rowcount

        conn.commit()
        logger.info(f"Data cleanup: fixed={fixed}, removed={removed}")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")
    finally:
        conn.close()
    return redirect(url_for("index"))


@app.route("/filters", methods=["GET", "POST"])
def manage_filters():
    conn = get_db()
    try:
        if request.method == "POST":
            conn.execute("""
                INSERT INTO filters (name, subject_types, regions, districts,
                    price_min, price_max, keywords)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                request.form.get("name", "Nový filter"),
                request.form.get("subject_types", ""),
                request.form.get("regions", ""),
                request.form.get("districts", ""),
                request.form.get("price_min") or None,
                request.form.get("price_max") or None,
                request.form.get("keywords", ""),
            ))
            conn.commit()
            return redirect(url_for("manage_filters"))

        filters = conn.execute("SELECT * FROM filters ORDER BY created_at DESC").fetchall()
        return render_template("filters.html", filters=filters, regions=REGIONS,
                             subject_types=SUBJECT_TYPES)
    finally:
        conn.close()


@app.route("/filters/<int:filter_id>/delete", methods=["POST"])
def delete_filter(filter_id):
    conn = get_db()
    try:
        conn.execute("DELETE FROM filters WHERE id=?", (filter_id,))
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for("manage_filters"))


@app.route("/api/stats")
def api_stats():
    conn = get_db()
    try:
        stats = {
            "total": conn.execute("SELECT COUNT(*) FROM auctions").fetchone()[0],
            "by_type": dict(conn.execute(
                "SELECT subject_type, COUNT(*) FROM auctions GROUP BY subject_type"
            ).fetchall()),
            "by_region": dict(conn.execute(
                "SELECT region, COUNT(*) FROM auctions WHERE region!='' GROUP BY region"
            ).fetchall()),
            "by_source": dict(conn.execute(
                "SELECT source, COUNT(*) FROM auctions GROUP BY source"
            ).fetchall()),
        }
        return jsonify(stats)
    finally:
        conn.close()


if __name__ == "__main__":
    scheduler.start()
    port = int(os.environ.get("PORT", 5555))
    debug = os.environ.get("RAILWAY_ENVIRONMENT") is None
    app.run(host="0.0.0.0", debug=debug, port=port, use_reloader=False)
