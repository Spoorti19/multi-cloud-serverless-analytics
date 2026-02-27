import os
from datetime import datetime, timezone
from flask import Flask, request, render_template_string
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

app = Flask(__name__)

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
# Recommended for public dataset:
#   GOOGLE_CLOUD_PROJECT = comm034-task2   (your billing/project)
#   BQ_DATASET          = bigquery-public-data.thelook_ecommerce   (public dataset)
#
# If your dataset is inside your own project:
#   BQ_DATASET = thelook_ecommerce
#
PROJECT_ID = (os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "").strip()
BQ_DATASET = os.getenv("BQ_DATASET", "bigquery-public-data.thelook_ecommerce").strip()

# Optional: set a region if needed (usually not required)
# BQ_LOCATION = os.getenv("BQ_LOCATION", "").strip()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def dataset_is_fully_qualified(ds: str) -> bool:
    # true if looks like "project.dataset"
    return "." in ds and len(ds.split(".")) == 2


def fq(table: str) -> str:
    """
    Fully-qualified table name with backticks.
    - If BQ_DATASET is "project.dataset" -> use that directly.
    - Else -> use PROJECT_ID.dataset (PROJECT_ID must be set for this case).
    """
    if dataset_is_fully_qualified(BQ_DATASET):
        return f"`{BQ_DATASET}.{table}`"

    # fallback: dataset only (needs project)
    if PROJECT_ID:
        return f"`{PROJECT_ID}.{BQ_DATASET}.{table}`"

    # last fallback (not ideal, but avoids crash)
    return f"`{BQ_DATASET}.{table}`"


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def run_bigquery(sql: str):
    """
    Runs query and returns (columns, rows).
    """
    client = bigquery.Client(project=PROJECT_ID or None)
    job = client.query(sql)

    results = job.result(timeout=60)
    columns = [field.name for field in results.schema]
    rows = [list(row.values()) for row in results]
    return columns, rows


# -----------------------------------------------------------------------------
# QUERIES (A/B/C)
# -----------------------------------------------------------------------------
QUERY_A = f"""
-- Query A: Top product categories by items sold
SELECT
  p.category AS product_category,
  COUNT(oi.id) AS items_sold,
  ROUND(SUM(oi.sale_price), 2) AS revenue
FROM {fq("order_items")} oi
JOIN {fq("products")} p
  ON oi.product_id = p.id
WHERE oi.status = 'Complete'
GROUP BY product_category
ORDER BY items_sold DESC
LIMIT 10;
"""

QUERY_B = f"""
-- Query B: Distribution centers ranked by avg delivery time (thelook public dataset)
SELECT
  dc.name AS distribution_center,
  ROUND(AVG(TIMESTAMP_DIFF(o.delivered_at, o.created_at, HOUR)), 2) AS avg_delivery_time_hours,
  COUNT(DISTINCT o.order_id) AS total_orders
FROM {fq("order_items")} oi
JOIN {fq("orders")} o
  ON oi.order_id = o.order_id
JOIN {fq("products")} p
  ON oi.product_id = p.id
JOIN {fq("distribution_centers")} dc
  ON p.distribution_center_id = dc.id
WHERE o.delivered_at IS NOT NULL
GROUP BY distribution_center
ORDER BY avg_delivery_time_hours DESC
LIMIT 10;
"""





QUERY_C = f"""
-- Query C: Order completion risk by product category (non-completion rate %)
SELECT
  p.category AS product_category,
  COUNT(oi.id) AS total_order_items,
  COUNTIF(o.delivered_at IS NULL) AS incomplete_orders,
  ROUND(
    COUNTIF(o.delivered_at IS NULL) / COUNT(oi.id) * 100,
    2
  ) AS non_completion_rate_percent
FROM {fq("order_items")} oi
JOIN {fq("orders")} o
  ON oi.order_id = o.order_id
JOIN {fq("products")} p
  ON oi.product_id = p.id
GROUP BY product_category
ORDER BY non_completion_rate_percent DESC;
"""


QUERIES = {
    "A": ("Top product categories by items sold", QUERY_A),
    "B": ("Distribution centers ranked by avg delivery time", QUERY_B),
    "C": ("Order completion risk by product category", QUERY_C),
}



# -----------------------------------------------------------------------------
# UI (Colorful + clean + responsive)
# -----------------------------------------------------------------------------
PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>B5: BigQuery Query Runner (Cloud Run)</title>

  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet">

  <style>
    :root{
      --c1:#0ea5e9;  /* sky */
      --c2:#8b5cf6;  /* violet */
      --c3:#22c55e;  /* green */
      --c4:#f97316;  /* orange */
      --c5:#ef4444;  /* red */
      --shadow: 0 14px 35px rgba(0,0,0,.08);
    }
    body{
      background:
        radial-gradient(1100px 600px at 18% 10%, rgba(14,165,233,.22), transparent 60%),
        radial-gradient(900px 520px at 85% 0%, rgba(139,92,246,.20), transparent 60%),
        radial-gradient(1000px 560px at 70% 95%, rgba(34,197,94,.16), transparent 60%),
        #f7f8fb;
      min-height: 100vh;
    }
    .hero{
      background: linear-gradient(110deg, var(--c1), var(--c2));
      color: #fff;
      border-radius: 22px;
      box-shadow: var(--shadow);
      overflow:hidden;
      position: relative;
    }
    .hero:after{
      content:"";
      position:absolute;
      right:-120px; top:-120px;
      width:320px; height:320px;
      border-radius:50%;
      background: rgba(255,255,255,.12);
      filter: blur(0px);
    }
    .pill{
      display:inline-flex;
      gap:.5rem;
      align-items:center;
      padding:.35rem .7rem;
      border-radius:999px;
      background: rgba(255,255,255,.18);
      border: 1px solid rgba(255,255,255,.20);
      font-weight: 600;
      font-size: .9rem;
    }
    code.badge{
      background: #111827;
      color: #fff;
      font-weight: 700;
      border-radius: 999px;
      padding: .35rem .6rem;
    }
    .qcard{
      border:0;
      border-radius: 18px;
      box-shadow: var(--shadow);
      overflow:hidden;
      transition: transform .12s ease;
    }
    .qcard:hover{ transform: translateY(-2px); }
    .topstripe{
      height: 7px;
      background: linear-gradient(90deg, var(--c1), var(--c2), var(--c3), var(--c4));
    }
    .qtitle{
      font-weight: 900;
      letter-spacing: -0.02em;
    }
    .btn-grad{
      border:0;
      background: linear-gradient(110deg, var(--c1), var(--c2));
      color:#fff;
      box-shadow: 0 12px 18px rgba(14,165,233,.18);
    }
    .btn-green{
      border:0;
      background: linear-gradient(110deg, var(--c3), #16a34a);
      color:#fff;
      box-shadow: 0 12px 18px rgba(34,197,94,.16);
    }
    .btn-orange{
      border:0;
      background: linear-gradient(110deg, var(--c4), #fb7185);
      color:#fff;
      box-shadow: 0 12px 18px rgba(249,115,22,.14);
    }
    .result-card{
      border:0;
      border-radius: 18px;
      box-shadow: var(--shadow);
    }
    .table thead th{
      position: sticky;
      top: 0;
      background: #fff;
      z-index: 1;
    }
    .muted{ color:#6b7280; }
    .footer-note{ color:#6b7280; font-size:.92rem; }
    .small-badge{
      border-radius: 999px;
      padding: .35rem .65rem;
      font-weight: 700;
      font-size: .85rem;
    }
  </style>
</head>

<body>
  <div class="container py-4 py-md-5">

    <div class="hero p-4 p-md-5 mb-5">
      <div class="d-flex flex-column flex-md-row justify-content-between gap-3">
        <div>
          <h1 class="display-6 mb-2 fw-bold">
            B5: BigQuery Query Runner <span class="opacity-75">(Cloud Run)</span>
          </h1>

          <div class="d-flex flex-wrap gap-2">
            <span class="pill"><i class="bi bi-cloud-check"></i> Flask + Gunicorn</span>
            <span class="pill"><i class="bi bi-database-check"></i> BigQuery</span>
            <span class="pill"><i class="bi bi-shield-lock"></i> ADC Auth</span>
          </div>

          <div class="mt-3">
            <small class="opacity-90">
              Billing/Project: <code class="badge">{{ project or "auto" }}</code>
              &nbsp; Dataset: <code class="badge">{{ dataset }}</code>
            </small>
            <div class="mt-2">
              <span class="small-badge bg-light text-dark">
                <i class="bi bi-clock-history"></i> Page loaded: {{ loaded_at }}
              </span>
            </div>
          </div>
        </div>
        </div>

    {% if error %}
      <div class="alert alert-danger border-0 shadow-sm mb-4">
        <div class="d-flex gap-2 align-items-start">
          <i class="bi bi-exclamation-triangle-fill"></i>
          <div>
            <div class="fw-bold">Query failed</div>
            <div class="small">{{ error }}</div>
          </div>
        </div>
      </div>
    {% endif %}

    <div class="row g-4 mt-4">
      <div class="col-12 col-lg-4">
        <div class="card qcard h-100">
          <div class="topstripe"></div>
          <div class="card-body p-4">
            <div class="d-flex align-items-center justify-content-between mb-2">
              <div class="qtitle">Query A</div>
              <i class="bi bi-tags fs-4 text-primary"></i>
            </div>
            <div class="muted mb-3">{{ queries["A"][0] }}</div>
            <form method="POST" action="/run">
              <input type="hidden" name="query" value="A">
              <button class="btn btn-grad w-100 py-2">
                <i class="bi bi-play-fill"></i> Run Query A
              </button>
            </form>
          </div>
        </div>
      </div>

      <div class="col-12 col-lg-4">
        <div class="card qcard h-100">
          <div class="topstripe"></div>
          <div class="card-body p-4">
            <div class="d-flex align-items-center justify-content-between mb-2">
              <div class="qtitle">Query B</div>
              <i class="bi bi-truck fs-4 text-success"></i>
            </div>
            <div class="muted mb-3">{{ queries["B"][0] }}</div>
            <form method="POST" action="/run">
              <input type="hidden" name="query" value="B">
              <button class="btn btn-green w-100 py-2">
                <i class="bi bi-play-fill"></i> Run Query B
              </button>
            </form>
          </div>
        </div>
      </div>

      <div class="col-12 col-lg-4">
        <div class="card qcard h-100">
          <div class="topstripe"></div>
          <div class="card-body p-4">
            <div class="d-flex align-items-center justify-content-between mb-2">
              <div class="qtitle">Query C</div>
              <i class="bi bi-graph-up-arrow fs-4 text-danger"></i>
            </div>
            <div class="muted mb-3">{{ queries["C"][0] }}</div>
            <form method="POST" action="/run">
              <input type="hidden" name="query" value="C">
              <button class="btn btn-orange w-100 py-2">
                <i class="bi bi-play-fill"></i> Run Query C
              </button>
            </form>
          </div>
        </div>
      </div>
    </div>

    {% if columns and rows %}
      <div class="card result-card mt-4">
        <div class="card-body p-4">
          <div class="d-flex flex-column flex-md-row gap-2 justify-content-between align-items-start align-items-md-center mb-3">
            <div>
              <h3 class="h4 mb-0 fw-bold">Results (Query {{ last_query }})</h3>
              <div class="muted small">{{ queries[last_query][0] }}</div>
            </div>
            <span class="badge text-bg-dark px-3 py-2">
              <i class="bi bi-table"></i> {{ rows|length }} rows
            </span>
          </div>

          <div class="table-responsive" style="max-height: 520px;">
            <table class="table table-hover align-middle">
              <thead>
                <tr>
                  {% for c in columns %}
                    <th class="text-nowrap">{{ c }}</th>
                  {% endfor %}
                </tr>
              </thead>
              <tbody>
                {% for r in rows %}
                  <tr>
                    {% for cell in r %}
                      <td class="text-nowrap">
                        {% if cell is number %}
                          {{ "%.2f"|format(cell) if cell is float else cell }}
                        {% else %}
                          {{ cell }}
                        {% endif %}
                      </td>
                    {% endfor %}
                  </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>

          
        </div>
      </div>
    {% else %}
      <div class="mt-4 footer-note">
        Data Last Updated: {{ last_updated }}
      </div>
    {% endif %}

  </div>
</body>
</html>
"""


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/")
def home():
    last_updated = os.getenv("DATA_LAST_UPDATED", "").strip() or "—"
    return render_template_string(
        PAGE,
        project=PROJECT_ID,
        dataset=BQ_DATASET,
        queries=QUERIES,
        columns=None,
        rows=None,
        last_query=None,
        error=None,
        last_updated=last_updated,
        loaded_at=now_utc_str(),
    )


@app.post("/run")
def run():
    q = (request.form.get("query") or "").strip().upper()
    if q not in QUERIES:
        return render_template_string(
            PAGE,
            project=PROJECT_ID,
            dataset=BQ_DATASET,
            queries=QUERIES,
            columns=None,
            rows=None,
            last_query=None,
            error="Invalid query selection. Use A, B, or C.",
            last_updated="—",
            loaded_at=now_utc_str(),
        ), 400

    _, sql = QUERIES[q]

    try:
        columns, rows = run_bigquery(sql)
        return render_template_string(
            PAGE,
            project=PROJECT_ID,
            dataset=BQ_DATASET,
            queries=QUERIES,
            columns=columns,
            rows=rows,
            last_query=q,
            error=None,
            last_updated="—",
            loaded_at=now_utc_str(),
        )
    except BadRequest as e:
        msg = e.message if hasattr(e, "message") else str(e)
        return render_template_string(
            PAGE,
            project=PROJECT_ID,
            dataset=BQ_DATASET,
            queries=QUERIES,
            columns=None,
            rows=None,
            last_query=None,
            error=f"BigQuery BadRequest: {msg}",
            last_updated="—",
            loaded_at=now_utc_str(),
        ), 400
    except Exception as e:
        return render_template_string(
            PAGE,
            project=PROJECT_ID,
            dataset=BQ_DATASET,
            queries=QUERIES,
            columns=None,
            rows=None,
            last_query=None,
            error=f"{type(e).__name__}: {str(e)}",
            last_updated="—",
            loaded_at=now_utc_str(),
        ), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
