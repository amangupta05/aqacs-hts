# from fastapi import FastAPI, Query
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from datetime import datetime
from api.routes import v1 

app = FastAPI(title="AQACS DEV API", version="0.0.1")
app.include_router(v1.router, prefix="/v1")


@app.get("/", response_class=HTMLResponse)
def root():
    """Basic landing page so hitting '/' does not return a 404."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>AQACS API</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2.5rem auto; max-width: 640px; line-height: 1.6; color: #222; }
    h1 { margin-bottom: 0.5rem; }
    a { color: #0366d6; text-decoration: none; }
    a:hover { text-decoration: underline; }
    code { background: #f6f8fa; padding: 0.15rem 0.35rem; border-radius: 3px; }
    ul { margin-top: 1rem; }
    li { margin-bottom: 0.6rem; }
    .note { margin-top: 1.5rem; font-size: 0.9rem; color: #555; }
  </style>
</head>
<body>
  <h1>AQACS Dev API</h1>
  <p>This FastAPI service exposes HTS tariff lookups and QA endpoints.</p>
  <ul>
    <li><a href="/docs">Interactive API docs</a> (Swagger UI)</li>
    <li><a href="/redoc">Schema reference</a> (Redoc)</li>
    <li><a href="/qa">Simple RAG QA playground</a></li>
    <li><code>GET /v1/health</code> for a JSON health probe</li>
  </ul>
  <p class="note">Running locally? Make requests via <code>http://127.0.0.1:8080</code> (or <code>http://localhost:8080</code>).</p>
</body>
</html>
"""


@app.get("/v1/health")
def health():
    return {
        "status": "ok",
        "env": "dev",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }


@app.get("/qa", response_class=HTMLResponse)
def qa_ui():
    """Minimal browser UI for exercising the RAG QA endpoint."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>AQACS RAG QA</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2rem auto; max-width: 720px; line-height: 1.5; }
    textarea { width: 100%; min-height: 120px; font-size: 1rem; }
    button { padding: 0.5rem 1.2rem; font-size: 1rem; margin-top: 0.75rem; }
    pre { background: #f4f4f4; padding: 1rem; overflow: auto; }
    .answer { border-left: 4px solid #007acc; padding-left: 1rem; margin-top: 1.5rem; }
    .sources { margin-top: 1rem; }
    .sources li { margin-bottom: 0.35rem; }
    .status { margin-top: 1rem; color: #666; font-size: 0.9rem; }
  </style>
</head>
<body>
  <h1>AQACS RAG QA playground</h1>
  <p>Submit a question to call <code>/v1/qa</code> against the active HTS snapshot.</p>
  <label for="question"><strong>Question</strong></label>
  <textarea id="question" placeholder="e.g. What is the duty rate for purebred breeding horses?"></textarea>
  <br />
  <label for="limit">Max contexts (1-12)</label>
  <input id="limit" type="number" min="1" max="12" value="8" />
  <br />
  <button onclick="runQuery()">Ask</button>
  <div class="status" id="status"></div>
  <div id="result" class="answer"></div>
  <script>
    async function runQuery() {
      const question = document.getElementById("question").value.trim();
      const limit = parseInt(document.getElementById("limit").value, 10) || 8;
      const status = document.getElementById("status");
      const result = document.getElementById("result");
      if (!question) {
        status.textContent = "Please enter a question first.";
        return;
      }
      status.textContent = "Querying /v1/qa...";
      result.innerHTML = "";
      try {
        const resp = await fetch("/v1/qa", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ question, limit })
        });
        const data = await resp.json();
        if (!resp.ok) {
          status.textContent = "Request failed: " + (data.detail || resp.statusText);
          return;
        }
        status.textContent = "Answered using snapshot " + data.snapshot_id;
        let html = "<h2>Answer</h2>";
        html += "<p><strong>" + (data.answer || "No answer returned") + "</strong></p>";
        html += "<p><em>Confidence:</em> " + (data.confidence ?? 0).toFixed(2) + "</p>";
        if (data.supporting_excerpt) {
          html += "<p><em>Excerpt:</em> " + data.supporting_excerpt + "</p>";
        }
        if (data.sources && data.sources.length) {
          html += "<div class='sources'><h3>Sources</h3><ol>";
          data.sources.forEach((s) => {
            html += "<li><code>" + (s.code || "n/a") + "</code> (chapter " + (s.chapter || "n/a") + ", row " + (s.row_index ?? "n/a") + ", " + (s.source_csv || "csv") + ") — score " + (s.score ?? 0).toFixed(3) + "</li>";
          });
          html += "</ol></div>";
        }
        result.innerHTML = html;
      } catch (err) {
        status.textContent = "Error: " + err;
      }
    }
  </script>
</body>
</html>
"""
