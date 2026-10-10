package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

var landingHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TaskFlow — Distributed Background Job Queue</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-dark: #0a0d14;
      --card-bg: rgba(18, 24, 38, 0.7);
      --card-border: rgba(255, 255, 255, 0.08);
      --accent-cyan: #00f2fe;
      --accent-blue: #4facfe;
      --accent-green: #10b981;
      --accent-purple: #8b5cf6;
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background-color: var(--bg-dark);
      background-image: 
        radial-gradient(circle at 15% 15%, rgba(79, 172, 254, 0.12) 0%, transparent 40%),
        radial-gradient(circle at 85% 85%, rgba(139, 92, 246, 0.12) 0%, transparent 40%);
      color: var(--text-main);
      font-family: 'Plus Jakarta Sans', sans-serif;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 2.5rem 1.25rem;
    }

    .container {
      max-width: 900px;
      width: 100%;
    }

    header {
      text-align: center;
      margin-bottom: 2.5rem;
    }

    .badge-bar {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      background: rgba(16, 185, 129, 0.1);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: #34d399;
      padding: 0.35rem 0.85rem;
      border-radius: 9999px;
      font-size: 0.85rem;
      font-weight: 600;
      margin-bottom: 1.25rem;
    }

    .pulse-dot {
      width: 8px;
      height: 8px;
      background: #10b981;
      border-radius: 50%;
      box-shadow: 0 0 10px #10b981;
      animation: pulse 2s infinite;
    }

    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }

    h1 {
      font-size: clamp(2rem, 4vw, 3rem);
      font-weight: 800;
      letter-spacing: -0.03em;
      line-height: 1.15;
      background: linear-gradient(135deg, #ffffff 40%, var(--accent-blue) 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      margin-bottom: 0.75rem;
    }

    p.subtitle {
      color: var(--text-muted);
      font-size: 1.05rem;
      max-width: 620px;
      margin: 0 auto 1.5rem;
      line-height: 1.6;
    }

    .pills {
      display: flex;
      flex-wrap: wrap;
      justify-content: center;
      gap: 0.5rem;
      margin-bottom: 2.5rem;
    }

    .pill {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--card-border);
      padding: 0.3rem 0.75rem;
      border-radius: 6px;
      font-size: 0.8rem;
      font-family: 'JetBrains Mono', monospace;
      color: #cbd5e1;
    }

    .section-title {
      font-size: 1.2rem;
      font-weight: 700;
      margin-bottom: 1rem;
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 1.25rem;
      margin-bottom: 2.5rem;
    }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      backdrop-filter: blur(12px);
      border-radius: 14px;
      padding: 1.25rem;
      transition: all 0.25s ease;
      text-decoration: none;
      color: inherit;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }

    .card:hover {
      border-color: rgba(79, 172, 254, 0.4);
      transform: translateY(-3px);
      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.4);
    }

    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 0.5rem;
    }

    .card-endpoint {
      font-family: 'JetBrains Mono', monospace;
      font-weight: 600;
      font-size: 0.95rem;
      color: var(--accent-cyan);
    }

    .badge-method {
      background: rgba(79, 172, 254, 0.15);
      color: var(--accent-blue);
      font-size: 0.7rem;
      font-weight: 700;
      padding: 0.15rem 0.45rem;
      border-radius: 4px;
      text-transform: uppercase;
    }

    .card-desc {
      color: var(--text-muted);
      font-size: 0.88rem;
      line-height: 1.5;
    }

    .code-box {
      background: rgba(10, 14, 22, 0.9);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 1.25rem;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.85rem;
      color: #e2e8f0;
      overflow-x: auto;
      line-height: 1.6;
      margin-bottom: 2rem;
    }

    .code-box .comment { color: #64748b; }
    .code-box .keyword { color: #38bdf8; }
    .code-box .str { color: #a7f3d0; }

    footer {
      text-align: center;
      color: var(--text-muted);
      font-size: 0.85rem;
      margin-top: auto;
      padding-top: 2rem;
      border-top: 1px solid var(--card-border);
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="badge-bar">
        <span class="pulse-dot"></span>
        TaskFlow Engine Live
      </div>
      <h1>TaskFlow</h1>
      <p class="subtitle">A distributed, transactional background job queue in Go powered by PostgreSQL with atomic row claiming, fencing protection, exponential retries, and Prometheus telemetry.</p>
      
      <div class="pills">
        <span class="pill">PostgreSQL SKIP LOCKED</span>
        <span class="pill">Lease Fencing Tokens</span>
        <span class="pill">Bounded Goroutine Pool</span>
        <span class="pill">At-Least-Once Delivery</span>
        <span class="pill">Go 1.24</span>
      </div>
    </header>

    <h2 class="section-title">⚡ Live Operational Endpoints</h2>
    <div class="grid">
      <a class="card" href="/health/live" target="_blank">
        <div>
          <div class="card-header">
            <span class="card-endpoint">/health/live</span>
            <span class="badge-method">GET</span>
          </div>
          <p class="card-desc">Liveness probe confirming the HTTP server process is responsive.</p>
        </div>
      </a>

      <a class="card" href="/health/ready" target="_blank">
        <div>
          <div class="card-header">
            <span class="card-endpoint">/health/ready</span>
            <span class="badge-method">GET</span>
          </div>
          <p class="card-desc">Readiness probe verifying PostgreSQL connection pool health.</p>
        </div>
      </a>

      <a class="card" href="/metrics" target="_blank">
        <div>
          <div class="card-header">
            <span class="card-endpoint">/metrics</span>
            <span class="badge-method">GET</span>
          </div>
          <p class="card-desc">Real-time Prometheus telemetry for job counts, durations, and active workers.</p>
        </div>
      </a>

      <a class="card" href="/api/v1/ping" target="_blank">
        <div>
          <div class="card-header">
            <span class="card-endpoint">/api/v1/ping</span>
            <span class="badge-method">GET</span>
          </div>
          <p class="card-desc">API v1 connectivity heartbeat ping.</p>
        </div>
      </a>
    </div>

    <h2 class="section-title">💻 Quick Job Submission Example</h2>
    <div class="code-box">
<span class="comment"># Submit a background job to TaskFlow</span>
curl -X POST https://taskflow-bu6t.onrender.com/api/v1/jobs \
  -H <span class="str">"Content-Type: application/json"</span> \
  -d '{
    <span class="keyword">"queue"</span>: <span class="str">"default"</span>,
    <span class="keyword">"type"</span>: <span class="str">"echo"</span>,
    <span class="keyword">"priority"</span>: 10,
    <span class="keyword">"max_attempts"</span>: 3,
    <span class="keyword">"payload"</span>: { <span class="str">"message"</span>: <span class="str">"Hello from TaskFlow!"</span> }
  }'
    </div>

    <footer>
      TaskFlow Distributed Queue &bull; Built with Go &bull; Deployed on Render
    </footer>
  </div>
</body>
</html>`

// HandleRoot serves an interactive status dashboard for browsers or JSON summary for API clients.
func HandleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "application/json") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"system":    "TaskFlow",
			"status":    "online",
			"version":   "1.0.0",
			"endpoints": []string{"/health/live", "/health/ready", "/metrics", "/api/v1/ping", "/api/v1/jobs"},
		})
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(landingHTML))
}
