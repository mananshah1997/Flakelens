# ❄️🔍 FlakeLens

**A fast, bi-directional lineage mapping tool for Snowflake and Grafana.**

FlakeLens is a modern Python desktop application that solves a classic data engineering problem: bridging the gap between your frontend visualizations and your backend data warehouse. It instantly answers two critical questions:
1. *"If I modify or drop this Snowflake table, which Grafana dashboards will break?"*
2. *"What specific Snowflake tables are powering this complex Grafana dashboard?"*

Built with a sleek dark-mode UI, FlakeLens fetches your dashboards, parses the raw SQL queries, and builds a comprehensive dependency map in seconds using background multi-threading.

---

## ✨ Features

* 🔄 **Bi-Directional Search:** Seamlessly toggle between "Find Dashboards from Table" and "Find Tables from Dashboard".
* ⚡ **Lightning Fast Indexing:** Utilizes a `ThreadPoolExecutor` to concurrently fetch and process Grafana dashboards without freezing the user interface.
* 🧠 **Smart SQL Parsing:** Powered by `sqlglot`, FlakeLens deeply understands Snowflake SQL dialects. It automatically bypasses Grafana macros (like `$__timeFilter`), ignores CTEs, and normalizes table identifiers (stripping databases to match exactly on `SCHEMA.TABLE`).
* 🎨 **Modern Desktop UI:** Built with `customtkinter`, featuring a responsive dark-mode interface, live typing autocomplete, animated loading states, and expandable accordion results.
* 🔒 **Secure by Design:** Credentials are never hardcoded. It uses a `.env` file to securely load your Grafana API token.

---

## 🛠️ Prerequisites

Before you begin, ensure you have met the following requirements:
* **Python 3.8** or higher installed.
* A **Grafana API Token** (Viewer access is sufficient).

---

## 🚀 Installation & Setup

**1. Clone the repository:**
```bash
git clone [https://github.com/yourusername/flakelens.git](https://github.com/yourusername/flakelens.git)
cd flakelens
```

**2. Install dependencies:**
```bash
pip install -r requirements.txt
```

**3. Configure Environment Variables:**
Create a file named .env in the root directory of the project and add your Grafana credentials:
* GRAFANA_URL=[https://your-company.grafana.net](https://your-company.grafana.net)
* GRAFANA_TOKEN=<your_api_token_here>
