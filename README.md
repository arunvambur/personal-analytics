# personal-analytics

1️⃣ Create the environment

From your project root:

python3 -m venv .venv


This creates a folder:

.venv/

2️⃣ Activate the environment

macOS / Linux

source .venv/bin/activate


Windows (PowerShell)

.venv\Scripts\Activate.ps1


You should now see:

(.venv)


in your terminal.

3️⃣ Upgrade pip
pip install --upgrade pip

4️⃣ Install dependencies
pip install pandas numpy httpx pyyaml duckdb python-dotenv

5️⃣ Freeze dependencies (important)
pip freeze > requirements.txt

🔁 Deactivate
deactivate

🥇 Best Practice for Your Project
trading-analytics/
├── .venv/              # local virtual environment
├── requirements.txt
├── pyproject.toml      # optional (recommended)
└── src/


👉 Add .venv/ to .gitignore