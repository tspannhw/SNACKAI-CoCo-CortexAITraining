# Snowflake Cortex AI: How & Why
## Half-Day Training Course — Customer Edition
### Accuracy, Security, and the AI Differentiator

**Duration:** 4.5 Hours  
**Audience:** Data Engineers, Analysts, Architects, Technical Decision Makers  
**Prerequisites:** Basic Snowflake familiarity, SQL fundamentals  

---

## Table of Contents

1. [Welcome & Course Overview](#welcome)
2. [Module 1 — The "Why": Cortex AI as a Strategic Differentiator](#module1)
3. [Module 2 — Semantic Modeling & Unified Semantic Layer](#module2)
4. [Module 3 — Text-to-SQL Accuracy & Self-Serve Analytics](#module3)
5. [Module 4 — Governance, Security & Compliance](#module4)
6. [Module 5 — Cortex Code: AI-Powered Development](#module5)
7. [Wrap-Up & Next Steps](#wrapup)

---

## Setup: Environment Prerequisites

Before the course begins, run the following setup SQL in your Snowflake account.

```sql
-- COURSE SETUP
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS CORTEX_COURSE_DB;
CREATE SCHEMA IF NOT EXISTS CORTEX_COURSE_DB.LABS;
CREATE WAREHOUSE IF NOT EXISTS CORTEX_COURSE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE DATABASE CORTEX_COURSE_DB;
USE SCHEMA CORTEX_COURSE_DB.LABS;
USE WAREHOUSE CORTEX_COURSE_WH;

-- Sample issuer/ESG data
CREATE OR REPLACE TABLE issuers (
    issuer_id      NUMBER AUTOINCREMENT PRIMARY KEY,
    issuer_name    VARCHAR(200),
    sector         VARCHAR(100),
    region         VARCHAR(100),
    market_cap_usd NUMBER(18,2),
    esg_score      FLOAT,
    esg_rating     VARCHAR(10),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE esg_disclosures (
    disclosure_id  NUMBER AUTOINCREMENT PRIMARY KEY,
    issuer_id      NUMBER REFERENCES issuers(issuer_id),
    disclosure_date DATE,
    document_type  VARCHAR(50),
    sentiment_score FLOAT,
    sentiment_label VARCHAR(20),
    summary_text   TEXT,
    source_file    VARCHAR(500),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE financial_metrics (
    metric_id      NUMBER AUTOINCREMENT PRIMARY KEY,
    issuer_id      NUMBER REFERENCES issuers(issuer_id),
    report_date    DATE,
    revenue        NUMBER(18,2),
    net_income     NUMBER(18,2),
    total_assets   NUMBER(18,2),
    debt_ratio     FLOAT,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Seed data
INSERT INTO issuers (issuer_name, sector, region, market_cap_usd, esg_score, esg_rating) VALUES
  ('Apple Inc.',         'Technology',   'North America', 2800000000000, 78.5, 'AA'),
  ('Microsoft Corp.',    'Technology',   'North America', 2500000000000, 82.1, 'AAA'),
  ('JPMorgan Chase',     'Financials',   'North America',  450000000000, 71.3, 'A'),
  ('Exxon Mobil',        'Energy',       'North America',  400000000000, 45.2, 'BB'),
  ('Nestle SA',          'Consumer',     'Europe',         320000000000, 80.4, 'AA'),
  ('HSBC Holdings',      'Financials',   'Europe',         180000000000, 69.8, 'A'),
  ('Toyota Motor',       'Automotive',   'Asia Pacific',   250000000000, 74.6, 'AA'),
  ('Samsung Electronics','Technology',   'Asia Pacific',   380000000000, 70.1, 'A'),
  ('BlackRock Inc.',     'Financials',   'North America',  120000000000, 76.9, 'AA'),
  ('Unilever PLC',       'Consumer',     'Europe',         130000000000, 83.7, 'AAA');

INSERT INTO esg_disclosures (issuer_id, disclosure_date, document_type, sentiment_score, sentiment_label, summary_text, source_file) VALUES
  (1, '2024-01-15', 'Annual Report',     0.72, 'POSITIVE', 'Strong carbon reduction commitments with 2030 net-zero target.', 's3://docs/aapl_2024_esg.pdf'),
  (1, '2023-07-10', 'ESG Update',        0.65, 'POSITIVE', 'Supply chain sustainability improvements noted.', 's3://docs/aapl_2023_esg.pdf'),
  (2, '2024-02-20', 'Annual Report',     0.85, 'POSITIVE', 'Achieved carbon neutrality across all datacenters.', 's3://docs/msft_2024_esg.pdf'),
  (3, '2024-03-05', 'ESG Report',        0.55, 'NEUTRAL',  'Moderate climate risk exposure with mitigation plans.', 's3://docs/jpm_2024_esg.pdf'),
  (4, '2024-01-30', 'Annual Report',    -0.30, 'NEGATIVE', 'Flaring emissions increased YoY, under regulatory scrutiny.', 's3://docs/xom_2024_esg.pdf'),
  (5, '2024-04-12', 'Sustainability',    0.80, 'POSITIVE', 'Packaging reduction exceeded 2025 targets two years early.', 's3://docs/nestle_2024_esg.pdf'),
  (6, '2024-02-28', 'ESG Disclosure',    0.60, 'POSITIVE', 'Financed emissions down 15% from 2022 baseline.', 's3://docs/hsbc_2024_esg.pdf'),
  (7, '2024-03-20', 'Sustainability',    0.70, 'POSITIVE', 'EV production milestones exceeded targets.', 's3://docs/toyota_2024_esg.pdf'),
  (8, '2023-12-01', 'Annual Report',     0.50, 'NEUTRAL',  'Semiconductor supply chain ESG audit underway.', 's3://docs/samsung_2023_esg.pdf'),
  (9, '2024-01-20', 'ESG Framework',     0.75, 'POSITIVE', 'Portfolio decarbonization index fund launched.', 's3://docs/blk_2024_esg.pdf'),
  (10,'2024-04-05', 'Sustainability',    0.88, 'POSITIVE', 'Reached 100% renewable energy sourcing globally.', 's3://docs/unilever_2024_esg.pdf');

INSERT INTO financial_metrics (issuer_id, report_date, revenue, net_income, total_assets, debt_ratio) VALUES
  (1, '2024-03-31', 381623000000, 96995000000, 352583000000, 0.31),
  (2, '2024-03-31', 211915000000, 72361000000, 411976000000, 0.19),
  (3, '2024-03-31', 158104000000, 49552000000, 3875393000000, 0.12),
  (4, '2024-03-31',  398675000000, 36010000000, 376317000000, 0.22),
  (5, '2024-03-31',   93769000000,  8976000000,  66318000000, 0.27),
  (6, '2024-03-31',   65932000000,  9400000000, 3038677000000, 0.08),
  (7, '2024-03-31',  274491000000, 18704000000, 671951000000, 0.20),
  (8, '2024-03-31',  224354000000, 14901000000, 399078000000, 0.30),
  (9, '2024-03-31',   17859000000,  5502000000, 1307448000000, 0.04),
  (10,'2024-03-31',   59953000000,  5100000000,  20320000000, 0.35);
```

---

<a name="welcome"></a>
## Welcome & Course Overview *(15 min)*

### What You Will Learn

By the end of this course you will be able to:

- Explain the Snowflake Cortex AI architecture and why it is secure-by-design
- Build and publish Semantic Views for natural language querying
- Use Cortex Analyst to deliver 92%+ accurate text-to-SQL self-serve analytics
- Configure governance policies that apply automatically to AI workloads
- Use Cortex Code to generate, explain, and run SQL and Python inside Snowflake

### The Core Insight

> Snowflake brings the AI to the data. Not the data to the AI.

This single architectural decision is what makes Cortex AI simultaneously the most accurate **and** the most secure AI platform for regulated data.

---

<a name="module1"></a>
## Module 1 — The "Why": Cortex AI as a Strategic Differentiator *(45 min)*

### 1.1 The Problem with Conventional AI Approaches

Most enterprise AI tools work like this:

```
Your Data → Export/Copy → Third-Party AI Service → Answer
```

Problems:
- **Data leaves your perimeter** — violates DORA, SEC 17a-4, GDPR
- **Stale data** — copies are always behind
- **No governance** — masking policies don't follow the data
- **Low accuracy** — AI has no schema context, hallucinates table names

### 1.2 The Snowflake Cortex Approach

```
Natural Language Query
        ↓
  Cortex Agent (inside Snowflake)
        ↓
  ┌──────────────────────────────┐
  │    Your Snowflake Account    │
  │  ┌────────┐  ┌────────────┐  │
  │  │ Tables │  │ PDF/Docs   │  │
  │  │  SQL   │  │ Vectors    │  │
  │  └────────┘  └────────────┘  │
  │         LLM runs HERE        │
  └──────────────────────────────┘
        ↓
  Verified, Governed Answer
```

### 1.3 Cortex AI Component Map

| Component | What It Does | Key Benefit |
|-----------|-------------|-------------|
| **Cortex Analyst** | Natural language → SQL | 92%+ accuracy |
| **Cortex Search** | Semantic search over unstructured docs | Retrieval-augmented generation |
| **Cortex Agents** | Orchestrates Analyst + Search + tools | Multi-step reasoning |
| **Cortex Code** | AI coding assistant in the IDE | Accelerated development |
| **Semantic Views** | Business logic layer for AI queries | Accuracy + consistency |
| **Cortex Functions** | COMPLETE, SENTIMENT, CLASSIFY, EXTRACT | Inline AI in SQL |

### 1.4 Lab 1-A: Your First Cortex Function

```sql
-- Sentiment analysis inline in SQL — no external API call
SELECT
    issuer_name,
    summary_text,
    SNOWFLAKE.CORTEX.SENTIMENT(summary_text)          AS sentiment_score_ai,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        'Summarize this ESG disclosure in one sentence: ' || summary_text
    )                                                  AS ai_summary
FROM esg_disclosures d
JOIN issuers i ON d.issuer_id = i.issuer_id
LIMIT 5;
```

### 1.5 Lab 1-B: Classify ESG Risk

```sql
-- Use AI_CLASSIFY to bucket disclosures by risk level
SELECT
    issuer_name,
    summary_text,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        summary_text,
        ['LOW_RISK', 'MEDIUM_RISK', 'HIGH_RISK', 'REGULATORY_CONCERN']
    ):label::VARCHAR AS risk_classification
FROM esg_disclosures d
JOIN issuers i ON d.issuer_id = i.issuer_id;
```

### 1.6 Lab 1-C: Extract Structured Data from Text

```sql
-- Extract key facts from unstructured ESG summaries
SELECT
    issuer_name,
    summary_text,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        summary_text,
        'What is the main environmental commitment mentioned?'
    ) AS commitment_extracted
FROM esg_disclosures d
JOIN issuers i ON d.issuer_id = i.issuer_id;
```

---

<a name="module2"></a>
## Module 2 — Semantic Modeling & Unified Semantic Layer *(60 min)*

### 2.1 What Is a Semantic View?

A **Semantic View** is not a database view. It is an AI-readable business logic layer that tells Cortex Analyst:
- What your tables mean
- How they join
- What your metrics, dimensions, and filters are
- What verified SQL answers look like for common questions

**Without a Semantic View:**  
Cortex Analyst guesses table names and relationships → lower accuracy

**With a Semantic View:**  
Cortex Analyst has full business context → 92%+ accuracy

### 2.2 Semantic View YAML Structure

```yaml
# semantic_model.yaml
name: esg_analytics
description: >
  ESG disclosure and financial performance analytics
  for the top issuers universe.

tables:
  - name: issuers
    base_table: CORTEX_COURSE_DB.LABS.ISSUERS
    description: Master list of tracked issuers with ESG ratings.
    primary_key: [issuer_id]
    columns:
      - name: issuer_id
        description: Unique issuer identifier.
        data_type: NUMBER
      - name: issuer_name
        description: Full legal name of the issuer.
        data_type: VARCHAR
        synonyms: [company, firm, entity]
      - name: sector
        description: GICS sector classification.
        data_type: VARCHAR
      - name: region
        description: Geographic region.
        data_type: VARCHAR
      - name: market_cap_usd
        description: Market capitalization in USD.
        data_type: NUMBER
        format: currency
      - name: esg_score
        description: Composite ESG score 0-100. Higher is better.
        data_type: FLOAT
      - name: esg_rating
        description: Letter rating — AAA, AA, A, BB, B, CCC.
        data_type: VARCHAR

  - name: esg_disclosures
    base_table: CORTEX_COURSE_DB.LABS.ESG_DISCLOSURES
    description: Individual ESG disclosure events with AI sentiment scores.
    primary_key: [disclosure_id]
    foreign_keys:
      - column: issuer_id
        references_table: issuers
        references_column: issuer_id
    columns:
      - name: disclosure_id
        data_type: NUMBER
      - name: issuer_id
        data_type: NUMBER
      - name: disclosure_date
        description: Date the disclosure was published.
        data_type: DATE
      - name: document_type
        description: Type of document — Annual Report, ESG Update, Sustainability Report.
        data_type: VARCHAR
      - name: sentiment_score
        description: >
          AI-generated sentiment score from -1.0 (very negative) to 1.0 (very positive).
        data_type: FLOAT
      - name: sentiment_label
        description: Categorical sentiment — POSITIVE, NEUTRAL, NEGATIVE.
        data_type: VARCHAR

  - name: financial_metrics
    base_table: CORTEX_COURSE_DB.LABS.FINANCIAL_METRICS
    description: Quarterly financial metrics per issuer.
    primary_key: [metric_id]
    foreign_keys:
      - column: issuer_id
        references_table: issuers
        references_column: issuer_id
    columns:
      - name: report_date
        data_type: DATE
      - name: revenue
        description: Total revenue in USD.
        data_type: NUMBER
        format: currency
      - name: net_income
        description: Net income in USD.
        data_type: NUMBER
        format: currency
      - name: debt_ratio
        description: Total debt as proportion of total assets.
        data_type: FLOAT

metrics:
  - name: avg_esg_score
    description: Average ESG score across all issuers.
    expr: AVG(issuers.esg_score)
    synonyms: [mean esg, average esg score, esg average]

  - name: avg_sentiment
    description: Average sentiment score across ESG disclosures.
    expr: AVG(esg_disclosures.sentiment_score)
    synonyms: [sentiment average, mean sentiment, average sentiment]

  - name: net_sentiment_change
    description: >
      Change in average sentiment score between the most recent
      disclosure and the prior disclosure for each issuer.
    expr: >
      AVG(esg_disclosures.sentiment_score) -
      LAG(AVG(esg_disclosures.sentiment_score))
        OVER (PARTITION BY esg_disclosures.issuer_id
              ORDER BY esg_disclosures.disclosure_date)

  - name: total_market_cap
    description: Sum of market cap across selected issuers.
    expr: SUM(issuers.market_cap_usd)

  - name: profit_margin
    description: Net income as a percentage of revenue.
    expr: AVG(financial_metrics.net_income / NULLIF(financial_metrics.revenue, 0))

dimensions:
  - name: sector
    expr: issuers.sector
    description: GICS sector for grouping and filtering.

  - name: region
    expr: issuers.region
    description: Geographic region.

  - name: esg_rating
    expr: issuers.esg_rating
    description: ESG letter rating bucket.

  - name: sentiment_label
    expr: esg_disclosures.sentiment_label
    description: Positive / Neutral / Negative sentiment bucket.

  - name: document_type
    expr: esg_disclosures.document_type

filters:
  - name: top_500_issuers
    description: Filter to the top 500 issuers by market cap.
    expr: issuers.market_cap_usd > 100000000000

  - name: recent_disclosures
    description: Disclosures from the last 12 months.
    expr: esg_disclosures.disclosure_date >= DATEADD('year', -1, CURRENT_DATE())

  - name: positive_esg_only
    description: Issuers with AAA or AA ESG ratings.
    expr: issuers.esg_rating IN ('AAA', 'AA')

verified_queries:
  - question: "What is the average ESG score by sector?"
    sql: >
      SELECT sector, ROUND(AVG(esg_score), 2) AS avg_esg_score
      FROM CORTEX_COURSE_DB.LABS.ISSUERS
      GROUP BY sector
      ORDER BY avg_esg_score DESC;

  - question: "Which issuers have negative ESG sentiment?"
    sql: >
      SELECT i.issuer_name, d.sentiment_score, d.sentiment_label, d.disclosure_date
      FROM CORTEX_COURSE_DB.LABS.ESG_DISCLOSURES d
      JOIN CORTEX_COURSE_DB.LABS.ISSUERS i ON d.issuer_id = i.issuer_id
      WHERE d.sentiment_label = 'NEGATIVE'
      ORDER BY d.sentiment_score ASC;

  - question: "What is the net sentiment change for the top 10 issuers by market cap?"
    sql: >
      WITH ranked AS (
          SELECT i.issuer_name, i.market_cap_usd,
                 d.sentiment_score, d.disclosure_date,
                 ROW_NUMBER() OVER (PARTITION BY i.issuer_id ORDER BY d.disclosure_date DESC) AS rn
          FROM CORTEX_COURSE_DB.LABS.ISSUERS i
          JOIN CORTEX_COURSE_DB.LABS.ESG_DISCLOSURES d ON i.issuer_id = d.issuer_id
      ),
      latest AS (SELECT issuer_name, market_cap_usd, sentiment_score FROM ranked WHERE rn = 1),
      prior  AS (SELECT issuer_name, sentiment_score FROM ranked WHERE rn = 2)
      SELECT l.issuer_name,
             l.market_cap_usd,
             l.sentiment_score     AS latest_sentiment,
             p.sentiment_score     AS prior_sentiment,
             ROUND(l.sentiment_score - p.sentiment_score, 4) AS net_sentiment_change
      FROM latest l
      JOIN prior p ON l.issuer_name = p.issuer_name
      ORDER BY l.market_cap_usd DESC
      LIMIT 10;
```

### 2.3 Create the Semantic View in Snowflake

```sql
-- Step 1: Create a stage to host the YAML
CREATE OR REPLACE STAGE CORTEX_COURSE_DB.LABS.SEMANTIC_STAGE
  DIRECTORY = (ENABLE = TRUE);

-- Step 2: Upload the YAML via SnowSQL or Snowsight
-- PUT file:///path/to/semantic_model.yaml @CORTEX_COURSE_DB.LABS.SEMANTIC_STAGE;

-- Step 3: Create the Semantic View pointing at the staged file
CREATE OR REPLACE SEMANTIC VIEW CORTEX_COURSE_DB.LABS.ESG_SEMANTIC_VIEW
  FROM @CORTEX_COURSE_DB.LABS.SEMANTIC_STAGE/semantic_model.yaml;

-- Step 4: Verify
SHOW SEMANTIC VIEWS IN SCHEMA CORTEX_COURSE_DB.LABS;
DESCRIBE SEMANTIC VIEW CORTEX_COURSE_DB.LABS.ESG_SEMANTIC_VIEW;
```

### 2.4 Lab 2-A: Query Your Semantic View with Cortex Analyst

```python
# lab_2a_cortex_analyst.py
import os
import json
import snowflake.connector

conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
)

def ask_cortex_analyst(question: str, semantic_view: str) -> dict:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SNOWFLAKE.CORTEX.ANALYST(
            :1,
            OBJECT_CONSTRUCT('semantic_view', :2)
        )
    """, (question, semantic_view))
    row = cursor.fetchone()
    return json.loads(row[0])

SEMANTIC_VIEW = "CORTEX_COURSE_DB.LABS.ESG_SEMANTIC_VIEW"

questions = [
    "What is the average ESG score by sector?",
    "Which issuers have negative ESG sentiment?",
    "Show me the top 5 issuers by market cap with their ESG ratings.",
    "What is the net sentiment change for the top 10 issuers?",
    "Compare revenue and ESG score for technology sector issuers.",
]

for q in questions:
    print(f"\nQuestion: {q}")
    result = ask_cortex_analyst(q, SEMANTIC_VIEW)
    print(f"Generated SQL:\n{result.get('sql', 'N/A')}")
    print(f"Explanation: {result.get('explanation', '')}")
    print("-" * 60)
```

### 2.5 Lab 2-B: REST API Call to Cortex Analyst

```python
# lab_2b_cortex_analyst_rest.py
import os
import requests
import json
import snowflake.connector

conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
)

token = conn._rest._token_provider.get_token()

account = os.getenv("SNOWFLAKE_ACCOUNT", "your_account")
host = f"https://{account}.snowflakecomputing.com"

payload = {
    "messages": [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "What is the net sentiment change in ESG disclosures for our top 500 issuers?"
                }
            ]
        }
    ],
    "semantic_model_file": "@CORTEX_COURSE_DB.LABS.SEMANTIC_STAGE/semantic_model.yaml"
}

headers = {
    "Authorization": f"Snowflake Token=\"{token}\"",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

response = requests.post(
    f"{host}/api/v2/cortex/analyst/message",
    headers=headers,
    json=payload
)

result = response.json()
print(json.dumps(result, indent=2))
```

### 2.6 Lab 2-C: Build a Streamlit Analyst App

```python
# lab_2c_streamlit_analyst_app.py
import streamlit as st
import snowflake.connector
import os
import json
import pandas as pd

st.set_page_config(page_title="ESG Analyst", page_icon="🌿", layout="wide")
st.title("🌿 ESG Intelligence — Powered by Cortex Analyst")
st.caption("Ask any question about your ESG universe in plain English.")

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
    )

conn = get_connection()

SEMANTIC_VIEW = "CORTEX_COURSE_DB.LABS.ESG_SEMANTIC_VIEW"

suggested_questions = [
    "What is the average ESG score by sector?",
    "Which issuers have negative sentiment in their latest disclosure?",
    "Show top 10 issuers by market cap with ESG rating.",
    "Compare net income and ESG score for financials sector.",
    "What is the net sentiment change for top 500 issuers?",
]

st.subheader("Suggested Questions")
cols = st.columns(3)
selected_suggestion = None
for i, q in enumerate(suggested_questions):
    if cols[i % 3].button(q, key=f"sug_{i}"):
        selected_suggestion = q

question = st.text_input(
    "Or type your own question:",
    value=selected_suggestion or "",
    placeholder="e.g. Which issuers improved their ESG score the most this year?"
)

if st.button("Ask", type="primary") and question:
    with st.spinner("Cortex Analyst is reasoning..."):
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT SNOWFLAKE.CORTEX.ANALYST(
                    %s,
                    OBJECT_CONSTRUCT('semantic_view', %s)
                )
            """, (question, SEMANTIC_VIEW))
            row = cur.fetchone()
            result = json.loads(row[0])

            sql_generated = result.get("sql", "")
            explanation   = result.get("explanation", "")
            confidence    = result.get("confidence", None)

            col1, col2 = st.columns([1, 1])

            with col1:
                st.subheader("Generated SQL")
                st.code(sql_generated, language="sql")
                if explanation:
                    st.info(f"**Explanation:** {explanation}")
                if confidence:
                    st.metric("Confidence", f"{round(confidence * 100, 1)}%")

            with col2:
                st.subheader("Results")
                if sql_generated:
                    df = pd.read_sql(sql_generated, conn)
                    st.dataframe(df, use_container_width=True)
                    st.download_button(
                        "Download CSV",
                        df.to_csv(index=False),
                        file_name="esg_query_results.csv"
                    )

        except Exception as e:
            st.error(f"Error: {e}")
```

---

<a name="module3"></a>
## Module 3 — Text-to-SQL Accuracy & Self-Serve Analytics *(45 min)*

### 3.1 Why 92%+ Accuracy Matters

Industry baseline for text-to-SQL without semantic context: **~55-65%**  
Cortex Analyst with Semantic View: **92%+**

The difference is the Semantic View which provides:
- Table and column descriptions
- Join paths (no hallucinated JOINs)
- Metric definitions (no ambiguous aggregations)
- Verified Query Representations (VQRs) — golden SQL examples

### 3.2 Verified Query Representations (VQRs)

VQRs are pre-validated SQL answers stored in your semantic model. When a question closely matches a VQR, Cortex Analyst uses it directly — guaranteeing 100% accuracy for your most common queries.

```sql
-- Manually validate a VQR
SELECT i.issuer_name, i.sector, ROUND(AVG(d.sentiment_score), 4) AS avg_sentiment
FROM CORTEX_COURSE_DB.LABS.ESG_DISCLOSURES d
JOIN CORTEX_COURSE_DB.LABS.ISSUERS i ON d.issuer_id = i.issuer_id
GROUP BY i.issuer_name, i.sector
ORDER BY avg_sentiment DESC;
```

### 3.3 Lab 3-A: Measure Text-to-SQL Accuracy

```python
# lab_3a_accuracy_benchmark.py
import os
import json
import snowflake.connector
import pandas as pd

conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
)

SEMANTIC_VIEW = "CORTEX_COURSE_DB.LABS.ESG_SEMANTIC_VIEW"

test_cases = [
    {
        "question": "What is the average ESG score by sector?",
        "expected_columns": ["sector", "avg_esg_score"],
        "expected_rows_min": 4,
    },
    {
        "question": "List issuers with negative sentiment",
        "expected_columns": ["issuer_name", "sentiment_score"],
        "expected_rows_min": 1,
    },
    {
        "question": "What is total market cap by region?",
        "expected_columns": ["region", "total_market_cap"],
        "expected_rows_min": 2,
    },
    {
        "question": "Show profit margin for all issuers",
        "expected_columns": ["issuer_name", "profit_margin"],
        "expected_rows_min": 5,
    },
    {
        "question": "Which issuers have AAA or AA ESG rating?",
        "expected_columns": ["issuer_name", "esg_rating"],
        "expected_rows_min": 3,
    },
]

results = []
cur = conn.cursor()

for tc in test_cases:
    try:
        cur.execute("""
            SELECT SNOWFLAKE.CORTEX.ANALYST(
                %s,
                OBJECT_CONSTRUCT('semantic_view', %s)
            )
        """, (tc["question"], SEMANTIC_VIEW))

        row = cur.fetchone()
        resp = json.loads(row[0])
        sql  = resp.get("sql", "")

        data_cur = conn.cursor()
        data_cur.execute(sql)
        rows = data_cur.fetchall()
        col_names = [d[0].lower() for d in data_cur.description]

        cols_correct = all(
            any(exp in col for col in col_names)
            for exp in tc["expected_columns"]
        )
        rows_correct = len(rows) >= tc["expected_rows_min"]
        passed       = cols_correct and rows_correct

        results.append({
            "question":    tc["question"],
            "passed":      passed,
            "row_count":   len(rows),
            "columns":     col_names,
            "sql_preview": sql[:120] + "...",
        })

    except Exception as e:
        results.append({"question": tc["question"], "passed": False, "error": str(e)})

df = pd.DataFrame(results)
accuracy = df["passed"].mean() * 100
print(f"\nAccuracy: {accuracy:.1f}% ({df['passed'].sum()}/{len(df)} passed)")
print(df[["question", "passed", "row_count"]].to_string(index=False))
```

### 3.4 Lab 3-B: Advanced Cortex Analyst — Multi-Turn Conversation

```python
# lab_3b_multi_turn.py
import os
import json
import snowflake.connector

conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
)

SEMANTIC_VIEW = "CORTEX_COURSE_DB.LABS.ESG_SEMANTIC_VIEW"
conversation_history = []

def chat(user_message: str) -> str:
    conversation_history.append({
        "role": "user",
        "content": [{"type": "text", "text": user_message}]
    })

    cur = conn.cursor()
    cur.execute("""
        SELECT SNOWFLAKE.CORTEX.ANALYST(
            %s,
            OBJECT_CONSTRUCT(
                'semantic_view',     %s,
                'conversation',      PARSE_JSON(%s)
            )
        )
    """, (user_message, SEMANTIC_VIEW, json.dumps(conversation_history)))

    row = cur.fetchone()
    result = json.loads(row[0])
    sql = result.get("sql", "")

    conversation_history.append({
        "role": "analyst",
        "content": [{"type": "sql", "statement": sql}]
    })
    return sql

turns = [
    "Show me average ESG score by sector.",
    "Now filter that to only North America.",
    "Sort the results by ESG score descending.",
    "Add market cap as a column.",
]

for turn in turns:
    print(f"\nUser: {turn}")
    sql = chat(turn)
    print(f"SQL:\n{sql}\n" + "-" * 60)
```

### 3.5 Lab 3-C: Embed Self-Serve Analytics in a Data Product

```sql
-- Create a secure view for client-facing self-serve analytics
CREATE OR REPLACE SECURE VIEW CORTEX_COURSE_DB.LABS.CLIENT_ESG_ANALYTICS AS
SELECT
    i.issuer_name,
    i.sector,
    i.region,
    i.esg_rating,
    i.esg_score,
    d.disclosure_date,
    d.sentiment_label,
    d.sentiment_score,
    f.revenue,
    f.net_income,
    ROUND(f.net_income / NULLIF(f.revenue, 0) * 100, 2) AS profit_margin_pct
FROM CORTEX_COURSE_DB.LABS.ISSUERS i
LEFT JOIN CORTEX_COURSE_DB.LABS.ESG_DISCLOSURES d ON i.issuer_id = d.issuer_id
LEFT JOIN CORTEX_COURSE_DB.LABS.FINANCIAL_METRICS f ON i.issuer_id = f.issuer_id;

-- Grant to analyst role
GRANT SELECT ON VIEW CORTEX_COURSE_DB.LABS.CLIENT_ESG_ANALYTICS TO ROLE ANALYST_ROLE;

-- Cortex Analyst can now serve this view through the semantic layer
-- Clients ask questions in plain English → verified SQL → governed results
```

---

<a name="module4"></a>
## Module 4 — Governance, Security & Compliance *(30 min)*

### 4.1 Security Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Snowflake Security Perimeter            │
│                                                         │
│  User Query                                             │
│      │                                                  │
│      ▼                                                  │
│  Cortex Agent ──► Semantic Layer ──► SQL Engine         │
│                                          │              │
│                                          ▼              │
│                                   Row Access Policy     │
│                                   Masking Policy        │
│                                   Column Policy         │
│                                          │              │
│                                          ▼              │
│                                   Governed Result       │
│                                                         │
│  LLMs (Llama, Mistral): run INSIDE this perimeter       │
│  Zero data egress. Zero external API calls.             │
└─────────────────────────────────────────────────────────┘
```

### 4.2 Lab 4-A: Dynamic Data Masking with AI Queries

```sql
-- Create a masking policy for sensitive financial data
CREATE OR REPLACE MASKING POLICY CORTEX_COURSE_DB.LABS.MASK_REVENUE AS
  (val NUMBER) RETURNS NUMBER ->
  CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ANALYST_ROLE') THEN val
    ELSE -999999999
  END;

-- Apply masking to the revenue column
ALTER TABLE CORTEX_COURSE_DB.LABS.FINANCIAL_METRICS
  MODIFY COLUMN revenue
  SET MASKING POLICY CORTEX_COURSE_DB.LABS.MASK_REVENUE;

-- Masking applies automatically even when Cortex Analyst generates the SQL
-- A restricted user asking "Show revenue by issuer" gets masked values
```

### 4.3 Lab 4-B: Row Access Policies for Multi-Tenant AI

```sql
-- Row-level security so clients only see their own issuers
CREATE OR REPLACE TABLE CORTEX_COURSE_DB.LABS.CLIENT_ISSUER_MAP (
    client_role  VARCHAR,
    issuer_id    NUMBER
);

INSERT INTO CORTEX_COURSE_DB.LABS.CLIENT_ISSUER_MAP VALUES
  ('CLIENT_A_ROLE', 1), ('CLIENT_A_ROLE', 2), ('CLIENT_A_ROLE', 3),
  ('CLIENT_B_ROLE', 4), ('CLIENT_B_ROLE', 5), ('CLIENT_B_ROLE', 6);

CREATE OR REPLACE ROW ACCESS POLICY CORTEX_COURSE_DB.LABS.ISSUER_ROW_POLICY AS
  (issuer_id NUMBER) RETURNS BOOLEAN ->
  EXISTS (
      SELECT 1 FROM CORTEX_COURSE_DB.LABS.CLIENT_ISSUER_MAP
      WHERE client_role = CURRENT_ROLE()
        AND issuer_id   = issuer_id
  )
  OR CURRENT_ROLE() IN ('SYSADMIN', 'ANALYST_ROLE');

ALTER TABLE CORTEX_COURSE_DB.LABS.ISSUERS
  ADD ROW ACCESS POLICY CORTEX_COURSE_DB.LABS.ISSUER_ROW_POLICY ON (issuer_id);

-- Now when Cortex Analyst generates SQL for CLIENT_A_ROLE,
-- the row policy filters automatically — no prompt injection possible
```

### 4.4 Lab 4-C: Audit AI Query History

```sql
-- Every Cortex Analyst query is logged in QUERY_HISTORY
SELECT
    query_id,
    query_text,
    user_name,
    role_name,
    start_time,
    total_elapsed_time / 1000 AS seconds,
    bytes_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text ILIKE '%CORTEX.ANALYST%'
   OR query_text ILIKE '%CORTEX.COMPLETE%'
ORDER BY start_time DESC
LIMIT 20;

-- Full audit trail satisfies SEC 17a-4 and DORA requirements
```

### 4.5 Compliance Reference

| Regulation | Requirement | How Cortex Satisfies It |
|-----------|-------------|------------------------|
| **SEC 17a-4** | Records must be immutable and auditable | Query history in ACCOUNT_USAGE, Time Travel |
| **DORA** | Data must not leave EU financial perimeter | Cortex runs in-region, no egress |
| **GDPR** | PII must be masked or anonymized | Masking policies apply to ALL queries inc. AI |
| **SOC 2 Type II** | Access must be logged and role-controlled | RBAC + query audit trail |

---

<a name="module5"></a>
## Module 5 — Cortex Code: AI-Powered Development *(45 min)*

### 5.1 What Is Cortex Code?

Cortex Code is the AI coding assistant embedded in the Snowflake IDE (Snowsight and the Cortex Code CLI). It:

- Generates SQL and Python from natural language — grounded in **your** schemas
- Explains query results and errors in plain English
- Suggests completions as you type, aware of your open files and active database
- Runs entirely inside Snowflake's security perimeter — your schema never leaves your account

### 5.2 Cortex Code Prompting Patterns

#### Pattern 1: Schema-Aware SQL Generation

```
Prompt: "Write a SQL query that shows the net sentiment change per issuer 
between their two most recent ESG disclosures. Use the issuers and 
esg_disclosures tables in CORTEX_COURSE_DB.LABS."
```

**Cortex Code output:**
```sql
WITH ranked_disclosures AS (
    SELECT
        i.issuer_id,
        i.issuer_name,
        d.sentiment_score,
        d.disclosure_date,
        ROW_NUMBER() OVER (
            PARTITION BY d.issuer_id
            ORDER BY d.disclosure_date DESC
        ) AS disclosure_rank
    FROM CORTEX_COURSE_DB.LABS.ISSUERS i
    JOIN CORTEX_COURSE_DB.LABS.ESG_DISCLOSURES d
      ON i.issuer_id = d.issuer_id
),
latest AS (
    SELECT issuer_id, issuer_name, sentiment_score
    FROM ranked_disclosures WHERE disclosure_rank = 1
),
prior AS (
    SELECT issuer_id, sentiment_score
    FROM ranked_disclosures WHERE disclosure_rank = 2
)
SELECT
    l.issuer_name,
    l.sentiment_score                               AS latest_sentiment,
    p.sentiment_score                               AS prior_sentiment,
    ROUND(l.sentiment_score - p.sentiment_score, 4) AS net_sentiment_change,
    CASE
        WHEN l.sentiment_score > p.sentiment_score THEN 'IMPROVING'
        WHEN l.sentiment_score < p.sentiment_score THEN 'DECLINING'
        ELSE 'STABLE'
    END                                             AS trend
FROM latest l
JOIN prior p ON l.issuer_id = p.issuer_id
ORDER BY net_sentiment_change DESC;
```

#### Pattern 2: Python Pipeline Generation

```
Prompt: "Write a Snowpark Python function that reads the esg_disclosures 
table, runs CORTEX.SENTIMENT on summary_text, and writes results back 
to a new table called esg_ai_enriched."
```

**Cortex Code output:**
```python
# lab_5a_snowpark_pipeline.py
import os
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import col, call_builtin

session = Session.builder.config(
    "connection_name",
    os.getenv("SNOWFLAKE_CONNECTION_NAME") or "default"
).create()

session.use_database("CORTEX_COURSE_DB")
session.use_schema("LABS")

df = session.table("ESG_DISCLOSURES")

df_enriched = df.with_column(
    "AI_SENTIMENT_SCORE",
    call_builtin(
        "SNOWFLAKE.CORTEX.SENTIMENT",
        col("SUMMARY_TEXT")
    )
).with_column(
    "AI_RISK_LABEL",
    call_builtin(
        "SNOWFLAKE.CORTEX.CLASSIFY_TEXT",
        col("SUMMARY_TEXT"),
        F.lit("['LOW_RISK', 'MEDIUM_RISK', 'HIGH_RISK', 'REGULATORY_CONCERN']")
    )["label"].cast("string")
).with_column(
    "AI_SUMMARY",
    call_builtin(
        "SNOWFLAKE.CORTEX.COMPLETE",
        F.lit("mistral-large2"),
        F.concat(
            F.lit("Summarize this ESG disclosure in one sentence: "),
            col("SUMMARY_TEXT")
        )
    )
)

df_enriched.write.mode("overwrite").save_as_table("ESG_AI_ENRICHED")
print(f"Wrote {df_enriched.count()} rows to ESG_AI_ENRICHED")
session.close()
```

#### Pattern 3: Notebook Cell Generation

Open a Snowflake Notebook and use these Cortex Code prompts cell by cell:

```
Prompt (Cell 1): "Import snowflake.snowpark, pandas, and matplotlib. 
Create a Snowpark session using the active notebook connection."
```

```python
# Cell 1 — Generated by Cortex Code
from snowflake.snowpark.context import get_active_session
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

session = get_active_session()
print("Session ready:", session.get_current_database())
```

```
Prompt (Cell 2): "Query the issuers and esg_disclosures tables. 
Return issuer_name, sector, esg_score, and avg sentiment_score. 
Convert to pandas."
```

```python
# Cell 2 — Generated by Cortex Code
df = session.sql("""
    SELECT
        i.issuer_name,
        i.sector,
        i.esg_score,
        ROUND(AVG(d.sentiment_score), 3) AS avg_sentiment
    FROM CORTEX_COURSE_DB.LABS.ISSUERS i
    JOIN CORTEX_COURSE_DB.LABS.ESG_DISCLOSURES d
      ON i.issuer_id = d.issuer_id
    GROUP BY i.issuer_name, i.sector, i.esg_score
    ORDER BY i.esg_score DESC
""").to_pandas()

print(df.shape)
df.head(10)
```

```
Prompt (Cell 3): "Plot a scatter chart of esg_score vs avg_sentiment, 
color-coded by sector, with issuer labels."
```

```python
# Cell 3 — Generated by Cortex Code
fig, ax = plt.subplots(figsize=(12, 7))

sectors = df["SECTOR"].unique()
colors  = plt.cm.Set2.colors

for i, sector in enumerate(sectors):
    mask = df["SECTOR"] == sector
    ax.scatter(
        df[mask]["ESG_SCORE"],
        df[mask]["AVG_SENTIMENT"],
        label=sector,
        color=colors[i % len(colors)],
        s=120,
        zorder=3
    )
    for _, row in df[mask].iterrows():
        ax.annotate(
            row["ISSUER_NAME"],
            (row["ESG_SCORE"], row["AVG_SENTIMENT"]),
            fontsize=8, ha="left", va="bottom"
        )

ax.set_xlabel("ESG Score", fontsize=12)
ax.set_ylabel("Avg Sentiment Score", fontsize=12)
ax.set_title("ESG Score vs Disclosure Sentiment by Sector", fontsize=14)
ax.legend(title="Sector", bbox_to_anchor=(1.01, 1), loc="upper left")
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()
```

### 5.3 Lab 5-B: Cortex Code CLI Workflow

```bash
# Install Cortex Code CLI
pip install snowflake-cli-labs

# Configure connection
snow connection add

# Open the Cortex Code interactive assistant
snow cortex code

# Example prompts in the CLI:
# > explain this error: invalid identifier 'ISSUER_NAME'
# > write a stored procedure that refreshes ESG scores daily
# > optimize this query for a large ESG disclosures table
# > generate a dbt model for the esg_analytics semantic layer
```

### 5.4 Lab 5-C: Stored Procedure from a Cortex Code Prompt

```
Prompt: "Write a Snowflake stored procedure called REFRESH_ESG_AI_SCORES 
that reads esg_disclosures, runs CORTEX.SENTIMENT on each row, 
and upserts results into esg_ai_enriched."
```

```sql
CREATE OR REPLACE PROCEDURE CORTEX_COURSE_DB.LABS.REFRESH_ESG_AI_SCORES()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS $$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, call_builtin
import snowflake.snowpark.functions as F

def run(session: Session) -> str:
    df = session.table("CORTEX_COURSE_DB.LABS.ESG_DISCLOSURES")

    df_scored = df.with_column(
        "AI_SENTIMENT_SCORE",
        call_builtin("SNOWFLAKE.CORTEX.SENTIMENT", col("SUMMARY_TEXT"))
    ).with_column(
        "AI_RISK_LABEL",
        call_builtin(
            "SNOWFLAKE.CORTEX.CLASSIFY_TEXT",
            col("SUMMARY_TEXT"),
            F.lit("['LOW_RISK','MEDIUM_RISK','HIGH_RISK','REGULATORY_CONCERN']")
        )["label"].cast("string")
    ).with_column(
        "REFRESHED_AT",
        F.current_timestamp()
    )

    df_scored.write.mode("overwrite").save_as_table(
        "CORTEX_COURSE_DB.LABS.ESG_AI_ENRICHED"
    )

    count = session.table("CORTEX_COURSE_DB.LABS.ESG_AI_ENRICHED").count()
    return f"Refreshed {count} rows in ESG_AI_ENRICHED"
$$;

-- Schedule it with a Task
CREATE OR REPLACE TASK CORTEX_COURSE_DB.LABS.DAILY_ESG_REFRESH
  WAREHOUSE = CORTEX_COURSE_WH
  SCHEDULE  = 'USING CRON 0 6 * * * UTC'
AS
  CALL CORTEX_COURSE_DB.LABS.REFRESH_ESG_AI_SCORES();

ALTER TASK CORTEX_COURSE_DB.LABS.DAILY_ESG_REFRESH RESUME;

-- Test it manually
CALL CORTEX_COURSE_DB.LABS.REFRESH_ESG_AI_SCORES();

SELECT * FROM CORTEX_COURSE_DB.LABS.ESG_AI_ENRICHED LIMIT 10;
```

### 5.5 Cortex Code Prompt Cheat Sheet

| Goal | Prompt Template |
|------|----------------|
| Generate SQL | `"Write SQL that [goal] using [table] in [db.schema]"` |
| Explain error | `"Explain this Snowflake error: [paste error]"` |
| Optimize query | `"Optimize this SQL for a [size] table: [paste SQL]"` |
| Write Snowpark | `"Write Snowpark Python that [transformation]"` |
| Generate a proc | `"Write a stored procedure that [task] using Python handler"` |
| Debug a pipeline | `"This Snowpark pipeline fails with [error]. Fix it: [code]"` |
| Write a task | `"Schedule [procedure] to run daily at 6am UTC using a Snowflake Task"` |
| Create semantic view | `"Generate a Semantic View YAML for these tables: [schema DDL]"` |

---

<a name="wrapup"></a>
## Wrap-Up — Q&A & Next Steps *(15 min)*

### Key Takeaways

1. **Security = Architecture**: Cortex AI keeps LLMs inside your Snowflake perimeter. Data never moves.
2. **Accuracy = Semantic Layer**: Build Semantic Views to get 92%+ text-to-SQL accuracy.
3. **Governance is automatic**: Masking and row access policies apply to every AI-generated query.
4. **Self-serve is a product**: Cortex Analyst turns your data warehouse into an interactive analytics product.
5. **Cortex Code accelerates everything**: Generate SQL, Snowpark, procedures, and semantic models from plain English.

### Your 30-Day Adoption Roadmap

| Week | Goal | Actions |
|------|------|---------|
| **Week 1** | Schema & Semantic View | Build Semantic View for one key domain |
| **Week 2** | Governance | Add masking + row access policies to AI-accessible tables |
| **Week 3** | Analyst App | Deploy a Streamlit Cortex Analyst app for one team |
| **Week 4** | Automate | Build Snowpark + Task pipeline for AI enrichment |

### Resources

- [Cortex Analyst Docs](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Semantic Views Reference](https://docs.snowflake.com/en/sql-reference/sql/create-semantic-view)
- [Cortex Functions Reference](https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex)
- [Cortex Code CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/cortex-code)
- [Snowpark Python](https://docs.snowflake.com/en/developer-guide/snowpark/python)
- [Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)

---

*© Snowflake Inc. — Course built with Cortex Code*
