import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

# Load credentials
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Connect to SQL
conn = pyodbc.connect(
    f"DRIVER={os.getenv('SQL_DRIVER')};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    f"UID={os.getenv('SQL_USERNAME')};"
    f"PWD={os.getenv('SQL_PASSWORD')}"
)
cursor = conn.cursor()

### PART 1: ROW-LEVEL GPT ANALYSIS ###
cursor.execute("""
    SELECT HashKey, ControlNo, ServiceType, Zone, BilledWeight, 
           NetAmount, ContractRate, PotentialError, TransactionDate
    FROM dbo.ups_base_rate_audit_sanitized
    WHERE GPT_Analysis IS NULL 
      AND (GPT_Reviewed = 0 OR GPT_Reviewed IS NULL)
      AND ROUND(PotentialError, 2) <> 0.00
    ORDER BY ABS(PotentialError) DESC
""")
rows = cursor.fetchall()

rows_reviewed = 0
controlno_used = None

for row in rows:
    hashkey, controlno, service, zone, weight, net, contract, error, txn_date = row
    controlno_used = controlno

    # Fetch close rate master candidates
    rate_query = """
    SELECT TOP 10 
        Zone, MinWeight, MaxWeight, Rate
    FROM dbo.ups_rate_master
    WHERE ServiceTypeDescription = ?
      AND ChildID = 12661
      AND Notes = 'Total'
      AND ? BETWEEN EffectiveDate AND ExpirationDate
      AND ABS(? - MinWeight) <= 100
    ORDER BY ABS(? - Rate) ASC
    """
    cursor.execute(rate_query, service, txn_date, weight, net)
    rate_rows = cursor.fetchall()

    rate_context = "\n".join(
        [f"- Zone {z}, {w1}-{w2} lbs, ${r}" for z, w1, w2, r in rate_rows]
    ) if rate_rows else "No nearby rate records found."

    prompt = f"""
We are auditing a UPS shipment for billing accuracy. Here is the record:

- Service Type: {service}
- Zone: {zone}
- Billed Weight: {weight} lbs
- Net Amount Charged: ${net}
- Contract Rate Used: ${contract}
- Potential Error: ${error}

Here are other nearby rates from the UPS rate master for the same service type, valid on the shipment date and within ±100 lbs:

{rate_context}

Does the Net Amount appear to match any of these alternate rates better than the one assigned? Could this be a mis-zoned or mis-weighed package? Respond in one sentence.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200
        )
        analysis = response.choices[0].message.content.strip()

        cursor.execute("""
            UPDATE dbo.ups_base_rate_audit_sanitized
            SET GPT_Analysis = ?, GPT_Reviewed = 1
            WHERE HashKey = ?
        """, (analysis, hashkey))
        conn.commit()
        rows_reviewed += 1
        print(f"✅ Row analyzed: {hashkey} - {analysis}")

    except Exception as e:
        print(f"❌ GPT row error for {hashkey}: {e}")

### PART 2: CONDITIONAL SUMMARY GPT ###
if rows_reviewed > 0:
    try:
        summary_df = pd.read_sql("""
            SELECT 
                ServiceType,
                Zone,
                BillToAccountNo,
                COUNT(*) AS NumErrors,
                AVG(PotentialError) AS AvgError,
                SUM(PotentialError) AS TotalError,
                AVG(BilledWeight) AS AvgWeight
            FROM dbo.ups_base_rate_audit_sanitized
            WHERE GPT_Analysis IS NOT NULL 
              AND ROUND(PotentialError, 2) <> 0.00
            GROUP BY ServiceType, Zone, BillToAccountNo
            ORDER BY TotalError DESC;
        """, conn)

        try:
            summary_table = summary_df.head(12).to_markdown(index=False)
        except ImportError:
            summary_table = summary_df.head(12).to_string(index=False)
            print("⚠️ Falling back to plain text summary due to missing 'tabulate'")

        summary_prompt = f"""
We are analyzing UPS shipment billing errors to identify where issues are most concentrated.

Here is a summary of the top 12 groupings by ServiceType, Zone, and Account Number, showing error counts and totals:

{summary_table}

What patterns stand out? Identify key problem areas in terms of service types, zones, weights, or specific accounts. Summarize in 2–3 sentences.
"""

        try:
            summary_response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": summary_prompt}],
                max_tokens=300
            )
            summary_text = summary_response.choices[0].message.content.strip()

            cursor.execute("""
                INSERT INTO dbo.gpt_ups_summary_analysis (ControlNo, AnalysisText, Timestamp)
                VALUES (?, ?, GETDATE())
            """, (controlno_used, summary_text))
            conn.commit()

            print("\n✅ Summary analysis complete:")
            print(summary_text)

        except Exception as e:
            print(f"❌ GPT summary error: {e}")

    except Exception as e:
        print(f"❌ Failed to generate summary DataFrame: {e}")
else:
    print("⚠️ No new rows reviewed — skipping summary analysis to conserve tokens.")

cursor.close()
conn.close()
