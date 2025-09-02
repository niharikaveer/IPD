import csv
from neo4j import GraphDatabase

# ⚡ Step 1: Local Neo4j connection details
URI = "neo4j://127.0.0.1:7687"   # default local connection
USER = "neo4j"                  # your DB username
PASSWORD = "12345678"           # the password you set during setup

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# ⚡ Step 2: Load cases into Neo4j from CSV
def load_cases_into_neo4j(csv_file):
    with driver.session() as session:
        with open(csv_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                session.execute_write(create_case_graph, row)

# ⚡ Step 3: Define graph structure
def create_case_graph(tx, row):
    tx.run("""
        MERGE (c:Case {case_number: $CaseNumber})
        SET c.title = $CaseTitle,
            c.date_of_judgment = $DateOfJudgment,
            c.file_name = $FileName,
            c.decision_summary = $DecisionSummary,
            c.outcome = $Outcome,
            c.citations = $Citations

        MERGE (court:Court {name: $CourtName})
        MERGE (c)-[:HEARD_IN]->(court)

        FOREACH (judge IN split($Judges, ";") |
            MERGE (j:Judge {name: trim(judge)})
            MERGE (c)-[:JUDGED_BY]->(j)
        )

        FOREACH (petitioner IN split($Petitioners, ";") |
            MERGE (p:Party {name: trim(petitioner)})
            MERGE (c)-[:FILED_BY]->(p)
        )

        FOREACH (respondent IN split($Respondents, ";") |
            MERGE (r:Party {name: trim(respondent)})
            MERGE (c)-[:AGAINST]->(r)
        )

        FOREACH (issue IN split($LegalIssues, ";") |
            MERGE (i:LegalIssue {description: trim(issue)})
            MERGE (c)-[:ABOUT]->(i)
        )
    """,
    FileName=row["File Name"],
    CaseTitle=row["Case Title"],
    CourtName=row["Court Name"],
    DateOfJudgment=row["Date of Judgment"],
    CaseNumber=row["Case Number"],
    Judges=row["Judges"],
    Petitioners=row["Petitioner(s)"],
    Respondents=row["Respondent(s)"],
    LegalIssues=row["Legal Issues"],
    DecisionSummary=row["Decision Summary"],
    Outcome=row["Outcome"],
    Citations=row["Citations"])

# ⚡ Step 4: Run the loader
load_cases_into_neo4j("extracted_cases_clean.csv")
driver.close()
