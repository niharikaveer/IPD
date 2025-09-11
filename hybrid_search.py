# hybrid_search.py
import chromadb
from sentence_transformers import SentenceTransformer
from neo4j import GraphDatabase

NEO4J_URI = "neo4j://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

PERSIST_DIR = "chroma_index"

# 1. Load Chroma
client = chromadb.PersistentClient(path=PERSIST_DIR)
collection = client.get_or_create_collection(name="legal_cases")

# 2. Load embedding model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

def hybrid_search(query, court=None, start_date=None, end_date=None, top_k=3):
    # Prepare filter dictionary for Chroma
    filters = {}
    if court:
        filters["court"] = court
    if start_date or end_date:
        # store dates in metadata as YYYY-MM-DD, so string comparison works
        date_filter = {}
        if start_date:
            date_filter["$gte"] = start_date
        if end_date:
            date_filter["$lte"] = end_date
        filters["date"] = date_filter

    print("\nFilters applied:", filters if filters else "None")

    # Encode query
    query_embedding = model.encode([query]).tolist()

    # Search in Chroma
    results = collection.query(
        query_embeddings=query_embedding,
        n_results=top_k,
        where=filters if filters else None
    )

    # Display results
    for i in range(len(results["ids"][0])):
        print("\n--- Result", i+1, "---")
        print("Case Title:", results["metadatas"][0][i].get("case_title"))
        print("Court:", results["metadatas"][0][i].get("court"))
        print("Date:", results["metadatas"][0][i].get("date"))
        print("Case Number:", results["metadatas"][0][i].get("case_number"))
        print("Local Path:", results["metadatas"][0][i].get("local_path"))
        print("Chunk Preview:", results["documents"][0][i][:300], "...")
        print("---------------------")

def neo4j_search(query, court=None, start_date=None, end_date=None, top_k=3):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    cypher = """
    MATCH (c:Case)-[:HEARD_IN]->(court:Court)
    WHERE (
    toLower(c.decision_summary) CONTAINS toLower($query)
    OR toLower(c.outcome) CONTAINS toLower($query)
    OR toLower(c.citations) CONTAINS toLower($query)
    )
    {court_filter}
    {date_filter}
    RETURN c, court
    ORDER BY c.date_of_judgment DESC
    LIMIT $top_k
    """

    court_filter = ""
    date_filter = ""
    params = {"query": query, "top_k": top_k}

    if court:
        court_filter = "AND toLower(court.name) = toLower($court)"
        params["court"] = court
    if start_date:
        date_filter += "AND c.date_of_judgment >= $start_date "
        params["start_date"] = start_date
    if end_date:
        date_filter += "AND c.date_of_judgment <= $end_date "
        params["end_date"] = end_date

    cypher = cypher.format(
        court_filter=court_filter,
        date_filter=date_filter
    )

    with driver.session() as session:
        results = session.run(cypher, parameters=params)
        for record in results:
            case = record["c"]
            court_node = record["court"]
            print("\n--- Neo4j Result ---")
            print("Case Title:", case.get("title"))
            print("Court:", court_node.get("name"))
            print("Date:", case.get("date_of_judgment"))
            print("Case Number:", case.get("case_number"))
            print("Summary:", case.get("decision_summary", "")[:300], "...")
            print("---------------------")
    driver.close()

if __name__ == "__main__":
    user_query = input("Enter your legal query: ").strip()
    court_name = input("Filter by court (leave blank for all): ").strip() or None
    start_date = input("Start date (YYYY-MM-DD, leave blank for none): ").strip() or None
    end_date = input("End date (YYYY-MM-DD, leave blank for none): ").strip() or None

    hybrid_search(user_query, court=court_name, start_date=start_date, end_date=end_date)
    neo4j_search(user_query,court=court_name,start_date=start_date,end_date=end_date)
