# query_optimizer.py

def check_indexed_filters(sql_query: str) -> bool:
    """
    Check if the query is using indexed filters.
    This is a simplified function that simulates checking if a query uses indexes.
    In a real-world scenario, this would involve query parsing and schema lookup.
    """
    # Example: Assume queries with 'WHERE' clauses on 'id' are indexed
    # You would replace this logic with real query analysis and schema information
    if "where" in sql_query.lower() and "id" in sql_query.lower():
        return True
    return False

def get_db_query_slo(sql_query: str) -> int:
    """
    Simulate getting the SLO (Service Level Objective) of a query.
    In real-world usage, this could check performance metrics, a query execution plan, or historical data.
    """
    # Simulated SLO based on query type
    # For example, joins might take longer, projections might be faster.
    if "join" in sql_query.lower():
        return 10  # 10 seconds for joins (exceeds SLO threshold)
    elif "group by" in sql_query.lower() or "sum(" in sql_query.lower():
        return 8  # Aggregation typically takes more time
    else:
        return 3  # Simple select queries should be fast
