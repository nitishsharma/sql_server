def validate_entitlements(sql_query: str, user_context: dict, enterprise_context: dict):
    """
    Validates if the user has entitlements to access the requested fields based on the SQL query.
    """
    # Entitlement logic here based on user roles and entitlements (e.g., Public, Confidential, etc.)
    if "highly_confidential" in sql_query.lower() and "Admin" not in user_context["roles"]:
        return False
    return True
