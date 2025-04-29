from airflow.decorators import task
import logging

@task
def reset_knowledge_graph(neo4j_conn_id: str = 'neo4j_default'):
    """
    Task to reset Neo4j knowledge graph database.
    
    Args:
        neo4j_conn_id: Connection ID for Neo4j
    
    Returns:
        Dict with operation results
    """
    from hooks.neo4j_hook import Neo4jHook
    
    logger = logging.getLogger(__name__)
    hook = Neo4jHook(neo4j_conn_id)
    
    logger.info("Resetting Neo4j knowledge graph database")
    # Run cleanup query
    hook.run_query("MATCH (n) DETACH DELETE n")
    # Re-create constraints
    hook.setup_constraints()
    logger.info("Knowledge graph reset complete")
    return {"status": "success", "operation": "reset"}

@task
def get_knowledge_graph_stats(neo4j_conn_id: str = 'neo4j_default'):
    """
    Task to retrieve Neo4j knowledge graph statistics.
    
    Args:
        neo4j_conn_id: Connection ID for Neo4j
    
    Returns:
        Dict with graph statistics
    """
    from hooks.neo4j_hook import Neo4jHook
    
    logger = logging.getLogger(__name__)
    hook = Neo4jHook(neo4j_conn_id)
    
    logger.info("Retrieving knowledge graph statistics")
    
    # Get document count
    doc_count = hook.run_query("MATCH (d:Document) RETURN count(d) AS count")[0]['count']
    
    # Get entity count
    entity_count = hook.run_query("MATCH (e:Entity) RETURN count(e) AS count")[0]['count']
    
    # Get entity types
    entity_types = hook.run_query("""
    MATCH (e:Entity)
    RETURN e.type AS type, count(e) AS count
    ORDER BY count DESC
    """)
    
    # Get relationship count
    rel_count = hook.run_query("""
    MATCH ()-[r]->() 
    WHERE type(r) <> 'REFERS_TO' AND type(r) <> 'APPEARS_IN'
    RETURN count(r) AS count
    """)[0]['count']
    
    # Get relationship types
    rel_types = hook.run_query("""
    MATCH ()-[r]->() 
    WHERE type(r) <> 'REFERS_TO' AND type(r) <> 'APPEARS_IN'
    RETURN type(r) AS type, count(r) AS count
    ORDER BY count DESC
    """)
    
    # Get mention count
    mention_count = hook.run_query("MATCH (m:Mention) RETURN count(m) AS count")[0]['count']
    
    # Format entity types as dict
    entity_types_dict = {item['type']: item['count'] for item in entity_types}
    
    # Format relationship types as dict
    rel_types_dict = {item['type']: item['count'] for item in rel_types}
    
    stats = {
        "document_count": doc_count,
        "entity_count": entity_count,
        "entity_types": entity_types_dict,
        "relationship_count": rel_count,
        "relationship_types": rel_types_dict,
        "mention_count": mention_count
    }
    
    logger.info(f"Knowledge Graph Statistics: {stats}")
    return stats