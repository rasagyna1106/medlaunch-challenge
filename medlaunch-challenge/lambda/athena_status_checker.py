import boto3
import logging
import os

log = logging.getLogger()
log.setLevel(logging.INFO)

athena = boto3.client("athena")

def handler(event, context):
    query_execution_id = event.get("queryExecutionId")
    
    if not query_execution_id:
        raise ValueError("queryExecutionId missing from event")

    log.info("Checking status for QueryExecutionId: %s", query_execution_id)

    response = athena.get_query_execution(
        QueryExecutionId=query_execution_id
    )
    
    state = response["QueryExecution"]["Status"]["State"]
    state_change_reason = response["QueryExecution"]["Status"].get("StateChangeReason", "")

    log.info("Query %s is in state: %s", query_execution_id, state)

    return {
        "queryExecutionId": query_execution_id,
        "state": state,
        "stateChangeReason": state_change_reason,
        "succeeded": state == "SUCCEEDED",
        "failed": state in ["FAILED", "CANCELLED"]
    }
