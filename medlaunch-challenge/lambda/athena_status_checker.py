"""
lambda/athena_status_checker.py
=================================
Stage 4 supporting Lambda - Athena Query Status Checker

Purpose
-------
Called by the Step Functions state machine to poll the status of an
Athena query. Returns the current state so the state machine can
decide whether to wait longer, copy results, or trigger a failure alert.

This function is intentionally kept simple and single-purpose. The
polling logic lives in Step Functions (Wait state + Choice state),
not here. Keeping Lambda stateless and focused means it can be
reused in any workflow that needs to check Athena query status.

Input event
-----------
{
    "queryExecutionId": "abc-123-def-456"
}

Output
------
{
    "queryExecutionId": "abc-123-def-456",
    "state": "SUCCEEDED",
    "stateChangeReason": "",
    "succeeded": true,
    "failed": false
}
"""

import boto3
import logging

log = logging.getLogger()
log.setLevel(logging.INFO)

athena = boto3.client("athena")


def handler(event, context):
    query_execution_id = event.get("queryExecutionId")

    if not query_execution_id:
        log.error("queryExecutionId missing from event: %s", event)
        raise ValueError("queryExecutionId is required but was not found in the event payload")

    log.info("Checking Athena query status for ID: %s", query_execution_id)

    response = athena.get_query_execution(
        QueryExecutionId=query_execution_id
    )

    status = response["QueryExecution"]["Status"]
    state = status["State"]
    state_change_reason = status.get("StateChangeReason", "")

    log.info("Query %s current state: %s", query_execution_id, state)

    if state_change_reason:
        log.info("State change reason: %s", state_change_reason)

    return {
        "queryExecutionId": query_execution_id,
        "state": state,
        "stateChangeReason": state_change_reason,
        "succeeded": state == "SUCCEEDED",
        "failed": state in ["FAILED", "CANCELLED"]
    }
