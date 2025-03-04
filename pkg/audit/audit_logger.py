import json
import os
import time

# log file to store all the user related logs
AUDIT_LOG_FILE = './audit.json'


# This function can be called anywhere a login, logout or other activity occurs.
def createAuditEvent(username, actionString, extraMsg, epochTimestampSec, orgId):
    log_entry = {
        "username": username,
        "actionString": actionString,
        "extraMsg": extraMsg,
        "epochTimestampSec": epochTimestampSec,
        "orgId": orgId
    }
    
    with open(AUDIT_LOG_FILE, "a") as f:
        f.write(json.dumps(log_entry) + "\n")

# This function prints out all the logs which is stored in audit.json file.
def read_audit_events(orgId, startEpochSec, endEpochSec):
    logs = []
    if not os.path.exists(AUDIT_LOG_FILE):
        return logs

    with open(AUDIT_LOG_FILE, "r") as f:
        for line in f:
            entry = json.loads(line)
            if entry["orgId"] == orgId and startEpochSec <= entry["epochTimestampSec"] <= endEpochSec:
                logs.append(entry)

    return logs

""" 
Improvement Ideas

Ideally, the logs can be stored in a cloud based storage such as S3 or a No-SQL database such as DynamoDB.


"""