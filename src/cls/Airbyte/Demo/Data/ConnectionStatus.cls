/// One row per connection: how the last run ended and when the last successful load was.
/// Updated at the end of every run.
Class Airbyte.Demo.Data.ConnectionStatus Extends %Persistent
{

Property ConnectionId As %String(MAXLEN = 100) [ Required ];

Property LastRunId As %Integer;

Property LastStatus As %String(MAXLEN = 20);

Property LastRunAt As %TimeStamp;

Property LastSuccessRunId As %Integer;

Property LastSuccessAt As %TimeStamp;

/// Category and message of the last run that ended with an error (kept after a later success)
Property LastErrorCategory As %String(MAXLEN = 30);

Property LastErrorMessage As %String(MAXLEN = 500);

Index ConnIdx On ConnectionId [ Unique ];

/// Record how a run ended. Never stores record data or credentials.
ClassMethod Record(pConnectionId As %String, pRunId As %Integer, pStatus As %String, pEnded As %TimeStamp, pCategory As %String = "", pMessage As %String = "") As %Status
{
    Set tObj = ..ConnIdxOpen(pConnectionId)
    If '$IsObject(tObj) {
        Set tObj = ..%New()
        Set tObj.ConnectionId = pConnectionId
    }
    Set tObj.LastRunId = pRunId
    Set tObj.LastStatus = pStatus
    Set tObj.LastRunAt = pEnded
    If pStatus = "Completed" {
        Set tObj.LastSuccessRunId = pRunId
        Set tObj.LastSuccessAt = pEnded
    } ElseIf (pStatus '= "Skipped") {
        Set tObj.LastErrorCategory = pCategory
        Set tObj.LastErrorMessage = pMessage
    }
    Quit tObj.%Save()
}

}
