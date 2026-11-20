/// A record the store did not accept because its key is missing or invalid. The payload is kept here
/// (not in logs) so it can be inspected. Fix it at the source, or change the KeyFields setting; the next
/// run reads the record again. Nothing here is deleted automatically.
Class Airbyte.Demo.Data.Rejected Extends %Persistent
{

Property RunId As %Integer;

Property BatchNo As %Integer;

Property ConnectionId As %String(MAXLEN = 100);

Property Stream As %String(MAXLEN = 200);

/// Why the key was flagged (never a value)
Property Reason As %String(MAXLEN = 500);

/// The record data as JSON
Property Payload As %Stream.GlobalCharacter;

/// New, Ignored
Property Status As %String(VALUELIST = ",New,Ignored") [ InitialExpression = "New" ];

Property CreatedAt As %TimeStamp;

Index RunIdx On RunId;

Index ConnIdx On (ConnectionId, Stream);

/// Add a rejected record. Call inside the batch transaction.
ClassMethod Add(pRunId As %Integer, pBatchNo As %Integer, pConnectionId As %String, pStream As %String, pReason As %String, pPayload As %String) As %Status
{
    Set tObj = ..%New()
    Set tObj.RunId = pRunId
    Set tObj.BatchNo = pBatchNo
    Set tObj.ConnectionId = pConnectionId
    Set tObj.Stream = pStream
    Set tObj.Reason = $Extract(pReason, 1, 500)
    Do tObj.Payload.Write(pPayload)
    Set tObj.CreatedAt = $ZDateTime($ZTimeStamp, 3, 1, 3)
    Quit tObj.%Save()
}

}
