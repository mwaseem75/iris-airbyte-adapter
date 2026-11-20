/// Tests for the Store operation's batch handling (rejected records, per-stream counts) and the
/// connection status. Test1 and Test2 build on each other (they run in name order). Uses its own connection id and removes what it wrote.
Class Airbyte.Test.StoreTest Extends %UnitTest.TestCase
{

Property RunId As %Integer;

Method OnBeforeAllTests() As %Status
{
    Set tRun = ##class(Airbyte.Demo.Data.Run).%New()
    Set tRun.ConnectionId = "unittest"
    Set tRun.Status = "Running"
    Set tSC = tRun.%Save()
    Set ..RunId = tRun.%Id()
    Quit tSC
}

Method OnAfterAllTests() As %Status
{
    Set tRunId = ..RunId
    &sql(DELETE FROM Airbyte_Demo_Data.Record WHERE ConnectionId = 'unittest')
    &sql(DELETE FROM Airbyte_Demo_Data.Rejected WHERE ConnectionId = 'unittest')
    &sql(DELETE FROM Airbyte_Demo_Data.RunStream WHERE RunId = :tRunId)
    &sql(DELETE FROM Airbyte_Demo_Data.Run WHERE ID = :tRunId)
    &sql(DELETE FROM Airbyte_Demo_Data.ConnectionStatus WHERE ConnectionId = 'unittest')
    Quit $$$OK
}

Method Batch(pRecords As %String, pNo As %Integer) As Airbyte.Demo.Msg.Batch [ Private ]
{
    Set tMsg = ##class(Airbyte.Demo.Msg.Batch).%New()
    Set tMsg.RunId = ..RunId
    Set tMsg.ConnectionId = "unittest"
    Set tMsg.Stream = "customers"
    Set tMsg.BatchNo = pNo
    Do tMsg.Records.Write(pRecords)
    Quit tMsg
}

Method Test1RejectedRecordIsStoredWithItsPayload()
{
    Set tRecs = "[{""stream"":""customers"",""key"":[""1""],""data"":{""id"":1,""name"":""A""}},"
        _"{""stream"":""customers"",""key"":null,""key_error"":""missing key field: email"",""data"":{""id"":4,""email"":null}}]"
    Set tCounts = ##class(Airbyte.Demo.Operation.Store).ProcessBatch(..Batch(tRecs, 1))
    Do $$$AssertEquals(tCounts, "1,0,0,1", "one inserted, one rejected")
    &sql(SELECT COUNT(*), MAX(BatchNo) INTO :n, :batch FROM Airbyte_Demo_Data.Rejected WHERE ConnectionId = 'unittest')
    Do $$$AssertEquals(n, 1, "one rejected row")
    Do $$$AssertEquals(batch, 1, "batch number kept")
    Set tId = ""
    &sql(SELECT MAX(ID) INTO :tId FROM Airbyte_Demo_Data.Rejected WHERE ConnectionId = 'unittest')
    Set tObj = ##class(Airbyte.Demo.Data.Rejected).%OpenId(tId)
    Do $$$AssertEquals(tObj.Reason, "missing key field: email", "reason kept")
    Do $$$AssertEquals(tObj.Status, "New", "status New")
    Do $$$AssertEquals(##class(%DynamicAbstractObject).%FromJSON(tObj.Payload).%Get("id"), 4, "payload kept as JSON")
    &sql(SELECT COUNT(*) INTO :tStored FROM Airbyte_Demo_Data.Record WHERE ConnectionId = 'unittest')
    Do $$$AssertEquals(tStored, 1, "the valid record is stored, the rejected one is not")
}

Method Test2CountsPerRunAndPerStream()
{
    Set tRunId = ..RunId
    Set tRecs = "[{""stream"":""customers"",""key"":[""1""],""data"":{""id"":1,""name"":""A""}},"
        _"{""stream"":""customers"",""key"":[""2""],""data"":{""id"":2,""name"":""B""}}]"
    Set tCounts = ##class(Airbyte.Demo.Operation.Store).ProcessBatch(..Batch(tRecs, 2))
    Do $$$AssertEquals(tCounts, "1,0,1,0", "key 1 unchanged, key 2 inserted")
    &sql(SELECT RecordsRead, Inserted, Unchanged, Rejected INTO :rd, :ins, :unch, :rej FROM Airbyte_Demo_Data.RunStream WHERE RunId = :tRunId AND Stream = 'customers')
    Do $$$AssertEquals(rd, 4, "read adds up over batches (2 + 2)")
    Do $$$AssertEquals(ins, 2, "inserted adds up (1 + 1)")
    Do $$$AssertEquals(unch, 1, "unchanged")
    Do $$$AssertEquals(rej, 1, "rejected")
    &sql(SELECT Inserted, Rejected INTO :rins, :rrej FROM Airbyte_Demo_Data.Run WHERE ID = :tRunId)
    Do $$$AssertEquals(rins, 2, "Run counters agree")
    Do $$$AssertEquals(rrej, 1, "Run rejected agrees")
}

Method TestFailedBatchLeavesNothing()
{
    &sql(SELECT COUNT(*) INTO :before FROM Airbyte_Demo_Data.Rejected WHERE ConnectionId = 'unittest')
    Set tThrown = 0
    Try {
        Do ##class(Airbyte.Demo.Operation.Store).ProcessBatch(..Batch("{not json", 3))
    } Catch ex {
        Set tThrown = 1
    }
    Do $$$AssertEquals(tThrown, 1, "bad batch throws")
    Do $$$AssertEquals($TLevel, 0, "no transaction left open")
}

/// The worker sends the hash of each record: Store only compares it with the stored one
Method Test3HashFromTheWorkerDecidesChangedOrUnchanged()
{
    Set tRec = "[{""stream"":""customers"",""key"":[""h1""],""hash"":""aaa"",""data"":{""id"":9,""v"":1}}]"
    Do $$$AssertEquals(##class(Airbyte.Demo.Operation.Store).ProcessBatch(..Batch(tRec, 10)), "1,0,0,0", "new record inserted")
    Do $$$AssertEquals(##class(Airbyte.Demo.Operation.Store).ProcessBatch(..Batch(tRec, 11)), "0,0,1,0", "same hash: unchanged")
    Set tChanged = "[{""stream"":""customers"",""key"":[""h1""],""hash"":""bbb"",""data"":{""id"":9,""v"":2}}]"
    Do $$$AssertEquals(##class(Airbyte.Demo.Operation.Store).ProcessBatch(..Batch(tChanged, 12)), "0,1,0,0", "other hash: updated")
    Set tRec2 = ##class(Airbyte.Demo.Data.Record).RecordIdxOpen("unittest", "customers", "[""h1""]")
    Do $$$AssertEquals(tRec2.Hash, "bbb", "the hash from the worker is the stored hash")
    Do $$$AssertEquals(##class(%DynamicAbstractObject).%FromJSON(tRec2.Data.Read(1000)).%Get("v"), 2, "data was replaced")
    Set tRec2 = ""
    Do $$$AssertEquals(##class(Airbyte.Demo.Operation.Store).ProcessBatch(..Batch(tChanged, 13)), "0,0,1,0", "and unchanged again")
}

Method TestConnectionStatus()
{
    Set tSC = ##class(Airbyte.Demo.Data.ConnectionStatus).Record("unittest", 1, "Completed", "2026-11-20 10:00:00")
    Set tSC = ##class(Airbyte.Demo.Data.ConnectionStatus).Record("unittest", 2, "Failed", "2026-11-20 11:00:00", "unreachable", "worker not reachable")
    Set tObj = ##class(Airbyte.Demo.Data.ConnectionStatus).ConnIdxOpen("unittest")
    Do $$$AssertEquals(tObj.LastRunId, 2, "last run")
    Do $$$AssertEquals(tObj.LastStatus, "Failed", "last status")
    Do $$$AssertEquals(tObj.LastSuccessRunId, 1, "last success stays")
    Do $$$AssertEquals(tObj.LastErrorCategory, "unreachable", "error category")
    Set tSC = ##class(Airbyte.Demo.Data.ConnectionStatus).Record("unittest", 3, "Skipped", "2026-11-20 12:00:00")
    Set tObj = ##class(Airbyte.Demo.Data.ConnectionStatus).ConnIdxOpen("unittest")
    Do $$$AssertEquals(tObj.LastErrorCategory, "unreachable", "a skipped run does not overwrite the last error")
    Do $$$AssertEquals(tObj.LastStatus, "Skipped", "but it is the last run")
}

}
