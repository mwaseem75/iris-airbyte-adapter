/// ResetConnection must clear one connection completely and leave every other connection alone.
/// Uses two connection ids of its own and removes what it wrote.
Class Airbyte.Test.ResetConnectionTest Extends %UnitTest.TestCase
{

Method OnAfterAllTests() As %Status
{
    Do ##class(Airbyte.Demo.Util).ResetConnection("unittest-a")
    Do ##class(Airbyte.Demo.Util).ResetConnection("unittest-b")
    Quit $$$OK
}

/// One run, one stream count, one record, one rejected record, one status row and a state for a connection
Method Fill(pConnectionId As %String) [ Private ]
{
    Set tRun = ##class(Airbyte.Demo.Data.Run).%New()
    Set tRun.ConnectionId = pConnectionId
    Set tRun.Status = "Completed"
    $$$ThrowOnError(tRun.%Save())
    Set tRunId = tRun.%Id()
    &sql(INSERT INTO Airbyte_Demo_Data.RunStream (RunId, Stream, RecordsRead) VALUES (:tRunId, 's', 1))
    Set tRec = ##class(Airbyte.Demo.Data.Record).%New()
    Set tRec.ConnectionId = pConnectionId
    Set tRec.Stream = "s"
    Set tRec.RecordKey = "[""1""]"
    Do tRec.Data.Write("{}")
    $$$ThrowOnError(tRec.%Save())
    $$$ThrowOnError(##class(Airbyte.Demo.Data.Rejected).Add(tRunId, 1, pConnectionId, "s", "test", "{}"))
    $$$ThrowOnError(##class(Airbyte.Demo.Data.ConnectionStatus).Record(pConnectionId, tRunId, "Completed", $ZDateTime($ZTimeStamp, 3, 1, 3)))
    Set ^Airbyte.Demo.State(pConnectionId) = "[]"
}

Method Count(pTable As %String, pConnectionId As %String) As %Integer [ Private ]
{
    Set tRS = ##class(%SQL.Statement).%ExecDirect(, "SELECT COUNT(*) FROM Airbyte_Demo_Data."_pTable_" WHERE ConnectionId = ?", pConnectionId)
    Do tRS.%Next()
    Quit tRS.%GetData(1)
}

Method TestOnlyTheNamedConnectionIsCleared()
{
    Do ..Fill("unittest-a")
    Do ..Fill("unittest-b")
    Do ##class(Airbyte.Demo.Util).ResetConnection("unittest-a")
    For tTable = "Run", "Record", "Rejected", "ConnectionStatus" {
        Do $$$AssertEquals(..Count(tTable, "unittest-a"), 0, tTable_" of the reset connection is empty")
        Do $$$AssertEquals(..Count(tTable, "unittest-b"), 1, tTable_" of the other connection is kept")
    }
    Do $$$AssertEquals($Data(^Airbyte.Demo.State("unittest-a")), 0, "state of the reset connection is gone")
    Do $$$AssertEquals($Data(^Airbyte.Demo.State("unittest-b")) > 0, 1, "state of the other connection is kept")
    &sql(SELECT COUNT(*) INTO :tStreams FROM Airbyte_Demo_Data.RunStream rs JOIN Airbyte_Demo_Data.Run r ON rs.RunId = r.ID WHERE r.ConnectionId = 'unittest-b')
    Do $$$AssertEquals(tStreams, 1, "per-stream counts of the other connection are kept")
}

}
