/// Short helper commands for the demo.
/// Terminal:  docker exec -i iris iris session IRIS -U USER "##class(Airbyte.Demo.Util).Records()"
Class Airbyte.Demo.Util
{

/// Is the production running and are the credentials there?
ClassMethod Status()
{
    Set tRunning = ##class(Ens.Director).IsProductionRunning(.tName)
    Write !,"production: ",$Select(tRunning:"RUNNING ("_tName_")", 1:"STOPPED"),!
    Write "credentials 'Postgres': ",$Select(##class(Ens.Config.Credentials).%ExistsId("Postgres"):"yes", 1:"MISSING"),!
    &sql(SELECT COUNT(*) INTO :tCount FROM Airbyte_Demo_Data.Record)
    Write "stored records: ",tCount,!
}

/// Stored records per stream, and the total
ClassMethod Records()
{
    Write !
    Do ##class(%SQL.Statement).%ExecDirect(, "SELECT Stream, COUNT(*) AS Records FROM Airbyte_Demo_Data.Record GROUP BY Stream ORDER BY Stream").%Display()
    Write !
    Do ##class(%SQL.Statement).%ExecDirect(, "SELECT COUNT(*) AS Total FROM Airbyte_Demo_Data.Record").%Display()
}

/// Every stored row with this key, across streams (isolation: same key, one row per stream)
ClassMethod Key(pKey As %String = "[""1""]")
{
    Set tRS = ##class(%SQL.Statement).%ExecDirect(, "SELECT ID, Stream, RecordKey, SUBSTR(Hash,1,12) AS Hash FROM Airbyte_Demo_Data.Record WHERE RecordKey = ? ORDER BY Stream", pKey)
    Write !,"stream",?14,"key",?24,"hash",?40,"data",!
    While tRS.%Next() {
        Set tRec = ##class(Airbyte.Demo.Data.Record).%OpenId(tRS.%Get("ID"))
        Write tRS.%Get("Stream"),?14,tRS.%Get("RecordKey"),?24,tRS.%Get("Hash"),?40,tRec.Data.Read(300),!
    }
}

/// The runs and their counters
ClassMethod Runs()
{
    Write !
    Do ##class(%SQL.Statement).%ExecDirect(, "SELECT RunId, Status, RecordsRead AS Rd, Inserted AS Ins, Updated AS Upd, Unchanged AS Unch, Rejected AS Rej FROM Airbyte_Demo_Data.Run ORDER BY ID").%Display()
}

/// The source state IRIS has stored (what the next read starts from)
ClassMethod State(pConnectionId As %String = "demo")
{
    Write !,$Get(^Airbyte.Demo.State(pConnectionId), "<none>"),!
}

/// Forget everything IRIS stored (records, runs, state). Also reset the source: demo/scenarios/00_reset_source.sql
ClassMethod Reset()
{
    Do ##class(Airbyte.Demo.Data.Record).%KillExtent()
    Do ##class(Airbyte.Demo.Data.Run).%KillExtent()
    Kill ^Airbyte.Demo.State
    Write !,"IRIS side reset: records, runs and state cleared",!
}

}
