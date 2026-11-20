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

/// The Extract service with the settings of the running production
ClassMethod Service(Output pService As Airbyte.Demo.Service.Extract) As %Status [ Private ]
{
    Quit ##class(Ens.Director).CreateBusinessService("Extract", .pService)
}

/// The connector's settings: required fields, optional fields, and which ones are secret
/// (secrets belong in the Credentials entry, see the CredentialFields setting)
ClassMethod Spec()
{
    Set tSC = ..Service(.tSvc)
    If $$$ISERR(tSC) {
        Do $System.Status.DisplayError(tSC)
        Quit
    }
    Try {
        Set tSpec = tSvc.Spec()
    } Catch ex {
        Write !,"spec failed: ",ex.AsStatus(),!
        Quit
    }
    Set tSchema = tSpec.%Get("connection_specification")
    Set tSecret = ""
    Set tIt = tSpec.%Get("secret_fields").%GetIterator()
    While tIt.%GetNext(.tI, .tPath) {
        Set tSecret(tPath) = 1
    }
    Write !,tSpec.%Get("connector")," ",tSpec.%Get("version"),!
    Set tReq = ""
    Set tIt = tSchema.%Get("required").%GetIterator()
    While tIt.%GetNext(.tI, .tName) {
        Set tReq(tName) = 1
    }
    Write !,"required:",!
    Set tIt = tSchema.%Get("properties").%GetIterator()
    While tIt.%GetNext(.tName, .tProp) {
        If $Data(tReq(tName)) {
            Write "  ",tName,?42,..TypeOf(tProp),$Select($Data(tSecret(tName)):"  SECRET", 1:""),!
        }
    }
    Write !,"optional:",!
    Set tIt = tSchema.%Get("properties").%GetIterator()
    While tIt.%GetNext(.tName, .tProp) {
        If '$Data(tReq(tName)) {
            Write "  ",tName,?42,..TypeOf(tProp),$Select($Data(tSecret(tName)):"  SECRET", 1:""),!
        }
    }
    Write !,"secret fields (set them through Credentials and CredentialFields, not in ConnectorConfig):",!
    Set tPath = ""
    For {
        Set tPath = $Order(tSecret(tPath))
        Quit:tPath=""
        Write "  ",tPath,!
    }
}

ClassMethod TypeOf(pProp As %DynamicObject) As %String [ Private ]
{
    Set tType = pProp.%Get("type")
    If $IsObject(tType) {
        Set tType = tType.%ToJSON()
    }
    If (tType = "") && pProp.%IsDefined("oneOf") {
        Set tType = "one of several options"
    }
    Quit tType
}

/// Validate the connection with the settings of the running production
ClassMethod Check()
{
    Set tSC = ..Service(.tSvc)
    If $$$ISERR(tSC) {
        Do $System.Status.DisplayError(tSC)
        Quit
    }
    Try {
        Set tResp = tSvc.PostJSON("/check", tSvc.SourceBody(), .tCode)
    } Catch ex {
        Write !,"check failed: ",ex.AsStatus(),!
        Quit
    }
    If (tCode = 200) && tResp.%Get("ok") {
        Write !,"check: ok",!
        Quit
    }
    Set tErr = $Select(tCode = 200:tResp.%Get("error"), 1:tResp.%Get("error"))
    Write !,"check failed (HTTP ",tCode,"): ",tErr.%Get("category")," - ",tErr.%Get("message"),!
    If tErr.%IsDefined("details") {
        Set tIt = tErr.%Get("details").%GetIterator()
        While tIt.%GetNext(.tI, .tD) {
            Write "  ",tD.%Get("path"),": ",tD.%Get("reason"),!
        }
    }
}

/// List the streams of the source: primary key, cursor, sync modes
ClassMethod Discover()
{
    Set tSC = ..Service(.tSvc)
    If $$$ISERR(tSC) {
        Do $System.Status.DisplayError(tSC)
        Quit
    }
    Try {
        Set tResp = tSvc.PostJSON("/discover", tSvc.SourceBody(), .tCode)
    } Catch ex {
        Write !,"discover failed: ",ex.AsStatus(),!
        Quit
    }
    If tCode '= 200 {
        Set tErr = tResp.%Get("error")
        Write !,"discover failed (HTTP ",tCode,"): ",tErr.%Get("category")," - ",tErr.%Get("message"),!
        Quit
    }
    Write !,"stream",?20,"primary key",?46,"cursor",?60,"sync modes",!
    Set tIt = tResp.%Get("streams").%GetIterator()
    While tIt.%GetNext(.tI, .tS) {
        Write tS.%Get("name"),?20,tS.%Get("primary_key").%ToJSON(),?46,tS.%Get("default_cursor_field").%ToJSON(),?60,tS.%Get("supported_sync_modes").%ToJSON(),!
    }
}

/// The runs and their counters
ClassMethod Runs()
{
    Write !
    Do ##class(%SQL.Statement).%ExecDirect(, "SELECT RunId, ConnectionId AS Conn, Status, RecordsRead AS Rd, Inserted AS Ins, Updated AS Upd, Unchanged AS Unch, Rejected AS Rej, RetryCount AS Retries, ErrorCategory AS ErrCat, DurationSeconds AS Secs, StoreSeconds AS StoreSecs FROM Airbyte_Demo_Data.Run ORDER BY ID").%Display()
}

/// Counts per stream for one run
ClassMethod RunDetail(pRunId As %Integer)
{
    Write !
    Do ##class(%SQL.Statement).%ExecDirect(, "SELECT RunId, Stream, RecordsRead AS Rd, Inserted AS Ins, Updated AS Upd, Unchanged AS Unch, Rejected AS Rej FROM Airbyte_Demo_Data.RunStream WHERE RunId = ? ORDER BY Stream", pRunId).%Display()
}

/// Where every connection stands: last run, last success, last error
ClassMethod Connections()
{
    Write !
    Do ##class(%SQL.Statement).%ExecDirect(, "SELECT ConnectionId, LastRunId AS LastRun, LastStatus, LastRunAt, LastSuccessRunId AS OkRun, LastSuccessAt AS OkAt, LastErrorCategory AS ErrCat, LastErrorMessage AS ErrMsg FROM Airbyte_Demo_Data.ConnectionStatus ORDER BY ConnectionId").%Display()
}

/// Rejected records (key missing or invalid). The payload is only shown with pPayload = 1.
/// Fix them at the source or change KeyFields; the next run reads them again.
ClassMethod Rejects(pPayload As %Boolean = 0)
{
    Write !
    Do ##class(%SQL.Statement).%ExecDirect(, "SELECT ID, RunId, BatchNo AS Batch, ConnectionId AS Conn, Stream, Status, Reason FROM Airbyte_Demo_Data.Rejected ORDER BY ID").%Display()
    If pPayload {
        Write !
        Set tRS = ##class(%SQL.Statement).%ExecDirect(, "SELECT ID FROM Airbyte_Demo_Data.Rejected ORDER BY ID")
        While tRS.%Next() {
            Set tObj = ##class(Airbyte.Demo.Data.Rejected).%OpenId(tRS.%Get("ID"))
            Write "payload ",tObj.%Id(),": ",tObj.Payload.Read(500),!
        }
    }
}

/// Mark a rejected record as Ignored (it stays in the table)
ClassMethod IgnoreRejected(pId As %Integer)
{
    Set tObj = ##class(Airbyte.Demo.Data.Rejected).%OpenId(pId)
    If '$IsObject(tObj) {
        Write !,"no rejected record ",pId,!
        Quit
    }
    Set tObj.Status = "Ignored"
    Write !,$Select($$$ISOK(tObj.%Save()):"rejected record "_pId_" marked Ignored", 1:"could not save"),!
}

/// The source state IRIS has stored (what the next read starts from)
ClassMethod State(pConnectionId As %String = "demo")
{
    Write !,$Get(^Airbyte.Demo.State(pConnectionId), "<none>"),!
}

/// Forget everything IRIS stored (records, runs, state). Also reset the source: demo/scenarios/00_reset_source.sql
/// Remove the records, runs, rejected records, status and saved state of ONE connection (other connections
/// are not touched). Use it to start a connection from scratch, for example before a benchmark.
ClassMethod ResetConnection(pConnectionId As %String)
{
    If pConnectionId = "" {
        Write !,"ResetConnection needs a connection id",!
        Quit
    }
    // runs go with their per-stream counts; the record and rejected tables hold the connection id themselves
    &sql(DELETE FROM Airbyte_Demo_Data.RunStream WHERE RunId IN (SELECT ID FROM Airbyte_Demo_Data.Run WHERE ConnectionId = :pConnectionId))
    &sql(DELETE FROM Airbyte_Demo_Data.Run WHERE ConnectionId = :pConnectionId)
    &sql(DELETE FROM Airbyte_Demo_Data.Record WHERE ConnectionId = :pConnectionId)
    &sql(DELETE FROM Airbyte_Demo_Data.Rejected WHERE ConnectionId = :pConnectionId)
    &sql(DELETE FROM Airbyte_Demo_Data.ConnectionStatus WHERE ConnectionId = :pConnectionId)
    Kill ^Airbyte.Demo.State(pConnectionId)
    Write !,"connection ",pConnectionId," reset: records, runs, rejected records, status and state cleared",!
}

ClassMethod Reset()
{
    Do ##class(Airbyte.Demo.Data.Record).%KillExtent()
    Do ##class(Airbyte.Demo.Data.Run).%KillExtent()
    Do ##class(Airbyte.Demo.Data.RunStream).%KillExtent()
    Do ##class(Airbyte.Demo.Data.ConnectionStatus).%KillExtent()
    Do ##class(Airbyte.Demo.Data.Rejected).%KillExtent()
    // %KillExtent leaves the stream data (record data, rejected payloads) behind: clear it as well
    Kill ^Airbyte.Demo.Data.RecordS, ^Airbyte.Demo.Data.RejectedS
    Kill ^Airbyte.Demo.State
    Write !,"IRIS side reset: records, runs and state cleared",!
}

}
