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
    Do ##class(%SQL.Statement).%ExecDirect(, "SELECT RunId, Status, RecordsRead AS Rd, Inserted AS Ins, Updated AS Upd, Unchanged AS Unch, Rejected AS Rej, RetryCount AS Retries, ErrorCategory AS ErrCat FROM Airbyte_Demo_Data.Run ORDER BY ID").%Display()
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
