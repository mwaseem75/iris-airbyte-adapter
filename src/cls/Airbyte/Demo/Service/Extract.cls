Include Ensemble

/// Prototype. Business Service that drives the worker and hands each batch to the Store operation.
///
/// - Polling is OFF by default (PollingEnabled=0); a run is started with ##class(Airbyte.Demo.Service.Extract).RunNow().
/// - One run per ConnectionId at a time (lock). If another run is active, or the worker answers 409,
///   the run is logged as Skipped and stops; there is no retry.
/// - The source state goes to the worker with each read and is saved in ^Airbyte.Demo.State(ConnectionId)
///   ONLY after every batch has been returned OK by the operation.
/// - Credentials come from an Ens.Config.Credentials entry. Neither credentials nor record data are logged.
Class Airbyte.Demo.Service.Extract Extends Ens.BusinessService
{

Parameter ADAPTER = "Ens.InboundAdapter";

Parameter SETTINGS = "WorkerURL:Connection,ConnectionId:Connection,Streams:Connection,CursorFields:Connection,Mode:Connection,BatchSize:Connection,ConnectorVersion:Connection,SourceHost:Connection,SourcePort:Connection,SourceDatabase:Connection,Credentials:Connection:credentialsSelector,ReadTimeout:Connection,PollingEnabled:Basic,Target:Basic";

/// Base URL of the worker, as seen from IRIS
Property WorkerURL As %String(MAXLEN = 300) [ InitialExpression = "http://worker:8000" ];

Property ConnectionId As %String(MAXLEN = 100) [ InitialExpression = "demo" ];

/// Comma separated stream names
Property Streams As %String(MAXLEN = 500) [ InitialExpression = "customers,order_lines,suppliers" ];

/// Comma separated stream=cursor_field pairs. A stream without a cursor is read as full_refresh.
Property CursorFields As %String(MAXLEN = 500) [ InitialExpression = "customers=updated_at,suppliers=updated_at" ];

Property Mode As %String(VALUELIST = ",full_refresh,incremental") [ InitialExpression = "incremental" ];

Property BatchSize As %Integer [ InitialExpression = 500 ];

Property ConnectorVersion As %String [ InitialExpression = "3.8.11" ];

/// Host of the source as seen from the connector container (it is not on the compose network)
Property SourceHost As %String(MAXLEN = 200) [ InitialExpression = "host.docker.internal" ];

Property SourcePort As %Integer [ InitialExpression = 5433 ];

Property SourceDatabase As %String [ InitialExpression = "demo" ];

/// Ens.Config.Credentials entry holding the source user and password
Property Credentials As %String [ InitialExpression = "Postgres" ];

/// HTTP timeout in seconds for the worker calls (a read takes about 25 s)
Property ReadTimeout As %Integer [ InitialExpression = 120 ];

/// 0 = runs only start with RunNow()
Property PollingEnabled As %Boolean [ InitialExpression = 0 ];

/// Business Operation that stores the batches
Property Target As %String [ InitialExpression = "Store" ];

Method OnProcessInput(pInput As %RegisteredObject, Output pOutput As %RegisteredObject) As %Status
{
    Set tManual = $IsObject(pInput)
    If 'tManual && '..PollingEnabled {
        Quit $$$OK
    }
    Quit ..Run(tManual && (pInput.StringValue = "verbose"))
}

/// Start one run now (production must be running). Prints a summary.
ClassMethod RunNow(pVerbose As %Boolean = 1) As %Status
{
    Set tSC = ##class(Ens.Director).CreateBusinessService("Extract", .tService)
    If $$$ISERR(tSC) {
        Quit tSC
    }
    Set tInput = ##class(Ens.StringContainer).%New($Select(pVerbose:"verbose", 1:"quiet"))
    Quit tService.ProcessInput(tInput, .tOutput)
}

Method Run(pVerbose As %Boolean = 0) As %Status
{
    Set tConn = ..ConnectionId
    Lock +^Airbyte.Demo.Lock(tConn):0
    If '$Test {
        $$$LOGWARNING("a run is already active for connection "_tConn_"; skipped")
        If pVerbose {
            Write !,"Run skipped: another run is active for connection ",tConn,!
        }
        Quit $$$OK
    }
    Set tSC = ..DoRun(pVerbose)
    Lock -^Airbyte.Demo.Lock(tConn)
    Quit tSC
}

Method DoRun(pVerbose As %Boolean) As %Status
{
    Set tRun = ##class(Airbyte.Demo.Data.Run).%New()
    Set tRun.ConnectionId = ..ConnectionId
    Set tRun.Started = $ZDateTime($ZTimeStamp, 3, 1, 3)
    Set tRun.Status = "Running"
    Set tSC = tRun.%Save()
    If $$$ISERR(tSC) {
        Quit tSC
    }
    Set tRunId = tRun.%Id()
    Set tRun = ""
    Set tRead = 0, tStatus = "Completed", tMessage = "", tSC = $$$OK
    Try {
        Set tRead = ..Extract(tRunId, pVerbose)
    } Catch ex {
        Set tErr = ex.AsStatus()
        Set tMessage = $Extract($System.Status.GetOneStatusText(tErr), 1, 500)
        If ex.Name = "WorkerBusy" {
            Set tStatus = "Skipped"
            $$$LOGWARNING("worker is busy (409); run "_tRunId_" skipped, no retry")
        } Else {
            Set tStatus = "Failed"
            Set tSC = tErr
            $$$LOGERROR("run "_tRunId_" failed: "_tMessage)
        }
    }
    Set tEnded = $ZDateTime($ZTimeStamp, 3, 1, 3)
    &sql(UPDATE Airbyte_Demo_Data.Run SET Status = :tStatus, Ended = :tEnded, RecordsRead = :tRead, Message = :tMessage WHERE ID = :tRunId)
    If pVerbose {
        &sql(SELECT RecordsRead, Inserted, Updated, Unchanged, Rejected INTO :r, :i, :u, :n, :j FROM Airbyte_Demo_Data.Run WHERE ID = :tRunId)
        Write "run ",tRunId,": ",tStatus," read=",r," inserted=",i," updated=",u," unchanged=",n," rejected=",j
        If tMessage '= "" {
            Write " (",tMessage,")"
        }
        Write !
    }
    Quit tSC
}

/// Calls the worker, sends every batch to the operation, then saves the new state. Throws on any failure.
/// Returns the number of records read.
Method Extract(pRunId As %Integer, pVerbose As %Boolean) As %Integer
{
    Set tConn = ..ConnectionId
    $$$ThrowOnError(##class(Ens.Config.Credentials).GetCredentialsObj(.tCred, $ClassName(), "Ens.Config.Credentials", ..Credentials))

    Set tBody = {"connector":"source-postgres", "version":(..ConnectorVersion), "mode":(..Mode), "batch_size":(+..BatchSize)}
    Set tBody.config = {"host":(..SourceHost), "port":(+..SourcePort), "database":(..SourceDatabase), "username":(tCred.Username), "password":(tCred.Password), "schemas":["public"], "ssl_mode":{"mode":"disable"}, "replication_method":{"method":"Standard"}, "tunnel_method":{"tunnel_method":"NO_TUNNEL"}}
    Set tStreams = []
    Set tList = $ListFromString(..Streams, ",")
    For i = 1:1:$ListLength(tList) {
        Set tName = $ZStrip($List(tList, i), "<>W")
        Set tSpec = {"name":(tName)}
        Set tCursor = ..CursorFor(tName)
        If tCursor '= "" {
            Do tSpec.%Set("cursor_field", tCursor)
        }
        Do tStreams.%Push(tSpec)
    }
    Set tBody.streams = tStreams

    Set tStateBefore = $Get(^Airbyte.Demo.State(tConn))
    If tStateBefore '= "" {
        Set tBody.state = ##class(%DynamicAbstractObject).%FromJSON(tStateBefore)
    }
    If pVerbose {
        Write !,"state BEFORE run: ",$Select(tStateBefore = "":"<none>", 1:tStateBefore),!
    }

    // 1. read (synchronous, about 25 s)
    Set tReq = ..NewRequest()
    Set tReq.ContentType = "application/json"
    Do tReq.EntityBody.Write(tBody.%ToJSON())
    $$$ThrowOnError(tReq.Post("/reads"))
    Set tCode = tReq.HttpResponse.StatusCode
    Set tResp = ##class(%DynamicAbstractObject).%FromJSON(tReq.HttpResponse.Data)
    If tCode = 409 {
        Throw ##class(%Exception.General).%New("WorkerBusy", 409, , "worker is busy (409)")
    }
    If tCode '= 200 {
        Throw ##class(%Exception.General).%New("WorkerError", tCode, , "worker returned "_tCode_" "_tResp.error.category_": "_tResp.error.message)
    }
    Set tReadId = tResp.%Get("read_id")
    Set tBatches = tResp.%Get("batches")
    Set tRead = 0
    Set tIt = tResp.counts.%GetIterator()
    While tIt.%GetNext(.tStream, .tCount) {
        Set tRead = tRead + tCount
        Set tRows(tStream, "read") = tCount
    }

    // 2. every batch, one message per stream, synchronous; any failure throws and the state is NOT saved
    For n = 1:1:tBatches {
        Set tGet = ..NewRequest()
        $$$ThrowOnError(tGet.Get("/reads/"_tReadId_"/batches/"_n))
        If tGet.HttpResponse.StatusCode '= 200 {
            Throw ##class(%Exception.General).%New("WorkerError", tGet.HttpResponse.StatusCode, , "batch "_n_" could not be fetched")
        }
        Set tBatch = ##class(%DynamicAbstractObject).%FromJSON(tGet.HttpResponse.Data)
        Kill tPerStream
        Set tIt = tBatch.records.%GetIterator()
        While tIt.%GetNext(.tIdx, .tRec) {
            Set tS = tRec.%Get("stream")
            If '$Data(tPerStream(tS)) {
                Set tPerStream(tS) = []
            }
            Do tPerStream(tS).%Push(tRec)
        }
        Set tS = ""
        For {
            Set tS = $Order(tPerStream(tS))
            Quit:tS=""
            Set tMsg = ##class(Airbyte.Demo.Msg.Batch).%New()
            Set tMsg.RunId = pRunId
            Set tMsg.ConnectionId = tConn
            Set tMsg.Stream = tS
            Set tMsg.ReadId = tReadId
            Set tMsg.BatchNo = n
            Do tPerStream(tS).%ToJSON(tMsg.Records)
            Set tRsp = ""
            $$$ThrowOnError(..SendRequestSync(..Target, tMsg, .tRsp))
            If $IsObject(tRsp) {
                Set tParts = $ListFromString(tRsp.StringValue, ",")
                Set tRows(tS, "inserted") = $Get(tRows(tS, "inserted")) + $List(tParts, 1)
                Set tRows(tS, "updated") = $Get(tRows(tS, "updated")) + $List(tParts, 2)
                Set tRows(tS, "unchanged") = $Get(tRows(tS, "unchanged")) + $List(tParts, 3)
                Set tRows(tS, "rejected") = $Get(tRows(tS, "rejected")) + $List(tParts, 4)
            }
        }
    }

    // 3. all batches returned OK: only now fetch and save the new state
    Set tSGet = ..NewRequest()
    $$$ThrowOnError(tSGet.Get("/reads/"_tReadId_"/state"))
    If tSGet.HttpResponse.StatusCode '= 200 {
        Throw ##class(%Exception.General).%New("WorkerError", tSGet.HttpResponse.StatusCode, , "state could not be fetched")
    }
    Set tNewState = ##class(%DynamicAbstractObject).%FromJSON(tSGet.HttpResponse.Data).%Get("state").%ToJSON()
    Set ^Airbyte.Demo.State(tConn) = tNewState
    If pVerbose {
        Set tStream = ""
        For {
            Set tStream = $Order(tRows(tStream))
            Quit:tStream=""
            Write "  ",tStream,": read=",$Get(tRows(tStream, "read"), 0)," inserted=",$Get(tRows(tStream, "inserted"), 0)," updated=",$Get(tRows(tStream, "updated"), 0)," unchanged=",$Get(tRows(tStream, "unchanged"), 0)," rejected=",$Get(tRows(tStream, "rejected"), 0),!
        }
        Write "state AFTER run (saved after all batches were OK): ",tNewState,!
    }
    Quit tRead
}

Method NewRequest() As %Net.HttpRequest [ Private ]
{
    Set tReq = ##class(%Net.HttpRequest).%New()
    Do ##class(%Net.URLParser).Parse(..WorkerURL, .tUrl)
    Set tReq.Server = tUrl("host")
    Set tReq.Port = $Get(tUrl("port"), 80)
    Set tReq.Timeout = ..ReadTimeout
    Quit tReq
}

Method CursorFor(pStream As %String) As %String [ Private ]
{
    Set tList = $ListFromString(..CursorFields, ",")
    For i = 1:1:$ListLength(tList) {
        Set tPair = $List(tList, i)
        If $ZStrip($Piece(tPair, "=", 1), "<>W") = pStream {
            Quit
        }
        Set tPair = ""
    }
    Quit $ZStrip($Piece($Get(tPair), "=", 2), "<>W")
}

}
