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

Parameter SETTINGS = "WorkerURL:Connection,ConnectionId:Connection,Streams:Connection,CursorFields:Connection,KeyFields:Connection,Mode:Connection,BatchSize:Connection,ConnectorName:Connection,ConnectorVersion:Connection,ConnectorConfig:Connection,Credentials:Connection:credentialsSelector,CredentialFields:Connection,ReadTimeout:Connection,HttpTimeout:Connection,PollInterval:Connection,MaxRetries:Connection,BackoffSeconds:Connection,PollingEnabled:Basic,Target:Basic";

/// Base URL of the worker, as seen from IRIS
Property WorkerURL As %String(MAXLEN = 300) [ InitialExpression = "http://worker:8000" ];

Property ConnectionId As %String(MAXLEN = 100) [ InitialExpression = "demo" ];

/// Comma separated stream names
Property Streams As %String(MAXLEN = 500) [ InitialExpression = "customers,order_lines,suppliers" ];

/// Comma separated stream=cursor_field pairs. A stream without a cursor is read as full_refresh.
Property CursorFields As %String(MAXLEN = 500) [ InitialExpression = "customers=updated_at,suppliers=updated_at" ];

/// Comma separated stream=col1+col2 pairs: the columns that form the record key, for streams without a
/// usable primary key. Records where one of them is missing or empty are rejected (see Util.Rejects()).
Property KeyFields As %String(MAXLEN = 500);

Property Mode As %String(VALUELIST = ",full_refresh,incremental") [ InitialExpression = "incremental" ];

Property BatchSize As %Integer [ InitialExpression = 500 ];

Property ConnectorVersion As %String [ InitialExpression = "3.8.11" ];

/// Airbyte connector name, for example source-postgres. The worker only runs connectors it allows.
Property ConnectorName As %String [ InitialExpression = "source-postgres" ];

/// Connector settings as JSON, WITHOUT secrets (the connector's own field names; see Util.Spec()).
/// The default is the demo Postgres as seen from the connector container (it is not on the compose network).
Property ConnectorConfig As %String(MAXLEN = 20000) [ InitialExpression = "{""host"":""host.docker.internal"",""port"":5433,""database"":""demo"",""schemas"":[""public""],""ssl_mode"":{""mode"":""disable""},""replication_method"":{""method"":""Standard""},""tunnel_method"":{""tunnel_method"":""NO_TUNNEL""}}" ];

/// Ens.Config.Credentials entry holding the secrets
Property Credentials As %String [ InitialExpression = "Postgres" ];

/// Which config fields get their value from a credentials entry: config path=Property, comma separated.
/// Property alone (Username, Password) comes from the Credentials setting. Another entry is named in front,
/// for example credentials.client_secret=SalesforceApp.Password. Nested config fields use dots.
Property CredentialFields As %String(MAXLEN = 500) [ InitialExpression = "username=Username,password=Password" ];

/// Time limit in seconds for the whole run, retries included
Property ReadTimeout As %Integer [ InitialExpression = 600 ];

/// Timeout in seconds for each single HTTP call to the worker
Property HttpTimeout As %Integer [ InitialExpression = 30 ];

/// Seconds between status checks while the worker reads
Property PollInterval As %Integer [ InitialExpression = 2 ];

/// How often a run is repeated after a temporary error (worker unreachable, HTTP 5xx, connection, timeout, internal)
Property MaxRetries As %Integer [ InitialExpression = 3 ];

/// Wait before retry n is BackoffSeconds * 2^(n-1): 5, 10, 20 s by default
Property BackoffSeconds As %Integer [ InitialExpression = 5 ];

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

/// Start one run now (production must be running). Prints a summary. pItem is the name of the Extract
/// item in the production (a second connection uses its own item).
ClassMethod RunNow(pVerbose As %Boolean = 1, pItem As %String = "Extract") As %Status
{
    Set tSC = ##class(Ens.Director).CreateBusinessService(pItem, .tService)
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

/// How an error is handled, from the worker's error category and the HTTP status (0 if there was none):
///   busy      the worker is full or has a read for this connection: skip, no retry
///   config    something the operator must fix (auth, config, validation, not_found): stop, no retry
///   transient worth retrying: worker unreachable, HTTP 5xx, categories connection, timeout, internal, connector
///   permanent anything else (cancelled, store errors, other 4xx): stop, no retry
ClassMethod Classify(pCategory As %String, pHttpStatus As %Integer = 0) As %String
{
    If (pHttpStatus = 409) || (pCategory = "busy") {
        Quit "busy"
    }
    If $ListFind($ListBuild("auth", "config", "validation", "not_found"), pCategory) {
        Quit "config"
    }
    If $ListFind($ListBuild("unreachable", "connection", "timeout", "internal", "connector"), pCategory) || (pHttpStatus >= 500) {
        Quit "transient"
    }
    Quit "permanent"
}

ClassMethod IsTransient(pCategory As %String, pHttpStatus As %Integer = 0) As %Boolean
{
    Quit ..Classify(pCategory, pHttpStatus) = "transient"
}

/// Sentence for the operator when a run stops because of a configuration problem
Method ConfigHint(pCategory As %String, pText As %String) As %String [ Private ]
{
    If pCategory = "auth" {
        Quit "authentication failed: check the credentials entry '"_..Credentials_"'"
    }
    Quit "configuration problem ("_pCategory_"): "_pText
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
    // ReadTimeout limits the whole run, retries included
    Set tT0 = $ZHorolog
    Set tDeadline = tT0 + ..ReadTimeout
    Set tRead = 0, tStatus = "Completed", tMessage = "", tCategory = "", tSC = $$$OK
    Set tRetries = 0, tDone = 0
    While 'tDone {
        Set tFailed = 0
        Try {
            Set tRead = ..Extract(tRunId, pVerbose, tDeadline, (tRetries = 0))
        } Catch ex {
            Set tFailed = 1
            Set tErr = ex.AsStatus()
            If $ListFind($ListBuild("WorkerBusy", "WorkerError", "ConfigError", "RunTimeout", "StoreError"), ex.Name) {
                Set tCategory = $Select(ex.Name = "WorkerBusy":"busy", ex.Name = "ConfigError":"config", ex.Name = "RunTimeout":"timeout", ex.Name = "StoreError":"store", 1:ex.Location)
                Set tText = ex.Data
                Set tHttp = $Select(ex.Name = "WorkerError":ex.Code, 1:0)
                Set tClass = $Select(ex.Name = "RunTimeout":"permanent", 1:..Classify(tCategory, tHttp))
            } Else {
                // unexpected error inside IRIS: not a worker problem, so no retry
                Set tCategory = "iris", tClass = "permanent"
                Set tText = $System.Status.GetOneStatusText(tErr)
            }
            Set tText = $Extract(tText, 1, 400)
        }
        If 'tFailed {
            Set tDone = 1
            If tRetries {
                Set tMessage = "completed after "_tRetries_" retr"_$Select(tRetries = 1:"y", 1:"ies")_" (last error: "_tCategory_")"
            }
        } ElseIf tClass = "busy" {
            Set tDone = 1, tStatus = "Skipped", tMessage = tText
            $$$LOGWARNING("worker is busy (409); run "_tRunId_" skipped, no retry")
        } ElseIf tClass = "config" {
            Set tDone = 1, tStatus = "ConfigError", tSC = tErr
            Set tMessage = $Extract(..ConfigHint(tCategory, tText), 1, 500)
            $$$LOGERROR("run "_tRunId_" stopped, configuration problem ("_tCategory_"): "_tMessage)
        } ElseIf tClass = "transient" {
            Set tWait = ..BackoffSeconds * (2 ** tRetries)
            If (tRetries >= ..MaxRetries) || ($ZHorolog + tWait >= tDeadline) {
                Set tDone = 1, tStatus = "Failed", tSC = tErr
                Set tMessage = $Extract("failed after "_tRetries_" retries: "_tCategory_": "_tText, 1, 500)
                $$$LOGERROR("run "_tRunId_" failed after "_tRetries_" retries ("_tCategory_")")
            } Else {
                Set tRetries = tRetries + 1
                $$$LOGWARNING("run "_tRunId_": temporary error ("_tCategory_"), retry "_tRetries_" of "_..MaxRetries_" in "_tWait_" s")
                &sql(UPDATE Airbyte_Demo_Data.Run SET RetryCount = :tRetries, ErrorCategory = :tCategory WHERE ID = :tRunId)
                If pVerbose {
                    Write !,"temporary error (",tCategory,"); retry ",tRetries," of ",..MaxRetries," in ",tWait," s",!
                }
                Hang tWait
            }
        } Else {
            Set tDone = 1, tStatus = "Failed", tSC = tErr
            Set tMessage = $Extract(tCategory_": "_tText, 1, 500)
            $$$LOGERROR("run "_tRunId_" failed ("_tCategory_")")
        }
    }
    Set tEnded = $ZDateTime($ZTimeStamp, 3, 1, 3)
    If tStatus = "Completed" {
        Set tCategory = $Select(tRetries:tCategory, 1:"")
    }
    Set tDuration = $Number($ZHorolog - tT0, 2)
    &sql(UPDATE Airbyte_Demo_Data.Run SET Status = :tStatus, Ended = :tEnded, RecordsRead = :tRead, Message = :tMessage, RetryCount = :tRetries, ErrorCategory = :tCategory, DurationSeconds = :tDuration WHERE ID = :tRunId)
    Do ##class(Airbyte.Demo.Data.ConnectionStatus).Record(..ConnectionId, tRunId, tStatus, tEnded, $Select(tStatus = "Completed":"", 1:tCategory), $Select(tStatus = "Completed":"", 1:tMessage))
    &sql(SELECT RecordsRead, Inserted, Updated, Unchanged, Rejected INTO :lr, :li, :lu, :ln, :lj FROM Airbyte_Demo_Data.Run WHERE ID = :tRunId)
    $$$LOGINFO("run "_tRunId_": connection="_..ConnectionId_" status="_tStatus_" read="_lr_" inserted="_li_" updated="_lu_" unchanged="_ln_" rejected="_lj_" duration="_tDuration_"s retries="_tRetries)
    If pVerbose {
        &sql(SELECT RecordsRead, Inserted, Updated, Unchanged, Rejected INTO :r, :i, :u, :n, :j FROM Airbyte_Demo_Data.Run WHERE ID = :tRunId)
        Write "run ",tRunId,": ",tStatus," read=",r," inserted=",i," updated=",u," unchanged=",n," rejected=",j," retries=",tRetries
        If tMessage '= "" {
            Write " (",tMessage,")"
        }
        Write !
    }
    Quit tSC
}

/// Starts the read in the worker (async), stores every batch in order while the worker is still reading,
/// then saves the new state. Throws on any failure and then does NOT save the state. Each attempt starts
/// from the state saved by the last successful run. Returns the number of records read.
Method Extract(pRunId As %Integer, pVerbose As %Boolean, pDeadline As %Decimal, pWarn As %Boolean = 1) As %Integer
{
    Set tConn = ..ConnectionId
    Set tBody = ..SourceBody(pWarn)
    Do tBody.%Set("mode", ..Mode)
    Do tBody.%Set("batch_size", +..BatchSize, "number")
    Do tBody.%Set("connection_id", tConn)
    Do tBody.%Set("wait", 0, "boolean")
    Set tKeys = ..KeyFieldsObject()
    If tKeys.%Size() {
        Do tBody.%Set("key_fields", tKeys)
    }
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

    // 1. start the read; the worker answers at once
    Set tResp = ..Call("POST", "/reads", tBody, .tCode)
    If tCode '= 202 {
        Do ..ThrowWorkerError(tCode, tResp)
    }
    Set tReadId = tResp.%Get("read_id")
    Set tNext = 1, tLastReady = -1, tOK = 0, tRead = 0
    Try {
        // 2. poll; every batch that is ready is stored (synchronously, one message per stream) before the next one
        For {
            If $ZHorolog > pDeadline {
                Throw ##class(%Exception.General).%New("RunTimeout", 0, "timeout", "run time limit (ReadTimeout "_..ReadTimeout_" s) reached")
            }
            Set tSt = ..Call("GET", "/reads/"_tReadId, , .tCode)
            If tCode '= 200 {
                Do ..ThrowWorkerError(tCode, tSt)
            }
            Set tStatus = tSt.%Get("status")
            If tStatus = "failed" {
                Do ..ThrowWorkerError(0, tSt)
            }
            If tStatus = "cancelled" {
                Throw ##class(%Exception.General).%New("WorkerError", 0, "cancelled", "the read was cancelled in the worker")
            }
            Set tReady = $Select(tStatus = "completed":tSt.%Get("batches"), 1:+tSt.%Get("batches_ready"))
            If tReady '= tLastReady {
                $$$LOGINFO("read "_tReadId_": status "_tStatus_", batches ready "_tReady_", stored "_(tNext - 1))
                Set tLastReady = tReady
            }
            Set tWaiting = 0
            While (tNext <= tReady) && 'tWaiting {
                Set tWaiting = ..StoreBatch(pRunId, tConn, tReadId, tNext, .tRows)
                Set:'tWaiting tNext = tNext + 1
            }
            If (tStatus = "completed") && (tNext > tReady) {
                Quit
            }
            Hang ..PollInterval
        }
        Set tIt = tSt.%Get("counts").%GetIterator()
        While tIt.%GetNext(.tStream, .tCount) {
            Set tRead = tRead + tCount
            Set tRows(tStream, "read") = tCount
        }

        // 3. all batches stored OK and the read completed: only now fetch and save the new state
        Set tSResp = ..Call("GET", "/reads/"_tReadId_"/state", , .tCode)
        If tCode '= 200 {
            Do ..ThrowWorkerError(tCode, tSResp)
        }
        Set tNewState = tSResp.%Get("state").%ToJSON()
        Set ^Airbyte.Demo.State(tConn) = tNewState
        Set tOK = 1
    } Catch ex {
        If 'tOK {
            // stop the worker's read; best effort
            Try {
                Do ..Call("DELETE", "/reads/"_tReadId)
            } Catch {
            }
        }
        Throw ex
    }
    // the state is saved: the worker can delete the batch files of this read (best effort)
    Try {
        Do ..Call("POST", "/reads/"_tReadId_"/ack", , .tAckCode)
        If tAckCode '= 200 {
            $$$LOGWARNING("worker did not accept the acknowledgement of read "_tReadId_" (HTTP "_tAckCode_")")
        }
    } Catch {
        $$$LOGWARNING("could not acknowledge read "_tReadId_" to the worker")
    }
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

/// Fetch batch pN and send it to the operation, one message per stream. Returns 1 if the batch is not
/// written yet (the worker answered not_ready), 0 when it was stored. Throws on any other failure.
Method StoreBatch(pRunId As %Integer, pConn As %String, pReadId As %String, pN As %Integer, ByRef pRows) As %Boolean [ Private ]
{
    Set tBatch = ..Call("GET", "/reads/"_pReadId_"/batches/"_pN, , .tCode)
    If tCode = 409 {
        Quit 1
    }
    If tCode '= 200 {
        Do ..ThrowWorkerError(tCode, tBatch)
    }
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
        Set tMsg.ConnectionId = pConn
        Set tMsg.Stream = tS
        Set tMsg.ReadId = pReadId
        Set tMsg.BatchNo = pN
        Do tPerStream(tS).%ToJSON(tMsg.Records)
        Set tRsp = ""
        Set tSC = ..SendRequestSync(..Target, tMsg, .tRsp)
        If $$$ISERR(tSC) {
            Throw ##class(%Exception.General).%New("StoreError", 0, "store", "batch "_pN_" was not stored: "_$Extract($System.Status.GetOneStatusText(tSC), 1, 300))
        }
        If $IsObject(tRsp) {
            Set tParts = $ListFromString(tRsp.StringValue, ",")
            Set pRows(tS, "inserted") = $Get(pRows(tS, "inserted")) + $List(tParts, 1)
            Set pRows(tS, "updated") = $Get(pRows(tS, "updated")) + $List(tParts, 2)
            Set pRows(tS, "unchanged") = $Get(pRows(tS, "unchanged")) + $List(tParts, 3)
            Set pRows(tS, "rejected") = $Get(pRows(tS, "rejected")) + $List(tParts, 4)
        }
    }
    Quit 0
}

/// Throw the error the worker reported (HTTP status pCode, 0 when it comes from a read's status)
Method ThrowWorkerError(pCode As %Integer, pResp As %DynamicObject) [ Private ]
{
    Set tCat = "", tMsg = ""
    If $IsObject(pResp) && pResp.%IsDefined("error") && $IsObject(pResp.%Get("error")) {
        Set tErr = pResp.%Get("error")
        Set tCat = tErr.%Get("category"), tMsg = tErr.%Get("message")
        If tErr.%IsDefined("details") {
            Set tIt = tErr.%Get("details").%GetIterator()
            While tIt.%GetNext(.tI, .tD) {
                Set tMsg = tMsg_"; "_tD.%Get("path")_" "_tD.%Get("reason")
            }
        }
    }
    If ..Classify(tCat, pCode) = "busy" {
        Throw ##class(%Exception.General).%New("WorkerBusy", 409, "busy", "worker is busy (409): "_tMsg)
    }
    Throw ##class(%Exception.General).%New("WorkerError", pCode, tCat, "worker "_$Select(pCode:"returned "_pCode_" ", 1:"reported ")_tCat_": "_tMsg)
}

/// Call the worker (pVerb GET, POST or DELETE). Returns the parsed answer; pCode is the HTTP status.
/// Throws WorkerError with category unreachable if there is no answer.
Method Call(pVerb As %String, pPath As %String, pBody As %DynamicObject = "", Output pCode As %Integer) As %DynamicObject
{
    Set tReq = ..NewRequest()
    If $IsObject(pBody) {
        Set tReq.ContentType = "application/json"
        Do tReq.EntityBody.Write(pBody.%ToJSON())
    }
    If pVerb = "GET" {
        Set tSC = tReq.Get(pPath)
    } ElseIf pVerb = "DELETE" {
        Set tSC = tReq.Delete(pPath)
    } Else {
        Set tSC = tReq.Post(pPath)
    }
    If $$$ISERR(tSC) {
        Throw ##class(%Exception.General).%New("WorkerError", 0, "unreachable", "worker not reachable at "_..WorkerURL)
    }
    Set pCode = tReq.HttpResponse.StatusCode
    Set tResp = {}
    Try {
        Set tResp = ##class(%DynamicAbstractObject).%FromJSON(tReq.HttpResponse.Data)
    } Catch {
    }
    Quit tResp
}

/// {connector, version, config} for the worker. The config is ConnectorConfig plus the values from the
/// Credentials entry. Throws if ConnectorConfig or CredentialFields is wrong. Never log the result.
Method SourceBody(pWarn As %Boolean = 0) As %DynamicObject
{
    Set tCred = ..CredFor(..Credentials)
    Set tConfig = ..BuildConfig(..ConnectorConfig, ..CredentialFields, tCred)
    If pWarn {
        Do ..WarnSecrets(..ConnectorConfig)
    }
    Quit {"connector":(..ConnectorName), "version":(..ConnectorVersion), "config":(tConfig)}
}

/// Parse the ConnectorConfig JSON and set the credential values at their paths. Intermediate objects
/// on a path are created if missing. Throws ConfigError with a message that never contains a value.
ClassMethod BuildConfig(pConfigJSON As %String, pCredFields As %String, pCred As %RegisteredObject) As %DynamicObject
{
    Try {
        Set tConfig = ##class(%DynamicAbstractObject).%FromJSON(pConfigJSON)
    } Catch ex {
        Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "ConnectorConfig is not valid JSON")
    }
    If 'tConfig.%IsA("%Library.DynamicObject") {
        Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "ConnectorConfig must be a JSON object")
    }
    Set tList = $ListFromString(pCredFields, ",")
    For i = 1:1:$ListLength(tList) {
        Set tPair = $ZStrip($List(tList, i), "<>W")
        Continue:tPair=""
        Set tPath = $ZStrip($Piece(tPair, "=", 1), "<>W")
        Set tProp = $ZStrip($Piece(tPair, "=", 2), "<>W")
        Set tCredUse = pCred
        If tProp [ "." {
            // entry.Property: another credentials entry (an entry name may itself contain dots)
            Set tEntry = $Piece(tProp, ".", 1, $Length(tProp, ".") - 1)
            Set tProp = $Piece(tProp, ".", $Length(tProp, "."))
            Set tCredUse = ..CredFor(tEntry)
        }
        If (tPath = "") || (tProp = "") || ($Length(tPair, "=") '= 2) {
            Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "CredentialFields entry '"_tPair_"' must look like path=Property")
        }
        If '$IsObject(tCredUse) {
            Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "no credentials found for CredentialFields")
        }
        If '##class(%Dictionary.CompiledProperty).%ExistsId(tCredUse.%ClassName(1)_"||"_tProp) {
            Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "credentials have no property '"_tProp_"' (for config path '"_tPath_"')")
        }
        Set tValue = $Property(tCredUse, tProp)
        If tValue = "" {
            Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "credential property '"_tProp_"' is empty (for config path '"_tPath_"')")
        }
        Set tNode = tConfig
        Set tParts = $Length(tPath, ".")
        For k = 1:1:tParts - 1 {
            Set tPart = $Piece(tPath, ".", k)
            If tPart = "" {
                Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "invalid config path '"_tPath_"'")
            }
            If tNode.%GetTypeOf(tPart) = "unassigned" {
                Do tNode.%Set(tPart, {})
            }
            Set tNode = tNode.%Get(tPart)
            If '$IsObject(tNode) || 'tNode.%IsA("%Library.DynamicObject") {
                Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "config path '"_tPath_"': '"_tPart_"' is not a JSON object in ConnectorConfig")
            }
        }
        Set tLeaf = $Piece(tPath, ".", tParts)
        If tLeaf = "" {
            Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "invalid config path '"_tPath_"'")
        }
        Do tNode.%Set(tLeaf, tValue)
    }
    Quit tConfig
}

/// Credentials object of a named Ens.Config.Credentials entry. Throws ConfigError if it does not exist.
ClassMethod CredFor(pEntry As %String) As %RegisteredObject
{
    Set tSC = ##class(Ens.Config.Credentials).GetCredentialsObj(.tCred, $ClassName(), "Ens.Config.Credentials", pEntry)
    If $$$ISERR(tSC) || '$IsObject($Get(tCred)) {
        Throw ##class(%Exception.General).%New("ConfigError", 5001, "BuildConfig", "credentials entry '"_pEntry_"' not found")
    }
    Quit tCred
}

/// The string at a dotted path in a JSON object, or "" if it is missing or not a simple value
ClassMethod ValueAt(pObj As %DynamicObject, pPath As %String) As %String
{
    Set tNode = pObj
    For i = 1:1:$Length(pPath, ".") {
        If '$IsObject(tNode) || 'tNode.%IsA("%Library.DynamicObject") {
            Set tNode = ""
            Quit
        }
        Set tNode = tNode.%Get($Piece(pPath, ".", i))
    }
    Quit $Select($IsObject(tNode):"", 1:tNode)
}

/// Warn (without values) if ConnectorConfig itself holds a value for a field the connector marks as secret
Method WarnSecrets(pConfigJSON As %String) [ Private ]
{
    Try {
        Set tRaw = ##class(%DynamicAbstractObject).%FromJSON(pConfigJSON)
        Set tIt = ..Spec().%Get("secret_fields").%GetIterator()
        While tIt.%GetNext(.tIdx, .tPath) {
            If ..ValueAt(tRaw, tPath) '= "" {
                $$$LOGWARNING("ConnectorConfig has a value for the secret field '"_tPath_"'; keep secrets in the Credentials entry and list the field in CredentialFields")
            }
        }
    } Catch ex {
        // no spec available: nothing to compare with
    }
}

/// POST a JSON body to the worker and return the parsed answer; pCode is the HTTP status
Method PostJSON(pPath As %String, pBody As %DynamicObject, Output pCode As %Integer) As %DynamicObject
{
    Quit ..Call("POST", pPath, pBody, .pCode)
}

/// The connector's spec from the worker: connection_specification (JSON Schema) and secret_fields
Method Spec() As %DynamicObject
{
    Set tResp = ..PostJSON("/spec", {"connector":(..ConnectorName), "version":(..ConnectorVersion)}, .tCode)
    If tCode '= 200 {
        Throw ##class(%Exception.General).%New("WorkerError", tCode, , "spec failed: "_tResp.error.category_": "_tResp.error.message)
    }
    Quit tResp
}

Method NewRequest() As %Net.HttpRequest [ Private ]
{
    Set tReq = ##class(%Net.HttpRequest).%New()
    Do ##class(%Net.URLParser).Parse(..WorkerURL, .tUrl)
    Set tReq.Server = tUrl("host")
    Set tReq.Port = $Get(tUrl("port"), 80)
    Set tReq.Timeout = ..HttpTimeout
    Quit tReq
}

/// KeyFields as {"stream": ["col1", "col2"]}
Method KeyFieldsObject() As %DynamicObject [ Private ]
{
    Set tObj = {}
    Set tList = $ListFromString(..KeyFields, ",")
    For i = 1:1:$ListLength(tList) {
        Set tPair = $ZStrip($List(tList, i), "<>W")
        Continue:tPair=""
        Set tName = $ZStrip($Piece(tPair, "=", 1), "<>W")
        Set tCols = []
        Set tColList = $ListFromString($Piece(tPair, "=", 2), "+")
        For k = 1:1:$ListLength(tColList) {
            Set tCol = $ZStrip($List(tColList, k), "<>W")
            Do:tCol'="" tCols.%Push(tCol)
        }
        Do:(tName'="")&&tCols.%Size() tObj.%Set(tName, tCols)
    }
    Quit tObj
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
