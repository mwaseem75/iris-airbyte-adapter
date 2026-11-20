Include Ensemble

/// Prototype. Stores one batch of one stream with insert-or-update and no duplicates.
/// One transaction per batch: the records and the Run counters commit together.
/// Per record: no/invalid key -> rejected; not found -> insert; hash differs -> update; same -> unchanged.
/// Rejected records go to Airbyte_Demo_Data.Rejected. Never deletes records. Logs counts only, never record data.
Class Airbyte.Demo.Operation.Store Extends Ens.BusinessOperation
{

Parameter INVOCATION = "Queue";

Parameter SETTINGS = "ClearBatchBodies:Basic";

/// 1 (default): once a batch is committed, the record data in the stored Batch message is emptied. The
/// message keeps its headers and counts, but message storage no longer holds a second copy of the data.
/// Set 0 to keep the data in the messages for debugging.
Property ClearBatchBodies As %Boolean [ InitialExpression = 1 ];

Method OnBatch(pRequest As Airbyte.Demo.Msg.Batch, Output pResponse As Ens.StringResponse) As %Status
{
    // response StringValue = "inserted,updated,unchanged,rejected"
    Set pResponse = ##class(Ens.StringResponse).%New()
    Set tSC = $$$OK
    Try {
        Set tCounts = ..ProcessBatch(pRequest)
        Set pResponse.StringValue = tCounts
        $$$LOGINFO("batch "_pRequest.BatchNo_" of "_pRequest.Stream_" (read "_pRequest.ReadId_"): inserted="_$Piece(tCounts, ",", 1)_" updated="_$Piece(tCounts, ",", 2)_" unchanged="_$Piece(tCounts, ",", 3)_" rejected="_$Piece(tCounts, ",", 4))
        If $Piece(tCounts, ",", 4) {
            $$$LOGWARNING($Piece(tCounts, ",", 4)_" record(s) of stream "_pRequest.Stream_" rejected: key missing or invalid (see table Airbyte_Demo_Data.Rejected)")
        }
    } Catch ex {
        Set tSC = ex.AsStatus()
    }
    If $$$ISOK(tSC) && ..ClearBatchBodies {
        // the batch is committed: message storage keeps the header and the counts, not the data
        Try {
            Do pRequest.Records.Clear()
            $$$ThrowOnError(pRequest.%Save())
        } Catch ex {
            $$$LOGWARNING("could not empty the body of batch "_pRequest.BatchNo_" of "_pRequest.Stream)
        }
    }
    Quit tSC
}

/// Store one batch in ONE transaction: the records, the rejected records and the counters of the run
/// (per run and per stream). Returns "inserted,updated,unchanged,rejected". Throws on failure after
/// rolling back, so a failed batch leaves nothing behind.
ClassMethod ProcessBatch(pRequest As Airbyte.Demo.Msg.Batch) As %String
{
    Set tIns = 0, tUpd = 0, tUnc = 0, tRej = 0
    Set tRunId = pRequest.RunId, tStream = pRequest.Stream
    Set tT0 = $ZHorolog
    Try {
        Set tRecs = ##class(%DynamicAbstractObject).%FromJSON(pRequest.Records)
        TSTART
        Set tIter = tRecs.%GetIterator()
        While tIter.%GetNext(.tIdx, .tRec) {
            Set tResult = ..StoreRecord(pRequest.ConnectionId, tStream, tRec, tRunId, pRequest.BatchNo)
            If tResult = "I" {
                Set tIns = tIns + 1
            } ElseIf tResult = "U" {
                Set tUpd = tUpd + 1
            } ElseIf tResult = "N" {
                Set tUnc = tUnc + 1
            } Else {
                Set tRej = tRej + 1
            }
        }
        Set tRead = tIns + tUpd + tUnc + tRej
        Set tSecs = $Number($ZHorolog - tT0, 3)
        &sql(UPDATE Airbyte_Demo_Data.Run
             SET StoreSeconds = StoreSeconds + :tSecs, Inserted = Inserted + :tIns, Updated = Updated + :tUpd,
                 Unchanged = Unchanged + :tUnc, Rejected = Rejected + :tRej
             WHERE ID = :tRunId)
        If SQLCODE < 0 {
            Throw ##class(%Exception.SQL).CreateFromSQLCODE(SQLCODE, %msg)
        }
        &sql(UPDATE Airbyte_Demo_Data.RunStream
             SET RecordsRead = RecordsRead + :tRead, Inserted = Inserted + :tIns, Updated = Updated + :tUpd,
                 Unchanged = Unchanged + :tUnc, Rejected = Rejected + :tRej
             WHERE RunId = :tRunId AND Stream = :tStream)
        If SQLCODE = 100 {
            &sql(INSERT INTO Airbyte_Demo_Data.RunStream (RunId, Stream, RecordsRead, Inserted, Updated, Unchanged, Rejected)
                 VALUES (:tRunId, :tStream, :tRead, :tIns, :tUpd, :tUnc, :tRej))
        }
        If SQLCODE < 0 {
            Throw ##class(%Exception.SQL).CreateFromSQLCODE(SQLCODE, %msg)
        }
        TCOMMIT
    } Catch ex {
        If $TLevel > 0 {
            TROLLBACK
        }
        Throw ex
    }
    Quit tIns_","_tUpd_","_tUnc_","_tRej
}

/// Returns "I" (inserted), "U" (updated), "N" (unchanged) or "R" (rejected). A rejected record is written
/// to the Rejected table (call inside the batch transaction).
ClassMethod StoreRecord(pConnectionId As %String, pStream As %String, pRec As %DynamicObject, pRunId As %Integer = "", pBatchNo As %Integer = "") As %String
{
    If (pRec.%GetTypeOf("key") '= "array") || (pRec.%Get("key_error") '= "") {
        Set tReason = pRec.%Get("key_error")
        Set:tReason="" tReason = "key missing or invalid"
        Set tPayload = $Select($IsObject(pRec.%Get("data")):pRec.%Get("data").%ToJSON(), 1:"{}")
        $$$ThrowOnError(##class(Airbyte.Demo.Data.Rejected).Add(pRunId, pBatchNo, pConnectionId, pStream, tReason, tPayload))
        Quit "R"
    }
    Set tKey = pRec.%Get("key").%ToJSON()
    Set tData = pRec.%Get("data")
    Set tText = tData.%ToJSON()
    Set tHash = ..CanonicalHash(tData)
    Set tNow = $ZDateTime($ZTimeStamp, 3, 1, 3)

    Set tObj = ##class(Airbyte.Demo.Data.Record).RecordIdxOpen(pConnectionId, pStream, tKey, , .tSC)
    If $IsObject(tObj) {
        If tObj.Hash = tHash {
            Quit "N"
        }
        Do tObj.Data.Clear()
        Do tObj.Data.Write(tText)
        Set tObj.Hash = tHash
        Set tObj.LastUpdated = tNow
        $$$ThrowOnError(tObj.%Save())
        Quit "U"
    }
    Set tObj = ##class(Airbyte.Demo.Data.Record).%New()
    Set tObj.ConnectionId = pConnectionId
    Set tObj.Stream = pStream
    Set tObj.RecordKey = tKey
    Do tObj.Data.Write(tText)
    Set tObj.Hash = tHash
    Set tObj.FirstLoaded = tNow
    Set tObj.LastUpdated = tNow
    $$$ThrowOnError(tObj.%Save())
    Quit "I"
}

/// SHA-256 over the record data as the worker returned it (values already normalised by the worker),
/// with the keys in sorted order so the hash does not depend on field order.
ClassMethod CanonicalHash(pData As %DynamicObject) As %String
{
    Set tIter = pData.%GetIterator()
    While tIter.%GetNext(.tName, .tValue) {
        Set tNames(tName) = ""
    }
    Set tCanon = {}
    Set tName = ""
    For {
        Set tName = $Order(tNames(tName))
        Quit:tName=""
        Set tType = pData.%GetTypeOf(tName)
        If (tType = "object") || (tType = "array") {
            Do tCanon.%Set(tName, pData.%Get(tName))
        } Else {
            Do tCanon.%Set(tName, pData.%Get(tName), tType)
        }
    }
    Set tBytes = $ZConvert(tCanon.%ToJSON(), "O", "UTF8")
    Quit $ZConvert(##class(%xsd.hexBinary).LogicalToXSD($SYSTEM.Encryption.SHAHash(256, tBytes)), "L")
}

XData MessageMap
{
<MapItems>
  <MapItem MessageType="Airbyte.Demo.Msg.Batch">
    <Method>OnBatch</Method>
  </MapItem>
</MapItems>
}

}
