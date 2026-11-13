Include Ensemble

/// Prototype. Stores one batch of one stream with insert-or-update and no duplicates.
/// One transaction per batch: the records and the Run counters commit together.
/// Per record: no/invalid key -> rejected; not found -> insert; hash differs -> update; same -> unchanged.
/// Never deletes records. Logs counts only, never record data.
Class Airbyte.Demo.Operation.Store Extends Ens.BusinessOperation
{

Parameter INVOCATION = "Queue";

Method OnBatch(pRequest As Airbyte.Demo.Msg.Batch, Output pResponse As Ens.StringResponse) As %Status
{
    // response StringValue = "inserted,updated,unchanged,rejected"
    Set pResponse = ##class(Ens.StringResponse).%New()
    Set tIns = 0, tUpd = 0, tUnc = 0, tRej = 0, tSC = $$$OK
    Set tRunId = pRequest.RunId
    Try {
        Set tRecs = ##class(%DynamicAbstractObject).%FromJSON(pRequest.Records)
        TSTART
        Set tIter = tRecs.%GetIterator()
        While tIter.%GetNext(.tIdx, .tRec) {
            Set tResult = ..StoreRecord(pRequest.ConnectionId, pRequest.Stream, tRec)
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
        &sql(UPDATE Airbyte_Demo_Data.Run
             SET Inserted = Inserted + :tIns, Updated = Updated + :tUpd,
                 Unchanged = Unchanged + :tUnc, Rejected = Rejected + :tRej
             WHERE ID = :tRunId)
        If SQLCODE < 0 {
            Throw ##class(%Exception.SQL).CreateFromSQLCODE(SQLCODE, %msg)
        }
        TCOMMIT
        Set pResponse.StringValue = tIns_","_tUpd_","_tUnc_","_tRej
        $$$LOGINFO("batch "_pRequest.BatchNo_" of "_pRequest.Stream_" (read "_pRequest.ReadId_"): inserted="_tIns_" updated="_tUpd_" unchanged="_tUnc_" rejected="_tRej)
    } Catch ex {
        If $TLevel > 0 {
            TROLLBACK
        }
        Set tSC = ex.AsStatus()
    }
    Quit tSC
}

/// Returns "I" (inserted), "U" (updated), "N" (unchanged) or "R" (rejected)
Method StoreRecord(pConnectionId As %String, pStream As %String, pRec As %DynamicObject) As %String
{
    If (pRec.%GetTypeOf("key") '= "array") || (pRec.%Get("key_error") '= "") {
        $$$LOGWARNING("record rejected in stream "_pStream_": "_pRec.%Get("key_error"))
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
