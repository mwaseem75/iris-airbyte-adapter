/// Prototype. One stored source record. Identity = (ConnectionId, Stream, RecordKey),
/// so the same id in two streams is stored twice.
Class Airbyte.Demo.Data.Record Extends %Persistent
{

Property ConnectionId As %String(MAXLEN = 100) [ Required ];

Property Stream As %String(MAXLEN = 200) [ Required ];

/// JSON array of the key parts as strings, e.g. ["1"] or ["1","2"]
Property RecordKey As %String(MAXLEN = 500) [ Required ];

/// SHA-256 (hex) of the record data, keys in sorted order
Property Hash As %String(MAXLEN = 64);

/// The record data exactly as the worker returned it (JSON)
Property Data As %Stream.GlobalCharacter;

Property FirstLoaded As %TimeStamp;

Property LastUpdated As %TimeStamp;

Index RecordIdx On (ConnectionId, Stream, RecordKey) [ Unique ];

}
