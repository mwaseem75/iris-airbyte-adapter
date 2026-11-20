/// Counts of one run for one stream. Filled by the Store operation in the same transaction as each batch.
Class Airbyte.Demo.Data.RunStream Extends %Persistent
{

Property RunId As %Integer [ Required ];

Property Stream As %String(MAXLEN = 200) [ Required ];

/// Records handed to the store (READ is a reserved SQL word, hence the SQL field name)
Property Read As %Integer [ InitialExpression = 0, SqlFieldName = RecordsRead ];

Property Inserted As %Integer [ InitialExpression = 0 ];

Property Updated As %Integer [ InitialExpression = 0 ];

Property Unchanged As %Integer [ InitialExpression = 0 ];

Property Rejected As %Integer [ InitialExpression = 0 ];

Index RunStreamIdx On (RunId, Stream) [ Unique ];

}
