/// One extraction run. Counters are updated by the Store operation in the
/// same transaction as the records of each batch.
Class Airbyte.Demo.Data.Run Extends %Persistent
{

Property RunId As %Integer [ Calculated, SqlComputeCode = {Set {*}={%%ID}}, SqlComputed ];

Property ConnectionId As %String(MAXLEN = 100);

Property Started As %TimeStamp;

Property Ended As %TimeStamp;

/// Running, Completed, Failed, Skipped, ConfigError (the operator must fix something; no retry)
Property Status As %String(MAXLEN = 20);

/// Records returned by the worker (READ is a reserved SQL word, hence the SQL field name)
Property Read As %Integer [ InitialExpression = 0, SqlFieldName = RecordsRead ];

Property Inserted As %Integer [ InitialExpression = 0 ];

Property Updated As %Integer [ InitialExpression = 0 ];

Property Unchanged As %Integer [ InitialExpression = 0 ];

Property Rejected As %Integer [ InitialExpression = 0 ];

/// Error or skip reason (never record data or credentials)
Property Message As %String(MAXLEN = 500);

/// How many times the run was repeated after a temporary error
Property RetryCount As %Integer [ InitialExpression = 0 ];

/// Category of the last error (auth, config, unreachable, connection, timeout, internal, store, busy ...)
Property ErrorCategory As %String(MAXLEN = 30);

/// Wall-clock seconds of the whole run, retries included
Property DurationSeconds As %Numeric(SCALE = 2);

/// Seconds the Store operation spent on this run's batches (all attempts). DurationSeconds minus this is
/// mostly extraction and waiting for the worker.
Property StoreSeconds As %Numeric(SCALE = 2) [ InitialExpression = 0 ];

Index ConnIdx On ConnectionId;

}
