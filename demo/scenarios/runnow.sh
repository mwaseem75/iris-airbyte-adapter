#!/bin/sh
# Start one run in the iris container (production must be running), then show row counts per stream.
docker exec -i iris iris session IRIS -U USER <<'IRIS'
set sc = ##class(Airbyte.Demo.Service.Extract).RunNow()
if $system.Status.IsError(sc) { do $system.Status.DisplayError(sc) }
set rs = ##class(%SQL.Statement).%ExecDirect(,"SELECT Stream, COUNT(*) AS n FROM Airbyte_Demo_Data.Record GROUP BY Stream")
while rs.%Next() { write "table: ",rs.Stream," rows=",rs.n,! }
set rs = ##class(%SQL.Statement).%ExecDirect(,"SELECT COUNT(*) AS n FROM Airbyte_Demo_Data.Record")
do rs.%Next() write "COUNT(*) = ",rs.n,!
halt
IRIS
