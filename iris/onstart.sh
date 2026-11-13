#!/bin/sh
# Runs after IRIS starts (iris-main --after): load ObjectScript from the mounted src folder, start the demo production.
# The marker file tells the compose healthcheck that this is finished (so `up --wait` means really ready).
rm -f /home/irisowner/onstart.done
iris session IRIS -U USER <<'IRISEOF'
set sc = $system.OBJ.LoadDir("/home/irisowner/dev/src/cls","ck",,1)
if ##class(%Dictionary.CompiledClass).%ExistsId("Airbyte.Demo.Production") { set sc = ##class(Airbyte.Demo.Production).Setup() if $system.Status.IsError(sc) { do $system.Status.DisplayError(sc) } }
halt
IRISEOF
touch /home/irisowner/onstart.done
