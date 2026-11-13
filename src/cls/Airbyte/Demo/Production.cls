Include Ensemble

/// Prototype: Extract (service) -> Store (operation).
/// Setup() creates the Postgres credentials from the container environment (POSTGRES_USER / POSTGRES_PASSWORD,
/// which come from .env) and starts the production. No secrets are stored in this class.
Class Airbyte.Demo.Production Extends Ens.Production
{

XData ProductionDefinition
{
<Production Name="Airbyte.Demo.Production" TestingEnabled="true" LogGeneralTraceEvents="true">
  <Description>Demo: Postgres -> Airbyte source-postgres (worker) -> IRIS table</Description>
  <ActorPoolSize>1</ActorPoolSize>
  <Item Name="Store" Category="" ClassName="Airbyte.Demo.Operation.Store" PoolSize="1" Enabled="true" Foreground="false" Comment="Insert-or-update, one transaction per batch" LogTraceEvents="true" Schedule="">
  </Item>
  <Item Name="Extract" Category="" ClassName="Airbyte.Demo.Service.Extract" PoolSize="1" Enabled="true" Foreground="false" Comment="Polling is off; start runs with RunNow()" LogTraceEvents="true" Schedule="">
    <Setting Target="Adapter" Name="CallInterval">3600</Setting>
    <Setting Target="Host" Name="PollingEnabled">0</Setting>
    <Setting Target="Host" Name="WorkerURL">http://worker:8000</Setting>
    <Setting Target="Host" Name="ConnectionId">demo</Setting>
    <Setting Target="Host" Name="Streams">customers,order_lines,suppliers</Setting>
    <Setting Target="Host" Name="CursorFields">customers=updated_at,suppliers=updated_at</Setting>
    <Setting Target="Host" Name="Mode">incremental</Setting>
    <Setting Target="Host" Name="BatchSize">500</Setting>
    <Setting Target="Host" Name="ReadTimeout">120</Setting>
    <Setting Target="Host" Name="Credentials">Postgres</Setting>
    <Setting Target="Host" Name="Target">Store</Setting>
  </Item>
</Production>
}

/// Create the credentials from the environment and start the production. Safe to call again.
ClassMethod Setup() As %Status
{
    Set tUser = $SYSTEM.Util.GetEnviron("POSTGRES_USER")
    Set tPass = $SYSTEM.Util.GetEnviron("POSTGRES_PASSWORD")
    If (tUser = "") || (tPass = "") {
        Quit $$$ERROR($$$GeneralError, "POSTGRES_USER / POSTGRES_PASSWORD are not set in the iris container (see .env and docker-compose.yml)")
    }
    Set tCred = ##class(Ens.Config.Credentials).%OpenId("Postgres")
    If '$IsObject(tCred) {
        Set tCred = ##class(Ens.Config.Credentials).%New()
        Set tCred.SystemName = "Postgres"
    }
    Set tCred.Username = tUser
    Set tCred.Password = tPass
    Set tSC = tCred.%Save()
    If $$$ISERR(tSC) {
        Quit tSC
    }
    If ##class(Ens.Director).IsProductionRunning() {
        Quit $$$OK
    }
    Do ##class(Ens.Director).SetAutoStart("Airbyte.Demo.Production")
    Quit ##class(Ens.Director).StartProduction("Airbyte.Demo.Production")
}

}
