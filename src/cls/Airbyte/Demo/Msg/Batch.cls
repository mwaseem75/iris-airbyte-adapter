/// One batch of records of ONE stream, as returned by the worker.
/// Records is a JSON array of {"stream","key","data"} (and "key_error" when the key is flagged).
Class Airbyte.Demo.Msg.Batch Extends Ens.Request
{

Property RunId As %Integer;

Property ConnectionId As %String(MAXLEN = 100);

Property Stream As %String(MAXLEN = 200);

Property ReadId As %String(MAXLEN = 100);

Property BatchNo As %Integer;

Property Records As %Stream.GlobalCharacter;

}
