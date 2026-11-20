/// Tests for Airbyte.Demo.Service.Extract:BuildConfig (merging credentials into the connector config)
Class Airbyte.Test.BuildConfigTest Extends %UnitTest.TestCase
{

/// A second credentials entry, for fields that use another entry than the Credentials setting
Method OnBeforeAllTests() As %Status
{
    Set tCred = ##class(Ens.Config.Credentials).%OpenId("AirbyteTestApp")
    If '$IsObject(tCred) {
        Set tCred = ##class(Ens.Config.Credentials).%New()
        Set tCred.SystemName = "AirbyteTestApp"
    }
    Set tCred.Username = "app-user"
    Set tCred.Password = "app-secret"
    Quit tCred.%Save()
}

Method OnAfterAllTests() As %Status
{
    Quit ##class(Ens.Config.Credentials).%DeleteId("AirbyteTestApp")
}

Method TestOtherCredentialsEntry()
{
    Set tConfig = ##class(Airbyte.Demo.Service.Extract).BuildConfig("{}", "username=Username,password=Password,credentials.client_secret=AirbyteTestApp.Password,credentials.client_id=AirbyteTestApp.Username", ..Cred())
    Do $$$AssertEquals(tConfig.password, "s3cret-value", "plain Password uses the default entry")
    Do $$$AssertEquals(tConfig.%Get("credentials").%Get("client_secret"), "app-secret", "entry.Password uses the named entry")
    Do $$$AssertEquals(tConfig.%Get("credentials").%Get("client_id"), "app-user", "entry.Username uses the named entry")
    Set tMsg = ..Failure("{}", "x=NoSuchEntry.Password", ..Cred())
    Do $$$AssertTrue(tMsg [ "credentials entry 'NoSuchEntry' not found", "unknown entry is reported")
    Do $$$AssertTrue(tMsg '[ "s3cret", "message has no secret")
}

Method Cred(pUser As %String = "demo", pPass As %String = "s3cret-value") As Ens.Config.Credentials [ Private ]
{
    Set tCred = ##class(Ens.Config.Credentials).%New()
    Set tCred.Username = pUser
    Set tCred.Password = pPass
    Quit tCred
}

/// The message of the error BuildConfig throws, or "" if it did not throw
Method Failure(pJSON As %String, pFields As %String, pCred As %RegisteredObject) As %String [ Private ]
{
    Set tMsg = ""
    Try {
        Do ##class(Airbyte.Demo.Service.Extract).BuildConfig(pJSON, pFields, pCred)
    } Catch ex {
        Set tMsg = ex.Data
    }
    Quit tMsg
}

Method TestFlatFields()
{
    Set tConfig = ##class(Airbyte.Demo.Service.Extract).BuildConfig("{""host"":""h"",""port"":5432}", "username=Username,password=Password", ..Cred())
    Do $$$AssertEquals(tConfig.host, "h", "existing field kept")
    Do $$$AssertEquals(tConfig.port, 5432, "number stays a number")
    Do $$$AssertEquals(tConfig.username, "demo", "username set")
    Do $$$AssertEquals(tConfig.password, "s3cret-value", "password set")
}

Method TestNestedPath()
{
    Set tConfig = ##class(Airbyte.Demo.Service.Extract).BuildConfig("{""tunnel_method"":{""tunnel_method"":""SSH_KEY_AUTH"",""tunnel_user"":""u""}}", "tunnel_method.tunnel_user_password=Password", ..Cred())
    Do $$$AssertEquals(tConfig.%Get("tunnel_method").%Get("tunnel_user_password"), "s3cret-value", "nested value set")
    Do $$$AssertEquals(tConfig.%Get("tunnel_method").%Get("tunnel_user"), "u", "sibling kept")
}

Method TestMissingIntermediateObjectIsCreated()
{
    Set tConfig = ##class(Airbyte.Demo.Service.Extract).BuildConfig("{}", "credentials.client_secret=Password", ..Cred())
    Do $$$AssertEquals(tConfig.%Get("credentials").%Get("client_secret"), "s3cret-value", "intermediate object created")
}

Method TestInvalidJSON()
{
    Set tMsg = ..Failure("{not json", "password=Password", ..Cred())
    Do $$$AssertTrue(tMsg [ "not valid JSON", "invalid JSON is reported")
    Set tMsg = ..Failure("[1,2]", "password=Password", ..Cred())
    Do $$$AssertTrue(tMsg [ "must be a JSON object", "a JSON array is rejected")
}

Method TestMissingCredential()
{
    Set tMsg = ..Failure("{}", "password=Password", ..Cred("demo", ""))
    Do $$$AssertTrue(tMsg [ "'Password' is empty", "empty credential is reported")
    Do $$$AssertTrue(tMsg '[ "s3cret", "message has no secret")
    Set tMsg = ..Failure("{}", "password=Password", "")
    Do $$$AssertTrue(tMsg [ "no credentials", "missing credentials entry is reported")
    Set tMsg = ..Failure("{}", "password=NoSuchProperty", ..Cred())
    Do $$$AssertTrue(tMsg [ "no property 'NoSuchProperty'", "unknown credentials property is reported")
}

Method TestInvalidPaths()
{
    Set tMsg = ..Failure("{""a"":""text""}", "a.b=Password", ..Cred())
    Do $$$AssertTrue(tMsg [ "'a' is not a JSON object", "path through a non-object is rejected")
    Set tMsg = ..Failure("{}", "a..b=Password", ..Cred())
    Do $$$AssertTrue(tMsg [ "invalid config path", "empty path part is rejected")
    Set tMsg = ..Failure("{}", "password", ..Cred())
    Do $$$AssertTrue(tMsg [ "must look like path=Property", "entry without = is rejected")
}

Method TestValueAt()
{
    Set tObj = {"a":{"b":"x"},"c":"","d":[1]}
    Do $$$AssertEquals(##class(Airbyte.Demo.Service.Extract).ValueAt(tObj, "a.b"), "x", "nested value")
    Do $$$AssertEquals(##class(Airbyte.Demo.Service.Extract).ValueAt(tObj, "a.zz"), "", "missing")
    Do $$$AssertEquals(##class(Airbyte.Demo.Service.Extract).ValueAt(tObj, "a"), "", "object is not a value")
    Do $$$AssertEquals(##class(Airbyte.Demo.Service.Extract).ValueAt(tObj, "c.x"), "", "path through a string")
}

}
