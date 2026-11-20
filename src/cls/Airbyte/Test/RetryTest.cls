/// Tests for how Extract classifies errors: retry, stop for the operator, or skip
Class Airbyte.Test.RetryTest Extends %UnitTest.TestCase
{

Method TestTransientErrorsAreRetried()
{
    Set tX = "Airbyte.Demo.Service.Extract"
    Do $$$AssertTrue($ClassMethod(tX, "IsTransient", "unreachable", 0), "worker unreachable")
    Do $$$AssertTrue($ClassMethod(tX, "IsTransient", "", 503), "HTTP 5xx without category")
    Do $$$AssertTrue($ClassMethod(tX, "IsTransient", "connection", 502), "category connection")
    Do $$$AssertTrue($ClassMethod(tX, "IsTransient", "timeout", 504), "category timeout")
    Do $$$AssertTrue($ClassMethod(tX, "IsTransient", "internal", 0), "category internal, from a read status")
    Do $$$AssertTrue($ClassMethod(tX, "IsTransient", "connector", 0), "category connector")
}

Method TestPermanentErrorsAreNot()
{
    Set tX = "Airbyte.Demo.Service.Extract"
    For tCat = "auth", "config", "validation", "not_found" {
        Do $$$AssertNotTrue($ClassMethod(tX, "IsTransient", tCat, 400), tCat_" is not retried")
        Do $$$AssertEquals($ClassMethod(tX, "Classify", tCat, 0), "config", tCat_" needs the operator")
    }
    Do $$$AssertNotTrue($ClassMethod(tX, "IsTransient", "cancelled", 0), "cancelled")
    Do $$$AssertNotTrue($ClassMethod(tX, "IsTransient", "store", 0), "store error")
    Do $$$AssertNotTrue($ClassMethod(tX, "IsTransient", "", 404), "other 4xx")
}

Method TestBusyIsSkipped()
{
    Set tX = "Airbyte.Demo.Service.Extract"
    Do $$$AssertEquals($ClassMethod(tX, "Classify", "busy", 409), "busy", "409 busy")
    Do $$$AssertEquals($ClassMethod(tX, "Classify", "", 409), "busy", "409 alone")
    Do $$$AssertNotTrue($ClassMethod(tX, "IsTransient", "busy", 409), "busy is not retried")
}

}
