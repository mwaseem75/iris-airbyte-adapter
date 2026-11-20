"""The connector's real error reason is read from its log file so the category is right."""
from app import read_job

TRACE = ('2026-11-20 08:39:40 - INFO - {"type":"TRACE","trace":{"type":"ERROR","emitted_at":1.0,"error":'
         '{"message":"State code: 28P01; Message: FATAL: password authentication failed for user \\"demo\\"",'
         '"internal_message":"x"}}}\n')


def test_reason_comes_from_the_connector_log(tmp_path):
    log = tmp_path / "connector.log"
    log.write_text("noise\n" + TRACE, encoding="utf-8")
    text = f"Connector failed. (AirbyteConnectorFailedError)\n    Log file: {log}\n"
    reason = read_job.log_reason(text)
    assert reason == 'State code: 28P01; Message: FATAL: password authentication failed for user "demo"'
    assert read_job.classify(reason) == "auth"


def test_no_log_file_gives_none(tmp_path):
    assert read_job.log_reason("Connector failed.") is None
    assert read_job.log_reason(f"Log file: {tmp_path / 'missing.log'}") is None


def test_connection_refused_is_a_connection_error():
    assert read_job.classify("Connection to host.docker.internal:5433 refused. Connection refused") == "connection"
