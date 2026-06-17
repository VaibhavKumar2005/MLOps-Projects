from src.Monitoring.drift_monitor import detect_drift

def test_no_drift():
    score = detect_drift(
        [1,2,3],
        [1,2,3]
    )

    assert score < 0.1