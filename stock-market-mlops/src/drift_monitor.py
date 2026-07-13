"""Compatibility entrypoint for drift monitoring automation."""

from src.Monitoring.drift_monitor import DriftMonitor


if __name__ == "__main__":
    monitor = DriftMonitor(window_size=100, drift_threshold=2.0)
    monitor.run()
