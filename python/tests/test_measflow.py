"""Tests for the measflow Python reader/writer."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import numpy as np
import pytest

from measflow import MeasReader, MeasWriter, MeasDataType, MeasTimestamp, MeasValue


DEMO_FILE = Path(__file__).parents[2] / "demo_measurement.meas"


@pytest.fixture
def tmp_meas(tmp_path):
    return str(tmp_path / "test.meas")


# ── Roundtrip tests ──────────────────────────────────────────────────────────

def test_roundtrip_float32(tmp_meas):
    data = np.array([1.0, 2.5, -3.14, 0.0, 1e6], dtype=np.float32)
    with MeasWriter(tmp_meas) as w:
        g = w.add_group("Sensors")
        ch = g.add_channel("Voltage", MeasDataType.Float32)
        ch.write_bulk(data.tolist())

    with MeasReader(tmp_meas) as r:
        result = r["Sensors"]["Voltage"].read_all()

    np.testing.assert_array_almost_equal(result, data)


def test_roundtrip_float64(tmp_meas):
    data = np.linspace(0, 1, 100)
    with MeasWriter(tmp_meas) as w:
        g = w.add_group("Data")
        ch = g.add_channel("Signal", MeasDataType.Float64)
        ch.write_bulk(data.tolist())

    with MeasReader(tmp_meas) as r:
        result = r["Data"]["Signal"].read_all()

    np.testing.assert_array_almost_equal(result, data)


def test_roundtrip_int32(tmp_meas):
    data = [-1000, 0, 42, 32767, -32768]
    with MeasWriter(tmp_meas) as w:
        g = w.add_group("G")
        ch = g.add_channel("Count", MeasDataType.Int32)
        ch.write_bulk(data)

    with MeasReader(tmp_meas) as r:
        result = r["G"]["Count"].read_all().tolist()

    assert result == data


def test_roundtrip_timestamp(tmp_meas):
    ts0 = MeasTimestamp.now()
    from datetime import timedelta
    timestamps = [ts0 + timedelta(milliseconds=i) for i in range(5)]

    with MeasWriter(tmp_meas) as w:
        g = w.add_group("Log")
        ch = g.add_channel("Time", MeasDataType.Timestamp)
        ch.write_bulk(timestamps)

    with MeasReader(tmp_meas) as r:
        result = r["Log"]["Time"].read_timestamps()

    assert len(result) == 5
    for expected, actual in zip(timestamps, result):
        assert expected.nanoseconds == actual.nanoseconds


def test_sample_count(tmp_meas):
    with MeasWriter(tmp_meas) as w:
        g = w.add_group("G")
        ch = g.add_channel("X", MeasDataType.Float32)
        for v in range(42):
            ch.write(float(v))

    with MeasReader(tmp_meas) as r:
        assert r["G"]["X"].sample_count == 42


# ── Multiple channels & groups ───────────────────────────────────────────────

def test_multiple_channels(tmp_meas):
    rng = np.random.default_rng(0)
    a = rng.random(50).astype(np.float32)
    b = rng.random(50).astype(np.float64)

    with MeasWriter(tmp_meas) as w:
        g = w.add_group("Motor")
        cha = g.add_channel("RPM", MeasDataType.Float32)
        chb = g.add_channel("Temp", MeasDataType.Float64)
        cha.write_bulk(a.tolist())
        chb.write_bulk(b.tolist())

    with MeasReader(tmp_meas) as r:
        np.testing.assert_array_almost_equal(r["Motor"]["RPM"].read_all(), a)
        np.testing.assert_array_almost_equal(r["Motor"]["Temp"].read_all(), b)


def test_multiple_groups(tmp_meas):
    with MeasWriter(tmp_meas) as w:
        g1 = w.add_group("GroupA")
        g1.add_channel("X", MeasDataType.Float32).write_bulk([1.0, 2.0, 3.0])
        g2 = w.add_group("GroupB")
        g2.add_channel("Y", MeasDataType.Float64).write_bulk([10.0, 20.0])

    with MeasReader(tmp_meas) as r:
        assert len(r.groups) == 2
        assert r["GroupA"]["X"].sample_count == 3
        assert r["GroupB"]["Y"].sample_count == 2


# ── Properties ───────────────────────────────────────────────────────────────

def test_group_properties(tmp_meas):
    with MeasWriter(tmp_meas) as w:
        g = w.add_group("Test")
        g.properties["Operator"] = "Alice"
        g.properties["Run"] = 42
        g.add_channel("V", MeasDataType.Float32).write(1.0)

    with MeasReader(tmp_meas) as r:
        props = r["Test"].properties
        assert props["Operator"].value == "Alice"
        assert props["Run"].value == 42


def test_channel_properties(tmp_meas):
    with MeasWriter(tmp_meas) as w:
        g = w.add_group("G")
        ch = g.add_channel("Voltage", MeasDataType.Float64)
        ch.properties["Unit"] = "V"
        ch.write(3.14)

    with MeasReader(tmp_meas) as r:
        assert r["G"]["Voltage"].properties["Unit"].value == "V"


# ── Interoperability ─────────────────────────────────────────────────────────

def test_read_csharp_demo_file():
    """Read the C#-generated demo file and verify basic structure."""
    with MeasReader(str(DEMO_FILE)) as r:
        assert len(r.groups) >= 1
        motor = r["Motor"]
        assert motor["RPM"].sample_count == 1000
        assert motor["OilTemperature"].sample_count == 1000

        rpm = motor["RPM"].read_all()
        assert len(rpm) == 1000
        # Sanity: RPM values should be in a plausible range
        assert rpm.min() > 2000
        assert rpm.max() < 4000
