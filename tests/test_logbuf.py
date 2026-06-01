from __future__ import annotations

import asyncio

import pytest

from mcp_supervisord.logbuf import MAX_LINE_BYTES, RingBuffer, pump_stream


def test_ring_overflow_drops_oldest():
    rb = RingBuffer(3)
    for i in range(5):
        rb.append("stdout", f"l{i}")
    msgs = [x["message"] for x in rb.tail(10)]
    assert msgs == ["l2", "l3", "l4"]


def test_tail_filters_by_stream():
    rb = RingBuffer(10)
    rb.append("stdout", "a")
    rb.append("stderr", "b")
    rb.append("stdout", "c")
    assert [x["message"] for x in rb.tail(10, "stdout")] == ["a", "c"]
    assert [x["message"] for x in rb.tail(10, "stderr")] == ["b"]
    assert len(rb.tail(10, "all")) == 3


def test_truncated_flag_set_for_big_lines():
    rb = RingBuffer(2)
    big = "x" * (MAX_LINE_BYTES + 100)
    rb.append("stdout", big[:MAX_LINE_BYTES], truncated=True)
    line = rb.tail(1)[0]
    assert line.get("truncated") is True
    assert len(line["message"]) == MAX_LINE_BYTES


async def _feed(reader: asyncio.StreamReader, data: bytes) -> None:
    reader.feed_data(data)
    reader.feed_eof()


@pytest.mark.asyncio
async def test_pump_stream_splits_lines():
    rb = RingBuffer(10)
    reader = asyncio.StreamReader(limit=64 * 1024)
    asyncio.create_task(_feed(reader, b"hello\nworld\n"))
    await pump_stream(reader, rb, "stdout")
    msgs = [x["message"] for x in rb.tail(10)]
    assert msgs == ["hello", "world"]
    assert all(x["stream"] == "stdout" for x in rb.tail(10))
