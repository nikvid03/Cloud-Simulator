import logging
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")


def _window_label(ts_ms: int) -> str:
    """Round ts_ms down to the nearest 30-min boundary, return HH:MM label in IST."""
    if ts_ms <= 0:
        return "??:??"
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=_IST)
    minute_floor = (dt.minute // 30) * 30
    return f"{dt.hour:02d}:{minute_floor:02d}"


class DataLossTracker:
    def __init__(self):
        self._stale_total = 0
        self._stale_by_instrument: dict = defaultdict(int)
        self._stale_by_window: dict = defaultdict(int)

        self._filter_total = 0
        self._filter_by_instrument: dict = defaultdict(int)
        self._filter_by_window: dict = defaultdict(int)

        self._failed_chunks: list[tuple[str, int, int]] = []

    def record_stale_drop(self, instrument: str, ts_ms: int) -> None:
        self._stale_total += 1
        self._stale_by_instrument[instrument] += 1
        self._stale_by_window[_window_label(ts_ms)] += 1
        logger.debug("[DataLoss] STALE  %s  ts=%d", instrument, ts_ms)

    def record_filter_drop(self, instrument: str, ts_ms: int) -> None:
        self._filter_total += 1
        self._filter_by_instrument[instrument] += 1
        self._filter_by_window[_window_label(ts_ms)] += 1
        logger.debug("[DataLoss] FILTER %s  ts=%d", instrument, ts_ms)

    def record_failed_chunk(self, date: str, start_ts: int, end_ts: int) -> None:
        self._failed_chunks.append((date, start_ts, end_ts))
        logger.debug("[DataLoss] CHUNK FAILED %s %d→%d", date, start_ts, end_ts)

    def print_summary(self) -> None:
        total_ticks = self._stale_total + self._filter_total
        lines = [
            "",
            "============================================================",
            "  DATA LOSS SUMMARY",
            "============================================================",
            f"  Stale Drops       : {self._stale_total:,}",
            f"  Filter Drops      : {self._filter_total:,}",
            f"  Failed Chunks     : {len(self._failed_chunks):,}",
            "  " + "-" * 26,
            f"  Total tick loss   : {total_ticks:,}  across"
            f" {len(self._failed_chunks)} failed fetch windows",
        ]

        if self._stale_total:
            lines += [
                "",
                "------------------------------------------------------------",
                "  STALE DROPS BY INSTRUMENT (top 10)",
                "------------------------------------------------------------",
            ]
            top = sorted(self._stale_by_instrument.items(), key=lambda x: x[1], reverse=True)[:10]
            for inst, count in top:
                lines.append(f"  {inst:<45} {count:,}")
            lines += ["", "  STALE DROPS BY TIME WINDOW", "  " + "-" * 30]
            for window, count in sorted(self._stale_by_window.items()):
                lines.append(f"  {window}    {count:,}")

        if self._filter_total:
            lines += [
                "",
                "------------------------------------------------------------",
                "  FILTER DROPS BY INSTRUMENT (top 10)",
                "------------------------------------------------------------",
            ]
            top = sorted(self._filter_by_instrument.items(), key=lambda x: x[1], reverse=True)[:10]
            for inst, count in top:
                lines.append(f"  {inst:<45} {count:,}")
            lines += ["", "  FILTER DROPS BY TIME WINDOW", "  " + "-" * 30]
            for window, count in sorted(self._filter_by_window.items()):
                lines.append(f"  {window}    {count:,}")

        if self._failed_chunks:
            lines += [
                "",
                "------------------------------------------------------------",
                "  FAILED FETCH CHUNKS",
                "------------------------------------------------------------",
            ]
            for date, start_ts, end_ts in self._failed_chunks:
                lines.append(
                    f"  {date}  {_window_label(start_ts)} – {_window_label(end_ts)}"
                    f"   (all ticks in window lost)"
                )

        lines.append("============================================================")
        output = "\n".join(lines)
        print(output)
        logger.debug(output)
