from __future__ import annotations

import html
import math
from typing import Any

from .analyze import duration_value_seconds
from .model import ExperimentReport


PALETTE = ["#2563eb", "#7c3aed", "#0891b2", "#059669", "#d97706", "#dc2626", "#4f46e5", "#64748b"]


def esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def svg_document(width: int, height: int, body: list[str], title: str) -> str:
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}">',
            "<style>",
            "text{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;fill:#1f2937}",
            ".title{font-size:18px;font-weight:600}.label{font-size:12px}.muted{font-size:11px;fill:#6b7280}",
            "</style>",
            *body,
            "</svg>",
            "",
        ]
    )


def format_minutes_seconds(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    minutes, remainder = divmod(rounded, 60)
    return f"{minutes}m {remainder}s"


def format_elapsed_clock(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    hours, remainder = divmod(rounded, 3600)
    minutes = remainder // 60
    return f"{hours:02d}:{minutes:02d}"


def nice_step(maximum: float, target_ticks: int = 5) -> float:
    rough = maximum / max(1, target_ticks)
    if rough <= 0:
        return 1
    exponent = 10 ** math.floor(math.log10(rough))
    fraction = rough / exponent
    nice_fraction = next(value for value in (1, 2, 5, 10) if fraction <= value)
    return nice_fraction * exponent


def horizontal_tick_seconds(total_seconds: float) -> int:
    target = total_seconds / 6
    for candidate in (60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 21600):
        if candidate >= target:
            return candidate
    return 43200


def format_tps(value: float) -> str:
    return f"{int(round(value)):,}"


def load_profile_svg(report: ExperimentReport) -> str:
    width = 1000
    left, right, top = 85, 30, 72
    plot_height = 198
    axis_y = top + plot_height
    chaos_steps = [
        event
        for event in report.test_definition.get("chaos_steps", [])
        if isinstance(event, dict)
    ]
    height = axis_y + 55 + max(1, len(chaos_steps)) * 28
    plot_width = width - left - right
    phases = report.test_definition.get("load_phases", [])
    total = max(1, sum(float(phase["duration_seconds"]) for phase in phases))
    base_tps = float(report.test_definition.get("base_tps") or 0)
    maximum_percent = max(
        [
            100.0,
            *[float(phase["start_percent"]) for phase in phases],
            *[float(phase["end_percent"]) for phase in phases],
        ]
    )
    maximum_tps = max(1.0, base_tps * maximum_percent / 100)
    tps_step = nice_step(maximum_tps)
    axis_max_tps = math.ceil(maximum_tps / tps_step) * tps_step

    def x(seconds: float) -> float:
        return left + plot_width * seconds / total

    def y(tps: float) -> float:
        return top + plot_height * (1 - tps / axis_max_tps)

    def phase_tps(percent: Any) -> float:
        return base_tps * float(percent) / 100

    def load_at(seconds: float) -> float:
        for phase in phases:
            start = float(phase["start_seconds"])
            duration = float(phase["duration_seconds"])
            end = start + duration
            if seconds <= end:
                progress = min(1.0, max(0.0, (seconds - start) / duration))
                start_tps = phase_tps(phase["start_percent"])
                end_tps = phase_tps(phase["end_percent"])
                return start_tps + (end_tps - start_tps) * progress
        return phase_tps(phases[-1]["end_percent"]) if phases else 0

    body = [f'<text class="title" x="{left}" y="28">Load profile and planned chaos events</text>']
    tick = 0.0
    while tick <= axis_max_tps + tps_step / 2:
        py = y(tick)
        body.append(
            f'<line x1="{left}" y1="{py:.1f}" x2="{width-right}" y2="{py:.1f}" stroke="#e5e7eb"/>'
        )
        body.append(
            f'<text class="muted" x="{left-10}" y="{py+4:.1f}" text-anchor="end">{format_tps(tick)}</text>'
        )
        tick += tps_step
    body.append(
        f'<text class="label" x="22" y="{top+plot_height/2:.1f}" text-anchor="middle" '
        f'transform="rotate(-90 22 {top+plot_height/2:.1f})">TPS</text>'
    )
    points = []
    phase_guides = []
    phase_labels = []
    for phase in phases:
        start = float(phase["start_seconds"])
        end = start + float(phase["duration_seconds"])
        start_point = (x(start), y(phase_tps(phase["start_percent"])))
        end_point = (x(end), y(phase_tps(phase["end_percent"])))
        points.extend([start_point, end_point])
        phase_guides.append(
            f'<line x1="{end_point[0]:.1f}" y1="{top}" x2="{end_point[0]:.1f}" y2="{axis_y}" '
            'stroke="#d1d5db" stroke-dasharray="3 4"/>'
        )
        dx = end_point[0] - start_point[0]
        dy = end_point[1] - start_point[1]
        length = max(1.0, math.hypot(dx, dy))
        label_x = (start_point[0] + end_point[0]) / 2 + 15 * dy / length
        label_y = (start_point[1] + end_point[1]) / 2 - 15 * dx / length
        angle = math.degrees(math.atan2(dy, dx))
        label = f"{phase['name']} · {format_minutes_seconds(float(phase['duration_seconds']))}"
        phase_labels.append(
            f'<text class="label" x="{label_x:.1f}" y="{label_y:.1f}" text-anchor="middle" '
            f'transform="rotate({angle:.2f} {label_x:.1f} {label_y:.1f})">{esc(label)}</text>'
        )
    body.extend(phase_guides)
    if points:
        area = " ".join(f"{px:.1f},{py:.1f}" for px, py in points)
        polygon = f"{left},{axis_y} {area} {width-right},{axis_y}"
        body.append(f'<polygon points="{polygon}" fill="#dbeafe" opacity="0.75"/>')
        body.append(f'<polyline points="{area}" fill="none" stroke="#2563eb" stroke-width="3"/>')
    body.extend(phase_labels)
    body.append(f'<line x1="{left}" y1="{axis_y}" x2="{width-right}" y2="{axis_y}" stroke="#374151"/>')
    x_tick = horizontal_tick_seconds(total)
    elapsed = 0
    while elapsed <= total:
        px = x(elapsed)
        body.append(f'<line x1="{px:.1f}" y1="{axis_y}" x2="{px:.1f}" y2="{axis_y+5}" stroke="#374151"/>')
        body.append(
            f'<text class="muted" x="{px:.1f}" y="{axis_y+21}" text-anchor="middle">{format_elapsed_clock(elapsed)}</text>'
        )
        elapsed += x_tick

    chaos_lines = []
    chaos_labels = []
    label_bottom = height - 22
    for index, event in enumerate(sorted(chaos_steps, key=lambda item: duration_value_seconds(str(item.get("at") or "")))):
        at = duration_value_seconds(str(event.get("at") or ""))
        px = x(at)
        point_y = y(load_at(at))
        label_y = label_bottom - index * 28
        label = f"{event.get('type', 'chaos')} · {format_minutes_seconds(at)}"
        estimated_width = max(110, len(label) * 7)
        if px + estimated_width + 12 <= width - right:
            text_x = px + 8
            rect_x = px + 4
            anchor = "start"
        else:
            text_x = px - 8
            rect_x = px - estimated_width - 4
            anchor = "end"
        chaos_lines.extend(
            [
                f'<line x1="{px:.1f}" y1="{point_y:.1f}" x2="{px:.1f}" y2="{label_y-17:.1f}" '
                'stroke="#dc2626" stroke-width="2"/>',
                f'<circle cx="{px:.1f}" cy="{point_y:.1f}" r="4" fill="#dc2626"/>',
            ]
        )
        chaos_labels.extend(
            [
                f'<rect x="{rect_x:.1f}" y="{label_y-16:.1f}" width="{estimated_width}" height="21" fill="white"/>',
                f'<text class="label" x="{text_x:.1f}" y="{label_y:.1f}" text-anchor="{anchor}" '
                f'fill="#991b1b">{esc(label)}</text>',
            ]
        )
    body.extend(chaos_lines)
    body.extend(chaos_labels)
    return svg_document(width, height, body, "Load profile and planned chaos events")


def comparison_values_svg(
    values: list[tuple[str, float | None]],
    title: str,
    unit: str,
) -> str:
    available = [float(value) for _, value in values if value is not None]
    width = 1000
    row_height = 38
    height = max(180, 90 + len(values) * row_height)
    left, right, top = 260, 80, 58
    plot_width = width - left - right
    maximum = max(available, default=1.0)
    if maximum <= 0:
        maximum = 1.0
    body = [f'<text class="title" x="24" y="30">{esc(title)}</text>']
    for index, (name, value) in enumerate(values):
        py = top + index * row_height
        body.append(f'<text class="label" x="{left-12}" y="{py+18}" text-anchor="end">{esc(name)}</text>')
        body.append(f'<rect x="{left}" y="{py}" width="{plot_width}" height="24" rx="3" fill="#f3f4f6"/>')
        if value is None:
            body.append(f'<text class="muted" x="{left+8}" y="{py+17}">unavailable</text>')
            continue
        bar_width = plot_width * float(value) / maximum
        color = PALETTE[index % len(PALETTE)]
        body.append(f'<rect x="{left}" y="{py}" width="{bar_width:.1f}" height="24" rx="3" fill="{color}"/>')
        body.append(f'<text class="label" x="{min(left+bar_width+8, width-right+5):.1f}" y="{py+17}">{float(value):.2f} {esc(unit)}</text>')
    return svg_document(width, height, body, title)


def comparison_bar_svg(report: ExperimentReport, measurement: str, title: str, unit: str) -> str:
    values = [(target.name, target.measurements.get(measurement)) for target in report.targets]
    return comparison_values_svg(values, title, unit)
