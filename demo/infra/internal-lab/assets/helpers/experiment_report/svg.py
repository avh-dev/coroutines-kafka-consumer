from __future__ import annotations

import base64
import html
import math
from pathlib import Path
from typing import Any

from .model import ExperimentReport


PALETTE = ["#2563eb", "#7c3aed", "#0891b2", "#059669", "#d97706", "#dc2626", "#4f46e5", "#64748b"]
TOPIC_COLORS = ("#bfdbfe", "#ddd6fe", "#ccfbf1", "#fde68a")
TOPIC_BOUNDARY_COLORS = ("#60a5fa", "#a78bfa", "#2dd4bf", "#fbbf24")
ICON_ROOT = Path(__file__).resolve().parent / "icons" / "services"
ACTION_COLORS = {
    "delete": "#dc2626",
    "crash": "#dc2626",
    "restart": "#2563eb",
    "degradation": "#d97706",
    "network": "#7c3aed",
    "outage": "#b91c1c",
    "chaos": "#64748b",
}
SERVICE_BADGES = {
    "ckc-demo": ("kubernetes", "K8S", "#326ce5"),
    "demo-stubs": ("demo-stubs", "STB", "#475569"),
    "redis": ("redis", "R", "#dc382d"),
    "kafka": ("kafka", "K", "#231f20"),
    "audit": ("audit", "AUD", "#0f766e"),
}


def esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def svg_document(width: int, height: int, body: list[str], title: str) -> str:
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="{esc(title)}">',
            "<style>",
            "text{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;fill:#1f2937}",
            ".title{font-size:18px;font-weight:600}.label{font-size:12px}.muted{font-size:11px;fill:#6b7280}",
            ".axis-label{font-size:11px;font-weight:500;fill:#374151}",
            ".card-title{font-size:12px;font-weight:600}.card-time{font-size:11px;fill:#4b5563}.icon-letter{font-size:9px;font-weight:700;fill:white}",
            ".table-head{font-size:10px;font-weight:600;fill:#4b5563}.table-cell{font-size:10px;fill:#374151}",
            ".table-base{fill:#6b7280}.table-arrow{font-weight:600}.table-new{font-weight:700}",
            "</style>",
            *body,
            "</svg>",
            "",
        ]
    )


def format_duration(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    if rounded == 0:
        return "0m"
    hours, remainder = divmod(rounded, 3600)
    minutes, seconds_remainder = divmod(remainder, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds_remainder or not parts:
        parts.append(f"{seconds_remainder}s")
    return " ".join(parts)


def format_phase_duration(seconds: float) -> str:
    return format_duration(seconds)


def nice_step(maximum: float, target_ticks: int = 5) -> float:
    rough = maximum / max(1, target_ticks)
    if rough <= 0:
        return 1
    exponent = 10 ** math.floor(math.log10(rough))
    fraction = rough / exponent
    nice_fraction = next(value for value in (1, 2, 5, 10) if fraction <= value)
    return nice_fraction * exponent


def horizontal_tick_seconds(total_seconds: float, plot_width: float = 885, minimum_spacing: float = 78) -> int:
    maximum_intervals = max(1, int(plot_width // minimum_spacing))
    target = total_seconds / maximum_intervals
    for candidate in (10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 21600):
        if candidate >= target:
            return candidate
    return 43200


def format_tps(value: float) -> str:
    return f"{int(round(value)):,}"


def smoothed_line_path(points: list[tuple[float, float]], radius: float = 10) -> str:
    unique = []
    for point in points:
        if not unique or point != unique[-1]:
            unique.append(point)
    if not unique:
        return ""
    if len(unique) == 1:
        return f"M {unique[0][0]:.1f} {unique[0][1]:.1f}"
    commands = [f"M {unique[0][0]:.1f} {unique[0][1]:.1f}"]
    for index in range(1, len(unique) - 1):
        previous = unique[index - 1]
        current = unique[index]
        following = unique[index + 1]
        before_distance = math.hypot(current[0] - previous[0], current[1] - previous[1])
        after_distance = math.hypot(following[0] - current[0], following[1] - current[1])
        trim = min(radius, before_distance / 3, after_distance / 3)
        before = (
            current[0] + (previous[0] - current[0]) * trim / before_distance,
            current[1] + (previous[1] - current[1]) * trim / before_distance,
        )
        after = (
            current[0] + (following[0] - current[0]) * trim / after_distance,
            current[1] + (following[1] - current[1]) * trim / after_distance,
        )
        commands.append(f"L {before[0]:.1f} {before[1]:.1f}")
        commands.append(f"Q {current[0]:.1f} {current[1]:.1f} {after[0]:.1f} {after[1]:.1f}")
    commands.append(f"L {unique[-1][0]:.1f} {unique[-1][1]:.1f}")
    return " ".join(commands)


def action_icon(action: str, x: float, y: float, size: float = 28) -> str:
    color = ACTION_COLORS.get(action, ACTION_COLORS["chaos"])
    center_x = x + size / 2
    center_y = y + size / 2
    common = 'stroke="white" stroke-width="1.8" fill="none" stroke-linecap="round" stroke-linejoin="round"'
    if action == "delete":
        symbol = (
            f'<path d="M {x+9:.1f} {y+10:.1f} L {x+10:.1f} {y+22:.1f} L {x+18:.1f} {y+22:.1f} L {x+19:.1f} {y+10:.1f}" {common}/>'
            f'<line x1="{x+7:.1f}" y1="{y+8:.1f}" x2="{x+21:.1f}" y2="{y+8:.1f}" {common}/>'
            f'<line x1="{x+11:.1f}" y1="{y+5:.1f}" x2="{x+17:.1f}" y2="{y+5:.1f}" {common}/>'
        )
    elif action == "crash":
        symbol = f'<path d="M {x+16:.1f} {y+4:.1f} L {x+8:.1f} {y+16:.1f} H {x+14:.1f} L {x+12:.1f} {y+24:.1f} L {x+21:.1f} {y+12:.1f} H {x+15:.1f} Z" fill="white"/>'
    elif action == "restart":
        symbol = (
            f'<path d="M {x+21:.1f} {y+10:.1f} A 9 9 0 1 0 {x+21:.1f} {y+19:.1f}" {common}/>'
            f'<path d="M {x+18:.1f} {y+6:.1f} L {x+22:.1f} {y+10:.1f} L {x+17:.1f} {y+11:.1f}" {common}/>'
        )
    elif action == "degradation":
        symbol = (
            f'<path d="M {x+7:.1f} {y+19:.1f} A 8 8 0 0 1 {x+21:.1f} {y+19:.1f}" {common}/>'
            f'<line x1="{center_x:.1f}" y1="{y+18:.1f}" x2="{x+10:.1f}" y2="{y+13:.1f}" {common}/>'
            f'<circle cx="{center_x:.1f}" cy="{y+18:.1f}" r="1.5" fill="white"/>'
        )
    elif action == "network":
        symbol = (
            f'<circle cx="{x+8:.1f}" cy="{y+9:.1f}" r="2.5" {common}/>'
            f'<circle cx="{x+20:.1f}" cy="{y+19:.1f}" r="2.5" {common}/>'
            f'<line x1="{x+10:.1f}" y1="{y+11:.1f}" x2="{x+18:.1f}" y2="{y+17:.1f}" {common}/>'
            f'<line x1="{x+7:.1f}" y1="{y+22:.1f}" x2="{x+21:.1f}" y2="{y+6:.1f}" {common}/>'
        )
    elif action == "outage":
        symbol = (
            f'<path d="M {x+9:.1f} {y+9:.1f} A 9 9 0 1 0 {x+19:.1f} {y+9:.1f}" {common}/>'
            f'<line x1="{center_x:.1f}" y1="{y+4:.1f}" x2="{center_x:.1f}" y2="{y+14:.1f}" {common}/>'
        )
    else:
        symbol = f'<text class="icon-letter" x="{center_x:.1f}" y="{center_y+3:.1f}" text-anchor="middle">!</text>'
    return (
        f'<g data-icon-role="action" data-action="{esc(action)}">'
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{size}" height="{size}" rx="6" fill="{color}"/>'
        f'{symbol}</g>'
    )


def service_icon_data(target: str) -> tuple[str, str, str, str | None]:
    asset_name, fallback, color = SERVICE_BADGES.get(
        target,
        (target or "unknown", (target[:3] or "?").upper(), "#64748b"),
    )
    for extension, mime_type in (("svg", "image/svg+xml"), ("png", "image/png"), ("webp", "image/webp")):
        path = ICON_ROOT / f"{asset_name}.{extension}"
        if path.is_file():
            encoded = base64.b64encode(path.read_bytes()).decode("ascii")
            return fallback, color, asset_name, f"data:{mime_type};base64,{encoded}"
    return fallback, color, asset_name, None


def service_icon(target: str, x: float, y: float, size: float = 28) -> str:
    fallback, color, asset_name, data_uri = service_icon_data(target)
    badge = "" if data_uri else f'<rect x="{x:.1f}" y="{y:.1f}" width="{size}" height="{size}" rx="6" fill="{color}"/>'
    content = (
        f'<image x="{x+1:.1f}" y="{y+1:.1f}" width="{size-2:.1f}" height="{size-2:.1f}" '
        f'href="{data_uri}" preserveAspectRatio="xMidYMid meet"/>'
        if data_uri
        else f'<text class="icon-letter" x="{x+size/2:.1f}" y="{y+size/2+3:.1f}" text-anchor="middle">{esc(fallback)}</text>'
    )
    return (
        f'<g data-icon-role="service" data-service="{esc(target)}" data-asset="{esc(asset_name)}">'
        f'{badge}'
        f'{content}</g>'
    )


def stubs_table_rows(scenario: dict[str, Any]) -> list[dict[str, Any]]:
    table = scenario.get("stubs_changes")
    if not isinstance(table, dict) or not isinstance(table.get("rows"), list):
        return []
    return [row for row in table["rows"] if isinstance(row, dict)]


def chaos_card_dimensions(scenario: dict[str, Any]) -> tuple[float, float]:
    rows = stubs_table_rows(scenario)
    if rows:
        return 600, 76 + len(rows) * 27
    title = str(scenario.get("title") or scenario.get("type") or "Chaos")
    at = float(scenario.get("at_seconds") or 0)
    duration = scenario.get("duration_seconds")
    end = float(scenario.get("end_seconds") or at)
    time_label = (
        f"{format_duration(at)}–{format_duration(end)} · {format_duration(float(duration))}"
        if duration is not None
        else format_duration(at)
    )
    return min(480, max(260, 104 + len(title) * 7.0 + len(time_label) * 6.2)), 38


def stubs_table_svg(
    scenario: dict[str, Any],
    card_x: float,
    card_y: float,
    card_width: float,
    color: str,
) -> list[str]:
    rows = stubs_table_rows(scenario)
    if not rows:
        return []
    columns = ("p90", "p95", "p99", "p100", "errors")
    table_x = card_x + 10
    table_y = card_y + 42
    table_width = card_width - 20
    name_width = 150
    value_width = (table_width - name_width) / len(columns)
    row_height = 27
    result = [
        f'<line x1="{table_x:.1f}" y1="{table_y-5:.1f}" x2="{table_x+table_width:.1f}" y2="{table_y-5:.1f}" stroke="#e5e7eb"/>',
    ]
    for index, column in enumerate(columns):
        cell_x = table_x + name_width + index * value_width
        label = f"{column}, ms" if column != "errors" else "errors, %"
        result.append(
            f'<text class="table-head" x="{cell_x+value_width/2:.1f}" y="{table_y+12:.1f}" text-anchor="middle">{label}</text>'
        )
    for row_index, row in enumerate(rows):
        row_y = table_y + 21 + row_index * row_height
        if row_index % 2 == 0:
            result.append(
                f'<rect x="{table_x:.1f}" y="{row_y-7:.1f}" width="{table_width:.1f}" height="{row_height}" rx="3" fill="#f8fafc"/>'
            )
        result.append(
            f'<text class="table-cell" x="{table_x+6:.1f}" y="{row_y+10:.1f}">{esc(row.get("name") or row.get("id") or "downstream")}</text>'
        )
        values = row.get("values") if isinstance(row.get("values"), dict) else {}
        for column_index, column in enumerate(columns):
            value = values.get(column) if isinstance(values.get(column), dict) else {}
            base = value.get("base")
            new = value.get("new")
            changed = bool(value.get("changed"))
            cell_x = table_x + name_width + column_index * value_width
            center_x = cell_x + value_width / 2
            if changed:
                result.append(
                    f'<rect data-stubs-cell="changed" x="{cell_x+3:.1f}" y="{row_y-5:.1f}" width="{value_width-6:.1f}" height="{row_height-4:.1f}" rx="4" fill="{color}" fill-opacity="0.10"/>'
                )
                result.append(
                    f'<text class="table-cell" x="{center_x:.1f}" y="{row_y+10:.1f}" text-anchor="middle">'
                    f'<tspan class="table-base">{esc(base)}</tspan>'
                    f'<tspan class="table-arrow" fill="{color}"> → </tspan>'
                    f'<tspan class="table-new" fill="{color}">{esc(new)}</tspan></text>'
                )
            else:
                result.append(
                    f'<text class="table-cell" x="{center_x:.1f}" y="{row_y+10:.1f}" text-anchor="middle">{esc(new)}</text>'
                )
    return result


def load_profile_svg(report: ExperimentReport) -> str:
    width = 1000
    phases = report.test_definition.get("load_phases", [])
    load_topics = [
        topic
        for topic in report.test_definition.get("load_topics", [])
        if isinstance(topic, dict) and float(topic.get("percent") or 0) > 0
    ]
    left, right = 75, 20
    top = 92 if load_topics else 28
    plot_height = 220
    axis_y = top + plot_height
    planned_chaos_scenarios = [
        scenario
        for scenario in report.test_definition.get("chaos_scenarios", [])
        if isinstance(scenario, dict)
    ]
    observed_events = [
        event
        for event in report.test_definition.get("observed_events", [])
        if isinstance(event, dict)
    ]
    chaos_scenarios = (
        [
            {
                "at_seconds": event.get("at_seconds", 0),
                "type": event.get("type", "event"),
                "action": "chaos",
                "target": (event.get("details") or {}).get("target") or (event.get("details") or {}).get("topic") or event.get("source", "event"),
                "title": f"{event.get('target_name', '')}: {event.get('title') or event.get('type')} [{event.get('status', '')}]",
            }
            for event in observed_events
        ]
        if observed_events
        else planned_chaos_scenarios
    )
    card_dimensions = [chaos_card_dimensions(scenario) for scenario in chaos_scenarios]
    card_gap = 10
    cards_height = sum(card_height for _card_width, card_height in card_dimensions)
    cards_height += max(0, len(card_dimensions) - 1) * card_gap
    height = axis_y + 52 + max(38, cards_height) + 18
    plot_width = width - left - right
    load_total = sum(float(phase["duration_seconds"]) for phase in phases)
    chaos_total = max(
        [
            float(scenario.get("end_seconds") or scenario.get("at_seconds") or 0)
            for scenario in chaos_scenarios
        ],
        default=0,
    )
    total = max(1, load_total, chaos_total)
    x_tick = horizontal_tick_seconds(total)
    x_ticks = []
    elapsed = 0
    while elapsed <= total:
        x_ticks.append((elapsed, left + plot_width * elapsed / total))
        elapsed += x_tick
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

    body = []
    if load_topics:
        legend_slot_width = plot_width / len(load_topics)
        for index, topic in enumerate(load_topics):
            percent = float(topic.get("percent") or 0)
            topic_max_tps = maximum_tps * percent / 100
            item_x = left + index * legend_slot_width
            color = TOPIC_COLORS[index % len(TOPIC_COLORS)]
            body.extend(
                [
                    f'<rect data-topic-legend="{esc(topic.get("topic"))}" x="{item_x:.1f}" y="34" width="12" height="12" rx="2" fill="{color}" stroke="{TOPIC_BOUNDARY_COLORS[index % len(TOPIC_BOUNDARY_COLORS)]}" stroke-width="0.8"/>',
                    f'<text class="axis-label" x="{item_x+19:.1f}" y="44">{esc(topic.get("topic"))} · {format_tps(percent)}% · max {format_tps(topic_max_tps)} TPS</text>',
                ]
            )
    grid_lines = []
    tick = 0.0
    while tick <= axis_max_tps + tps_step / 2:
        py = y(tick)
        grid_lines.append(
            f'<line x1="{left}" y1="{py:.1f}" x2="{width-right}" y2="{py:.1f}" stroke="#e5e7eb"/>'
        )
        body.append(
            f'<text class="axis-label" x="{left-10}" y="{py+4:.1f}" text-anchor="end">{format_tps(tick)}</text>'
        )
        tick += tps_step
    for _elapsed, px in x_ticks:
        grid_lines.append(
            f'<line data-grid-axis="x" x1="{px:.1f}" y1="{top}" x2="{px:.1f}" y2="{axis_y}" stroke="#e5e7eb"/>'
        )
    body.append(
        f'<text class="label" x="22" y="{top+plot_height/2:.1f}" text-anchor="middle" '
        f'transform="rotate(-90 22 {top+plot_height/2:.1f})">TPS</text>'
    )
    body.extend(grid_lines)
    points = []
    load_vertices = []
    phase_labels = []
    for phase in phases:
        start = float(phase["start_seconds"])
        end = start + float(phase["duration_seconds"])
        start_point = (x(start), y(phase_tps(phase["start_percent"])))
        end_point = (x(end), y(phase_tps(phase["end_percent"])))
        points.extend([start_point, end_point])
        load_vertices.extend(
            [
                (start, phase_tps(phase["start_percent"])),
                (end, phase_tps(phase["end_percent"])),
            ]
        )
        dx = end_point[0] - start_point[0]
        dy = end_point[1] - start_point[1]
        length = max(1.0, math.hypot(dx, dy))
        label_x = (start_point[0] + end_point[0]) / 2 + 15 * dy / length
        label_y = (start_point[1] + end_point[1]) / 2 - 15 * dx / length
        angle = math.degrees(math.atan2(dy, dx))
        label = f"{phase['name']} · {format_phase_duration(float(phase['duration_seconds']))}"
        phase_labels.append(
            f'<text class="label" x="{label_x:.1f}" y="{label_y:.1f}" text-anchor="middle" '
            f'transform="rotate({angle:.2f} {label_x:.1f} {label_y:.1f})">{esc(label)}</text>'
        )
    if points:
        area = " ".join(f"{px:.1f},{py:.1f}" for px, py in points)
        polygon = f"{left},{axis_y} {area} {width-right},{axis_y}"
        normal_ranges = [(0.0, total)]
        clip_paths = []
        for index, (range_start, range_end) in enumerate(normal_ranges):
            clip_paths.append(
                f'<clipPath id="load-fill-{index}"><rect x="{x(range_start):.1f}" y="{top}" '
                f'width="{max(0, x(range_end)-x(range_start)):.1f}" height="{plot_height}"/></clipPath>'
            )
        if clip_paths:
            body.append(f'<defs>{"".join(clip_paths)}</defs>')
        if len(load_topics) > 1:
            total_topic_percent = sum(float(topic.get("percent") or 0) for topic in load_topics)
            cumulative_percent = 0.0
            topic_polygons = []
            topic_boundaries = []
            for topic_index, topic in enumerate(load_topics):
                lower_percent = cumulative_percent / total_topic_percent
                cumulative_percent += float(topic.get("percent") or 0)
                upper_percent = cumulative_percent / total_topic_percent
                upper = [(x(seconds), y(tps * upper_percent)) for seconds, tps in load_vertices]
                lower = [(x(seconds), y(tps * lower_percent)) for seconds, tps in reversed(load_vertices)]
                topic_polygon = " ".join(f"{px:.1f},{py:.1f}" for px, py in [*upper, *lower])
                topic_polygons.append((topic, TOPIC_COLORS[topic_index % len(TOPIC_COLORS)], topic_polygon))
                if topic_index < len(load_topics) - 1:
                    topic_boundaries.append(
                        (
                            topic,
                            TOPIC_BOUNDARY_COLORS[topic_index % len(TOPIC_BOUNDARY_COLORS)],
                            smoothed_line_path(upper, radius=7),
                        )
                    )
            for range_index, _normal_range in enumerate(normal_ranges):
                for topic, color, topic_polygon in topic_polygons:
                    body.append(
                        f'<polygon data-profile-fill="topic" data-topic="{esc(topic.get("topic"))}" '
                        f'points="{topic_polygon}" fill="{color}" opacity="0.46" '
                        f'clip-path="url(#load-fill-{range_index})"/>'
                    )
                for topic, color, boundary_path in topic_boundaries:
                    body.append(
                        f'<path data-topic-boundary="{esc(topic.get("topic"))}" d="{boundary_path}" '
                        f'fill="none" stroke="{color}" stroke-width="1" stroke-opacity="0.72" '
                        f'clip-path="url(#load-fill-{range_index})"/>'
                    )
        else:
            for index, _normal_range in enumerate(normal_ranges):
                body.append(
                    f'<polygon data-profile-fill="normal" points="{polygon}" fill="#dbeafe" opacity="0.42" '
                    f'clip-path="url(#load-fill-{index})"/>'
                )
        load_path = smoothed_line_path(points)

    chaos_fills = []
    chaos_overlays = []
    chaos_markers = []
    chaos_cards = []
    card_bottom = height - 18
    card_y_positions = []
    card_cursor = card_bottom
    for _card_width, scenario_card_height in card_dimensions:
        card_y = card_cursor - scenario_card_height
        card_y_positions.append(card_y)
        card_cursor = card_y - card_gap
    for index, scenario in enumerate(chaos_scenarios):
        at = float(scenario.get("at_seconds") or 0)
        duration = scenario.get("duration_seconds")
        end = float(scenario.get("end_seconds") or at)
        action = str(scenario.get("action") or "chaos")
        target = str(scenario.get("target") or "")
        title = str(scenario.get("title") or scenario.get("type") or "Chaos")
        color = ACTION_COLORS.get(action, ACTION_COLORS["chaos"])
        start_x = x(at)
        end_x = x(end)
        connector_x = start_x
        estimated_width, card_height = card_dimensions[index]
        card_y = card_y_positions[index]
        if duration is not None:
            time_label = (
                f"{format_duration(at)}–{format_duration(end)}"
                f" · {format_duration(float(duration))}"
            )
        else:
            time_label = format_duration(at)
        table_rows = stubs_table_rows(scenario)
        if table_rows:
            action_x = connector_x - 14
            if connector_x + estimated_width - 19 <= width - 5:
                card_x = max(5, connector_x - 19)
                service_x = action_x + 34
                title_x = action_x + 72
            else:
                card_x = min(width - estimated_width - 5, connector_x - estimated_width + 19)
                service_x = action_x - 34
                title_x = card_x + 10
        elif connector_x + estimated_width - 19 <= width - 5:
            card_x = max(5, connector_x - 19)
            action_x = card_x + 5
            service_x = card_x + 39
            title_x = card_x + 76
        else:
            card_x = min(width - estimated_width - 5, connector_x - estimated_width + 19)
            action_x = card_x + estimated_width - 33
            service_x = action_x - 34
            title_x = card_x + 10
        icon_y = card_y + 5
        title_width = len(title) * 6.8
        time_x = title_x + title_width + 10
        if duration is not None:
            chaos_fills.append(
                f'<rect data-chaos-kind="interval" data-range-background="overlay" '
                f'data-scenario-type="{esc(scenario.get("type"))}" '
                f'x="{start_x:.1f}" y="{top}" width="{max(2, end_x-start_x):.1f}" height="{plot_height}" '
                f'fill="{color}" fill-opacity="0.38"/>'
            )
            chaos_overlays.extend(
                [
                    f'<line data-chaos-boundary="start" x1="{start_x:.1f}" y1="{top}" x2="{start_x:.1f}" y2="{axis_y}" stroke="{color}" stroke-width="1.2" stroke-opacity="0.8"/>',
                    f'<line data-chaos-boundary="end" x1="{end_x:.1f}" y1="{top}" x2="{end_x:.1f}" y2="{axis_y}" stroke="{color}" stroke-width="1.2" stroke-opacity="0.8"/>',
                    f'<line data-chaos-connector="interval" x1="{start_x:.1f}" y1="{axis_y}" x2="{start_x:.1f}" y2="{card_y:.1f}" stroke="{color}" stroke-width="2.4" stroke-opacity="0.82" stroke-dasharray="6 5"/>',
                ]
            )
        else:
            point_y = y(load_at(at))
            chaos_overlays.append(
                f'<line data-chaos-kind="instant" data-scenario-type="{esc(scenario.get("type"))}" '
                f'x1="{start_x:.1f}" y1="{top}" x2="{start_x:.1f}" y2="{card_y:.1f}" '
                f'stroke="{color}" stroke-width="2.4" stroke-opacity="0.82" stroke-dasharray="5 5"/>'
            )
            chaos_markers.append(f'<circle cx="{start_x:.1f}" cy="{point_y:.1f}" r="4.5" fill="{color}" stroke="white" stroke-width="1.5"/>')
        chaos_cards.extend(
            [
                f'<g data-chaos-card="{esc(scenario.get("type"))}"><title>{esc(title)} on {esc(target)} at {esc(time_label)}</title>',
                f'<rect x="{card_x:.1f}" y="{card_y:.1f}" width="{estimated_width:.1f}" height="{card_height}" rx="8" fill="white" fill-opacity="0.96" stroke="#d1d5db"/>',
                action_icon(action, action_x, icon_y),
                service_icon(target, service_x - 1, card_y + 4, 30),
                f'<text class="card-title" x="{title_x:.1f}" y="{card_y+24:.1f}">{esc(title)}</text>',
                f'<text class="card-time" x="{time_x:.1f}" y="{card_y+24:.1f}">· {esc(time_label)}</text>',
                *stubs_table_svg(scenario, card_x, card_y, estimated_width, color),
                "</g>",
            ]
        )

    body.extend(chaos_fills)
    body.extend(chaos_overlays)
    if points:
        body.append(
            f'<path data-load-profile="smoothed" d="{load_path}" fill="none" stroke="#2563eb" '
            'stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>'
        )
    body.extend(chaos_markers)
    body.extend(phase_labels)
    body.append(f'<line x1="{left}" y1="{axis_y}" x2="{width-right}" y2="{axis_y}" stroke="#374151"/>')

    time_labels = []
    for elapsed, px in x_ticks:
        label = format_duration(elapsed)
        label_width = max(34, len(label) * 7 + 10)
        time_labels.extend(
            [
                f'<rect data-time-label-background="true" x="{px-label_width/2:.1f}" y="{axis_y+7}" width="{label_width:.1f}" height="18" rx="3" fill="white" fill-opacity="0.82"/>',
                f'<text class="axis-label" x="{px:.1f}" y="{axis_y+20}" text-anchor="middle">{label}</text>',
            ]
        )
    body.extend(time_labels)
    body.append(
        f'<text class="muted" x="{width-right}" y="{axis_y+40}" text-anchor="end">Elapsed from experiment start</text>'
    )
    body.extend(chaos_cards)
    return svg_document(width, height, body, "Load profile and planned chaos scenarios")


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


def kafka_wire_breakdown_svg(report: ExperimentReport) -> str:
    segments = [
        ("Network headers", "#64748b"),
        ("Other TCP payload", "#cbd5e1"),
        ("Kafka protocol", "#8b5cf6"),
        ("Batch headers", "#f59e0b"),
        ("Compressed records", "#10b981"),
    ]
    rows = []
    for target in report.targets:
        roles = target.pcap_analysis.get("roles", {})
        for role in ("producer", "consumer"):
            data = roles.get(role, {}) if isinstance(roles, dict) else {}
            network = data.get("network", {}) if isinstance(data, dict) else {}
            protocol = data.get("protocol", {}) if isinstance(data, dict) else {}
            batches = protocol.get("record_batches", {}) if isinstance(protocol, dict) else {}
            kafka = int(protocol.get("kafka_pdu_bytes") or 0)
            batch_wire = int(batches.get("batch_wire_bytes") or 0)
            batch_headers = int(batches.get("batch_header_bytes") or 0)
            compressed = int(batches.get("compressed_record_bytes") or 0)
            tcp_payload = int(network.get("tcp_payload_bytes") or 0)
            values = [
                int(network.get("network_header_bytes") or 0),
                max(0, tcp_payload - kafka),
                max(0, kafka - batch_wire),
                batch_headers,
                compressed,
            ]
            rows.append((target.name, role, values, int(network.get("captured_wire_bytes") or 0)))
    width, left, right, top, row_height = 1100, 285, 100, 96, 34
    height = max(210, top + len(rows) * row_height + 38)
    plot_width = width - left - right
    maximum = max((total for _, _, _, total in rows), default=1) or 1
    body = [f'<text class="title" x="24" y="30">Kafka captured-wire breakdown</text>']
    legend_x = 24
    for label, color in segments:
        body.extend([
            f'<rect x="{legend_x}" y="48" width="12" height="12" rx="2" fill="{color}"/>',
            f'<text class="muted" x="{legend_x+18}" y="59">{esc(label)}</text>',
        ])
        legend_x += 42 + len(label) * 7
    for index, (name, role, values, total) in enumerate(rows):
        py = top + index * row_height
        body.append(f'<text class="label" x="{left-12}" y="{py+17}" text-anchor="end">{esc(name)} · {role}</text>')
        body.append(f'<rect x="{left}" y="{py}" width="{plot_width}" height="22" rx="3" fill="#f3f4f6"/>')
        offset = left
        for value, (_, color) in zip(values, segments):
            segment_width = plot_width * value / maximum
            if segment_width > 0:
                body.append(f'<rect x="{offset:.1f}" y="{py}" width="{segment_width:.1f}" height="22" fill="{color}"/>')
            offset += segment_width
        label = f"{total / 1024:.1f} KiB" if total else "unavailable"
        body.append(f'<text class="label" x="{min(offset+8, width-right+5):.1f}" y="{py+16}">{label}</text>')
    return svg_document(width, height, body, "Kafka captured-wire breakdown")
