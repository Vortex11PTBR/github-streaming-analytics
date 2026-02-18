"""Analytics helpers: read Parquet, compute stats, generate Plotly visuals,
export markdown reports and serve dashboards via Flask/Streamlit.

Designed to be a lightweight, production-friendly module with type hints
and clear interfaces for integration into notebooks or web apps.
"""

from __future__ import annotations

import os
from typing import Any, Dict

import pandas as pd  # type: ignore[import-untyped]
import plotly.express as px  # type: ignore[import-untyped]
import plotly.io as pio  # type: ignore[import-untyped]
import structlog
from flask import Flask, render_template_string

logger = structlog.get_logger()


def load_parquet(path: str) -> pd.DataFrame:
    """Load Parquet data into a pandas DataFrame using pyarrow backend.

    Args:
        path: file or directory containing Parquet files.
    """
    df = pd.read_parquet(path, engine="pyarrow")
    return df


def descriptive_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """Compute descriptive statistics and the specific metrics required.

    Returns a dict with summaries useful for reports.
    """
    stats: Dict[str, object] = {}
    stats["rows"] = len(df)
    stats["columns"] = list(df.columns)

    # Top 10 repos by events
    if "repo_name" in df.columns:
        top_repos = df["repo_name"].value_counts().head(10).to_dict()
    else:
        top_repos = {}
    stats["top_repos"] = top_repos

    # Event type distribution
    if "type" in df.columns:
        types = df["type"].value_counts().to_dict()
    else:
        types = {}
    stats["event_types"] = types

    # Commits per repo (if commits array flattened into rows elsewhere)
    if "commits" in df.columns:
        commits_count = (
            df["commits"]
            .apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 0)
            .sum()
        )
    else:
        commits_count = 0
    stats["commits_count"] = commits_count

    # Sentiment distribution if present
    if "sentiment" in df.columns:
        sentiment = df["sentiment"].value_counts().to_dict()
    else:
        sentiment = {}
    stats["sentiment"] = sentiment

    return stats


def make_figures(df: pd.DataFrame) -> Dict[str, Any]:
    """Create interactive Plotly figures for common analytics.

    Returns a dict mapping names to Plotly figure objects.
    """
    figs: Dict[str, object] = {}

    if "repo_name" in df.columns:
        repo_counts = df["repo_name"].value_counts().reset_index()
        repo_counts.columns = ["repo", "events"]
        figs["top_repos"] = px.bar(
            repo_counts.head(10), x="repo", y="events", title="Top 10 Repos by Events"
        )

    if "type" in df.columns:
        types = df["type"].value_counts().reset_index()
        types.columns = ["type", "count"]
        figs["event_types"] = px.pie(
            types, names="type", values="count", title="Event Types Distribution"
        )

    if "event_ts" in df.columns:
        ts = pd.to_datetime(df["event_ts"]).dt.floor("min")
        ts_counts = ts.value_counts().sort_index().reset_index()
        ts_counts.columns = ["event_ts", "count"]
        figs["events_time_series"] = px.line(
            ts_counts, x="event_ts", y="count", title="Events Over Time"
        )

    if "sentiment" in df.columns:
        sent = df["sentiment"].value_counts().reset_index()
        sent.columns = ["sentiment", "count"]
        figs["sentiment"] = px.bar(
            sent, x="sentiment", y="count", title="Commit Sentiment Distribution"
        )

    return figs


def export_report_markdown(
    df: pd.DataFrame, out_dir: str, report_name: str = "report.md"
) -> str:
    """Export a markdown report with descriptive stats and embedded figure images.

    Requires `kaleido` to save Plotly figures as PNG files.

    Returns the path to the generated markdown file.
    """
    os.makedirs(out_dir, exist_ok=True)
    stats = descriptive_stats(df)
    figs = make_figures(df)

    images_dir = os.path.join(out_dir, "images")
    os.makedirs(images_dir, exist_ok=True)

    md_lines = ["# Analytics Report", "", "## Summary", ""]
    md_lines.append(f"- Rows: {stats['rows']}")
    md_lines.append(f"- Columns: {', '.join(stats['columns'])}")
    md_lines.append("")

    # Top repos
    md_lines.append("## Top Repositories")
    for repo, cnt in stats.get("top_repos", {}).items():
        md_lines.append(f"- {repo}: {cnt}")
    md_lines.append("")

    # Event types
    md_lines.append("## Event Types")
    for t, cnt in stats.get("event_types", {}).items():
        md_lines.append(f"- {t}: {cnt}")
    md_lines.append("")

    # Save figures and reference
    for name, fig in figs.items():
        img_path = os.path.join(images_dir, f"{name}.png")
        try:
            fig.write_image(img_path)
            md_lines.append(f"### {name}")
            md_lines.append(f"![{name}]({os.path.relpath(img_path, out_dir)})")
            md_lines.append("")
        except Exception as e:
            logger.warning("figure_export_failed", name=name, error=str(e))

    md_path = os.path.join(out_dir, report_name)
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(md_lines))

    return md_path


def serve_dashboard_flask(
    df: pd.DataFrame, host: str = "0.0.0.0", port: int = 8501
) -> None:
    """Serve a simple Flask dashboard embedding Plotly charts.

    This function runs a Flask app; call from CLI or orchestrator.
    """
    app = Flask(__name__)
    figs = make_figures(df)

    @app.route("/")
    def index():
        parts = []
        for name, fig in figs.items():
            html = pio.to_html(fig, full_html=False, include_plotlyjs="cdn")
            parts.append(f"<h2>{name}</h2>" + html)
        page = (
            "<html><head><title>GitHub Events Dashboard</title></head><body>"
            + "".join(parts)
            + "</body></html>"
        )
        return render_template_string(page)

    app.run(host=host, port=port)


def serve_dashboard_streamlit(df: pd.DataFrame) -> None:
    """Run a Streamlit dashboard showing the main figures.

    Call this function from a script that is executed with `streamlit run`.
    """
    try:
        import streamlit as st

        st.set_page_config(layout="wide")
        st.title("GitHub Events Analytics")
        figs = make_figures(df)
        for name, fig in figs.items():
            st.subheader(name)
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        logger.exception("streamlit_run_failed", error=str(e))


if __name__ == "__main__":
    # simple CLI: load parquet path env PARQUET_PATH and run streamlit by default
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--parquet", help="path to parquet directory or file", required=True
    )
    parser.add_argument(
        "--serve",
        choices=["flask", "streamlit"],
        default="streamlit",
    )
    args = parser.parse_args()

    df = load_parquet(args.parquet)
    if args.serve == "flask":
        serve_dashboard_flask(df)
    else:
        # Streamlit expects to be run with `streamlit run`.
        # We allow direct invocation for convenience.
        serve_dashboard_streamlit(df)
