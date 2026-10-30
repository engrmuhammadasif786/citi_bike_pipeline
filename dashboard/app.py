"""NYC Citi Bike Lakehouse Executive Analytics Dashboard (Streamlit + Plotly).

Supports dual data-serving backends:
1. Databricks SQL Warehouse (`PIPELINE_ENV=databricks`) querying Unity Catalog `gold.*` Delta tables
2. Local DuckDB / Gold Parquet (`PIPELINE_ENV=local`) for zero-credential peer review
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from citi_bike_pipeline.config import PipelineConfig
from citi_bike_pipeline.ingestion.citibike_ingestor import CitiBikeIngestor
from citi_bike_pipeline.transformations.medallion_local import LocalMedallionPipeline

st.set_page_config(
    page_title="NYC Citi Bike Lakehouse Analytics",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(ttl=600, show_spinner="Loading Gold Medallion tables...")
def load_gold_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    """Load Gold tables from Databricks SQL Warehouse or Local DuckDB/Parquet."""
    config = PipelineConfig()

    if (
        config.pipeline_env == "databricks"
        and config.databricks_host
        and config.databricks_token
        and "xxxx" not in config.databricks_host
    ):
        from databricks import sql as dbsql

        catalog = config.databricks_catalog
        gold = config.gold_schema
        with dbsql.connect(
            server_hostname=config.databricks_host.replace("https://", ""),
            http_path=config.databricks_http_path,
            access_token=config.databricks_token,
        ) as conn:
            cat_df = pd.read_sql(f"SELECT * FROM {catalog}.{gold}.mart_categorical_usage", conn)
            temp_df = pd.read_sql(f"SELECT * FROM {catalog}.{gold}.mart_temporal_mobility", conn)
            stat_df = pd.read_sql(f"SELECT * FROM {catalog}.{gold}.{ 'dim_stations' }", conn)
        return cat_df, temp_df, stat_df, f"Databricks Unity Catalog ({catalog}.{gold})"

    cat_parquet = config.gold_dir / "mart_categorical_usage.parquet"
    temp_parquet = config.gold_dir / "mart_temporal_mobility.parquet"
    stat_parquet = config.gold_dir / "dim_stations.parquet"

    if not (cat_parquet.exists() and temp_parquet.exists() and stat_parquet.exists()):
        ingestor = CitiBikeIngestor(config)
        ingestor.ingest_all(upload_s3=False)
        local_pipeline = LocalMedallionPipeline(config)
        local_pipeline.run_bronze()
        local_pipeline.run_silver()
        local_pipeline.run_gold()

    cat_df = pd.read_parquet(cat_parquet)
    temp_df = pd.read_parquet(temp_parquet)
    stat_df = pd.read_parquet(stat_parquet)
    return cat_df, temp_df, stat_df, "Local Medallion Lakehouse (DuckDB + Gold Parquet)"


def render_dashboard() -> None:
    cat_df, temp_df, stat_df, backend_label = load_gold_tables()

    st.title("🚲 NYC Citi Bike Mobility & Fleet Analytics Dashboard")
    st.caption(
        f"**Architecture:** AWS S3 Data Lake → Databricks Medallion Lakehouse (PySpark + Delta Lake + dbt) | "
        f"**Active Serving Layer:** `{backend_label}`"
    )

    # Sidebar Filters
    st.sidebar.header("🔎 Interactive Filters")
    all_months = sorted(cat_df["trip_year_month"].unique().tolist())
    selected_months = st.sidebar.multiselect(
        "Trip Year-Month Partition",
        options=all_months,
        default=all_months,
    )

    all_riders = sorted(cat_df["rider_type"].unique().tolist())
    selected_riders = st.sidebar.multiselect(
        "Rider Segment",
        options=all_riders,
        default=all_riders,
    )

    all_bikes = sorted(cat_df["rideable_type_clean"].unique().tolist())
    selected_bikes = st.sidebar.multiselect(
        "Bike Fleet Type",
        options=all_bikes,
        default=all_bikes,
    )

    if not selected_months or not selected_riders or not selected_bikes:
        st.warning("Please select at least one value for each filter in the sidebar.")
        return

    filtered_cat = cat_df[
        cat_df["trip_year_month"].isin(selected_months)
        & cat_df["rider_type"].isin(selected_riders)
        & cat_df["rideable_type_clean"].isin(selected_bikes)
    ].copy()

    filtered_temp = temp_df[
        temp_df["trip_year_month"].isin(selected_months)
        & temp_df["rider_type"].isin(selected_riders)
        & temp_df["rideable_type_clean"].isin(selected_bikes)
    ].copy()

    # Executive Summary KPIs
    total_trips = int(filtered_cat["total_trips"].sum())
    weighted_duration = (
        (filtered_cat["avg_duration_minutes"] * filtered_cat["total_trips"]).sum() / total_trips
        if total_trips > 0
        else 0.0
    )
    weighted_distance = (
        (filtered_cat["avg_distance_km"] * filtered_cat["total_trips"]).sum() / total_trips
        if total_trips > 0
        else 0.0
    )
    ebike_trips = int(
        filtered_cat.loc[
            filtered_cat["rideable_type_clean"] == "Electric Bike", "total_trips"
        ].sum()
    )
    member_trips = int(
        filtered_cat.loc[filtered_cat["rider_type"] == "Annual Member", "total_trips"].sum()
    )

    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
    kpi1.metric("Total Validated Rides", f"{total_trips:,}")
    kpi2.metric("Avg Trip Duration", f"{weighted_duration:.1f} min")
    kpi3.metric("Avg Trip Distance", f"{weighted_distance:.2f} km")
    kpi4.metric(
        "Electric Bike Adoption",
        f"{(ebike_trips / total_trips * 100.0) if total_trips else 0.0:.1f}%",
    )
    kpi5.metric(
        "Annual Member Share",
        f"{(member_trips / total_trips * 100.0) if total_trips else 0.0:.1f}%",
    )

    st.divider()

    # ==========================================================================
    # TILE 1: Categorical Distribution (Required Rubric Tile #1)
    # ==========================================================================
    st.subheader("📊 Tile 1: Categorical Distribution — Fleet & Commute Window Breakdown")
    col_cat1, col_cat2 = st.columns(2)

    with col_cat1:
        by_time_bucket = (
            filtered_cat.groupby(["time_of_day_bucket", "rider_type", "rideable_type_clean"], as_index=False)[
                "total_trips"
            ]
            .sum()
        )
        bucket_order = [
            "Morning Rush (06-09)",
            "Midday (10-15)",
            "Evening Rush (16-19)",
            "Night / Off-Peak (20-05)",
        ]
        fig_cat_bar = px.bar(
            by_time_bucket,
            x="time_of_day_bucket",
            y="total_trips",
            color="rideable_type_clean",
            barmode="group",
            facet_col="rider_type",
            category_orders={"time_of_day_bucket": bucket_order},
            labels={
                "time_of_day_bucket": "Time-of-Day Window",
                "total_trips": "Total Trips",
                "rideable_type_clean": "Bike Type",
                "rider_type": "Rider Segment",
            },
            title="Ride Volume by Commute Window, Rider Segment & Bike Type",
            color_discrete_sequence=["#0052CC", "#36B37E", "#FFAB00"],
        )
        fig_cat_bar.update_layout(height=420, legend_title_text="Bike Type")
        st.plotly_chart(fig_cat_bar, use_container_width=True)

    with col_cat2:
        by_segment_duration = (
            filtered_cat.groupby(["rider_type", "rideable_type_clean"], as_index=False)
            .apply(
                lambda g: pd.Series(
                    {
                        "total_trips": g["total_trips"].sum(),
                        "avg_duration_minutes": (
                            (g["avg_duration_minutes"] * g["total_trips"]).sum()
                            / max(1, g["total_trips"].sum())
                        ),
                    }
                ),
                include_groups=False,
            )
            .reset_index(drop=True)
        )
        fig_sunburst = px.sunburst(
            by_segment_duration,
            path=["rider_type", "rideable_type_clean"],
            values="total_trips",
            color="avg_duration_minutes",
            color_continuous_scale="Blues",
            title="Categorical Share of Trips (Colored by Avg Duration in Minutes)",
        )
        fig_sunburst.update_layout(height=420)
        st.plotly_chart(fig_sunburst, use_container_width=True)

    st.divider()

    # ==========================================================================
    # TILE 2: Temporal Distribution (Required Rubric Tile #2)
    # ==========================================================================
    st.subheader("📈 Tile 2: Temporal Distribution — Daily Demand Timeline & Hourly Commute Curve")
    col_temp1, col_temp2 = st.columns(2)

    with col_temp1:
        daily_trend = (
            filtered_temp.groupby(["started_date", "rider_type"], as_index=False)["total_trips"]
            .sum()
            .sort_values("started_date")
        )
        fig_daily = px.line(
            daily_trend,
            x="started_date",
            y="total_trips",
            color="rider_type",
            markers=False,
            labels={
                "started_date": "Trip Date",
                "total_trips": "Daily Trips",
                "rider_type": "Rider Segment",
            },
            title="Daily Ride Volume Trend Across Selected Months",
            color_discrete_sequence=["#0052CC", "#FF5630"],
        )
        fig_daily.update_layout(height=420, hovermode="x unified")
        st.plotly_chart(fig_daily, use_container_width=True)

    with col_temp2:
        filtered_temp["day_category"] = filtered_temp["is_weekend"].map(
            {True: "Weekend (Sat-Sun)", False: "Weekday (Mon-Fri)"}
        )
        hourly_profile = (
            filtered_temp.groupby(["start_hour", "day_category"], as_index=False)["total_trips"]
            .sum()
            .sort_values("start_hour")
        )
        fig_hourly = px.area(
            hourly_profile,
            x="start_hour",
            y="total_trips",
            color="day_category",
            labels={
                "start_hour": "Hour of Day (00:00 - 23:00)",
                "total_trips": "Cumulative Trips",
                "day_category": "Day Type",
            },
            title="24-Hour Temporal Demand Profile: Weekday Commute vs. Weekend Leisure",
            color_discrete_sequence=["#0052CC", "#00B8D9"],
        )
        fig_hourly.update_layout(height=420, xaxis=dict(dtick=2))
        st.plotly_chart(fig_hourly, use_container_width=True)

    st.divider()

    # ==========================================================================
    # TILE 3: Bonus Geospatial & Station Utilization Insights
    # ==========================================================================
    st.subheader("🗺️ Tile 3: Station Utilization & Geospatial Hotspots")
    col_st1, col_st2 = st.columns(2)

    with col_st1:
        top_stations = stat_df.nlargest(10, "total_departures").sort_values(
            "total_departures", ascending=True
        )
        fig_stations = px.bar(
            top_stations,
            x="total_departures",
            y="station_name",
            orientation="h",
            color="ebike_share_pct",
            color_continuous_scale="Tealgrn",
            labels={
                "total_departures": "Total Departures",
                "station_name": "Station Name",
                "ebike_share_pct": "E-Bike %",
            },
            title="Top 10 High-Throughput Stations (Colored by E-Bike Share %)",
        )
        fig_stations.update_layout(height=420)
        st.plotly_chart(fig_stations, use_container_width=True)

    with col_st2:
        st.markdown("**Station Performance Summary (`gold.dim_stations`)**")
        st.dataframe(
            stat_df[
                [
                    "station_id",
                    "station_name",
                    "total_departures",
                    "avg_duration_minutes",
                    "avg_distance_km",
                    "member_share_pct",
                    "ebike_share_pct",
                ]
            ].head(12),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    render_dashboard()
