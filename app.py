from __future__ import annotations

import sys
import asyncio

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import streamlit as st

from connectors.mathworks import scrape_mathworks
from connectors.amazon import scrape_amazon
from storage.db import (
    get_conn,
    init_db,
    upsert_jobs,
    record_run,
    get_last_run,
    get_new_today,
    search_jobs,
    list_recent,
    list_locations,
)
from utils.location import display_location

conn = get_conn()
init_db(conn)

st.set_page_config(page_title="RoleRadar", layout="wide")

companies = [row[0] for row in conn.execute("SELECT DISTINCT company FROM jobs ORDER BY company").fetchall()]
companies = companies or ["MathWorks", "Amazon"]  # fallback for first run

selected_company = st.selectbox("Company", ["(All)"] + companies)

st.title("RoleRadar")
st.caption("Daily job tracker — highlights new postings since your last run (MathWorks example).")

col1, col2, col3 = st.columns([1.1, 1.1, 2.2], vertical_alignment="center")

with col1:
    st.subheader("Controls")
    if st.button("Run update now", type="primary"):
        with st.spinner("Fetching latest postings..."):
            # MathWorks (RSS)
            mw_jobs = scrape_mathworks()
            upsert_jobs(conn, mw_jobs)
            mw_new = get_new_today(conn, "MathWorks")
            record_run(conn, "MathWorks", total_jobs=len(mw_jobs), new_jobs=len(mw_new))

            # Amazon (JSON API)
            amz_jobs = scrape_amazon(base_query="simulation")  # or "" for everything
            upsert_jobs(conn, amz_jobs)
            amz_new = get_new_today(conn, "Amazon")
            record_run(conn, "Amazon", total_jobs=len(amz_jobs), new_jobs=len(amz_new))

        st.success(f"Updated. MathWorks new: {len(mw_new)} | Amazon new: {len(amz_new)}")

with col2:
    st.subheader("Status")
    last = get_last_run(conn, selected_company) if selected_company != "(All)" else None
    if last:
        run_date, ran_at, total_jobs, new_jobs = last
        st.caption(f"Last update: {ran_at} • Total: {total_jobs} • New: {new_jobs}")
    else:
        st.caption("Select a company to see last update details.")
    if last:
        run_date, ran_at, total_jobs, new_jobs = last
        st.metric("Last updated", ran_at)
        st.metric("Total jobs", total_jobs)
        st.metric("New today", new_jobs)
    else:
        st.info("No runs yet. Click **Run update now**.")

with col3:
    st.subheader("New postings (today)")
    show_limit = st.slider(
    "Show at most",
    min_value=10,
    max_value=200,
    value=10,
    step=10,
    )
    new_today = get_new_today(conn, selected_company) if selected_company != "(All)" else []
    if selected_company == "(All)":
        st.caption("Select a company to see new postings.")
    new_today.sort(key=lambda r: r[2], reverse=True)  # first_seen desc
    total_new = len(new_today)
    shown = new_today[:show_limit]
    st.caption(f"Showing first {len(shown)} of {total_new} new postings.")
    if not shown:
        st.write("No new postings found.")
    else:
        for title, url, first_seen, last_seen in shown:
            st.markdown(f"**{title}**  \n{url}")

st.divider()

left, right = st.columns([1, 2.2])

with left:
    st.subheader("Search")
    query = st.text_input("Filter by title keyword", placeholder="e.g., Simulation, Robotics, Engineer")
    raw_locations = list_locations(
        conn,
        None if selected_company == "(All)" else selected_company,
    )

    selected_loc = st.multiselect(
        "Location (multi-select)",
        raw_locations,
        default=[],
        help="Select one or more locations (e.g. all MA cities)",
    )

    st.caption("Tip: Run once per day and review only the “New postings” panel.")

with right:
    st.subheader("All tracked jobs (recent)")

    company_filter = None if selected_company == "(All)" else selected_company

    rows = (
        search_jobs(conn, company_filter, query)
        if query.strip()
        else list_recent(conn, company_filter)
    )

    if selected_loc:
        rows = [r for r in rows if r[2] in selected_loc]

    if not rows:
        st.write("No results.")
    else:
        # --- Option 2: Pagination ---
        page_size = st.selectbox(
            "Rows per page",
            [25, 50, 100],
            index=1,  # default 50
            key="all_jobs_page_size",
        )

        total = len(rows)
        max_page = max(1, (total - 1) // page_size + 1)

        page = st.number_input(
            "Page",
            min_value=1,
            max_value=max_page,
            value=1,
            step=1,
            key="all_jobs_page",
        )

        start = (page - 1) * page_size
        end = start + page_size
        page_rows = rows[start:end]

        st.caption(f"Showing {start + 1}–{min(end, total)} of {total} jobs")

        for title, url, location, first_seen, last_seen in page_rows:
            st.markdown(
                f"**{title}** ({display_location(location)})  \n{url}  \n"
                f"<small>First seen: {first_seen} • Last seen: {last_seen}</small>",
                unsafe_allow_html=True,
            )

