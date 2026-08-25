import pandas as pd
from lib.echorepo import EchoRepo

import streamlit as st

# =========================================================
# Page configuration
# =========================================================

st.set_page_config(
    page_title="ECHOREPO Research Explorer",
    page_icon="🌱",
    layout="wide",
)


# =========================================================
# ECHOREPO styling
# =========================================================

st.markdown(
    """
    <style>
        :root {
            --ech-green: #2e7d32;
            --ech-green-dark: #256a29;
            --ech-sand: #f5efe6;
            --ech-cream: #faf7f0;
            --ech-gold: #d5a11d;
            --ech-ink: #1c1c1c;
        }

        .stApp {
            background-color: var(--ech-cream);
        }

        h1, h2, h3 {
            color: var(--ech-ink);
        }

        [data-testid="stSidebar"] {
            background-color: var(--ech-sand);
        }

        div[data-testid="stMetric"] {
            background: white;
            border: 1px solid rgba(0,0,0,.08);
            border-radius: 12px;
            padding: 1rem;
        }

        .ech-header {
            background: #2e7d32;
            color: white;
            padding: 1.1rem 1.5rem;
            border-radius: 12px;
            margin-bottom: 1.5rem;
        }

        .ech-header h1 {
            color: white;
            margin: 0;
        }

        .ech-header p {
            margin: .25rem 0 0 0;
            opacity: .9;
        }

        button[data-baseweb="tab"] {
            font-weight: 600;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


st.markdown(
    """
    <div class="ech-header">
        <h1>ECHOREPO Research Explorer</h1>
        <p>
            Interactive exploration of canonical ECHOREPO soil,
            laboratory and biodiversity data
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# ECHOREPO connection
# =========================================================

echo = EchoRepo()


@st.cache_data(ttl=300)
def load_samples():
    return echo.samples()


@st.cache_data(ttl=300)
def load_parameters():
    return echo.parameters()


@st.cache_data(ttl=300)
def load_biodiversity():
    return echo.biodiversity()


@st.cache_data(ttl=300)
def load_images():
    return echo.images()


# =========================================================
# Small helpers
# =========================================================


def countries_for(df):
    if "country_code" not in df.columns:
        return []

    return sorted(df["country_code"].dropna().astype(str).unique().tolist())


def csv_download(df):
    return df.to_csv(index=False).encode("utf-8")


# =========================================================
# Global sidebar
# =========================================================

st.sidebar.header("ECHOREPO")

try:
    status = echo.ping()
    st.sidebar.success("ECHOREPO API connected")
except Exception as exc:
    st.sidebar.error("ECHOREPO API unavailable")
    st.sidebar.exception(exc)
    st.stop()


if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()


st.sidebar.caption("Data are loaded from the ECHOREPO canonical API.")


# =========================================================
# Tabs
# =========================================================

tab_samples, tab_lab, tab_biodiversity, tab_images = st.tabs(
    [
        "Samples",
        "Laboratory",
        "Biodiversity",
        "Images",
    ]
)


# =========================================================
# SAMPLES
# =========================================================

with tab_samples:
    st.header("Samples")

    samples = load_samples().copy()

    # Normalize useful fields
    for column in [
        "ph",
        "organic_carbon_pct",
        "lat",
        "lon",
    ]:
        if column in samples.columns:
            samples[column] = pd.to_numeric(
                samples[column],
                errors="coerce",
            )

    if "timestamp_utc" in samples.columns:
        samples["timestamp_utc"] = pd.to_datetime(
            samples["timestamp_utc"],
            errors="coerce",
            utc=True,
        )

    # -------------------------
    # Filters
    # -------------------------

    filter1, filter2 = st.columns(2)

    with filter1:
        sample_country = st.selectbox(
            "Country",
            ["All"] + countries_for(samples),
            key="sample_country",
        )

    with filter2:
        valid_ph = samples["ph"].dropna() if "ph" in samples.columns else pd.Series(dtype=float)

        if not valid_ph.empty:
            ph_min = float(valid_ph.min())
            ph_max = float(valid_ph.max())

            sample_ph = st.slider(
                "Soil pH",
                min_value=ph_min,
                max_value=ph_max,
                value=(ph_min, ph_max),
                step=0.1,
                key="sample_ph",
            )
        else:
            sample_ph = None

    filtered_samples = samples.copy()

    if sample_country != "All":
        filtered_samples = filtered_samples[filtered_samples["country_code"] == sample_country]

    if sample_ph is not None:
        filtered_samples = filtered_samples[
            filtered_samples["ph"].isna()
            | filtered_samples["ph"].between(
                sample_ph[0],
                sample_ph[1],
            )
        ]

    # -------------------------
    # Metrics
    # -------------------------

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Samples",
        f"{len(filtered_samples):,}",
    )

    c2.metric(
        "Countries",
        filtered_samples["country_code"].nunique(),
    )

    c3.metric(
        "Mean pH",
        (f"{filtered_samples['ph'].mean():.2f}" if filtered_samples["ph"].notna().any() else "—"),
    )

    c4.metric(
        "Mean organic carbon",
        (
            f"{filtered_samples['organic_carbon_pct'].mean():.2f}%"
            if (
                "organic_carbon_pct" in filtered_samples.columns
                and filtered_samples["organic_carbon_pct"].notna().any()
            )
            else "—"
        ),
    )

    # -------------------------
    # Charts
    # -------------------------

    chart1, chart2 = st.columns(2)

    with chart1:
        st.subheader("pH distribution")

        if "ph" in filtered_samples.columns:
            ph_counts = filtered_samples["ph"].dropna().round(1).value_counts().sort_index()

            st.bar_chart(ph_counts)

    with chart2:
        st.subheader("Samples by country")

        country_counts = (
            filtered_samples["country_code"].value_counts().sort_values(ascending=False)
        )

        st.bar_chart(country_counts)

    # -------------------------
    # Map
    # -------------------------

    st.subheader("Sample locations")

    if {"lat", "lon"} <= set(filtered_samples.columns):
        map_data = (
            filtered_samples[["lat", "lon"]]
            .dropna()
            .rename(
                columns={
                    "lat": "latitude",
                    "lon": "longitude",
                }
            )
        )

        if not map_data.empty:
            st.map(map_data)
        else:
            st.info("No coordinates available for this selection.")

    # -------------------------
    # Table
    # -------------------------

    st.subheader("Sample data")

    preferred_columns = [
        "sample_id",
        "timestamp_utc",
        "country_code",
        "lat",
        "lon",
        "ph",
        "organic_carbon_pct",
        "earthworms_count",
        "soil_structure_en",
        "soil_texture_en",
        "qa_status",
        "licence",
    ]

    display_columns = [c for c in preferred_columns if c in filtered_samples.columns]

    st.dataframe(
        filtered_samples[display_columns],
        use_container_width=True,
        hide_index=True,
    )

    st.download_button(
        "Download filtered samples",
        data=csv_download(filtered_samples),
        file_name="echorepo_samples.csv",
        mime="text/csv",
    )


# =========================================================
# LABORATORY
# =========================================================

with tab_lab:
    st.header("Laboratory measurements")

    parameters = load_parameters().copy()

    # Numeric version used for plotting/statistics
    parameters["result_numeric"] = pd.to_numeric(
        parameters["result_value"],
        errors="coerce",
    )

    # -----------------------------------------------------
    # Filters
    # -----------------------------------------------------

    f1, f2 = st.columns([1, 2])

    countries = countries_for(parameters)

    with f1:
        lab_country = st.selectbox(
            "Country",
            ["All"] + countries,
            key="lab_country",
        )

    parameter_codes = sorted(parameters["parameter_code"].dropna().astype(str).unique().tolist())

    # Pick a useful default if present
    default_elements = []

    for candidate in ["Cu", "Zn", "Fe"]:
        if candidate in parameter_codes:
            default_elements = [candidate]
            break

    if not default_elements and parameter_codes:
        default_elements = [parameter_codes[0]]

    with f2:
        selected_elements = st.multiselect(
            "Elements / parameters",
            parameter_codes,
            default=default_elements,
            key="lab_elements",
            help=(
                "Select one or several laboratory parameters "
                "whose distributions you want to inspect."
            ),
        )

    # -----------------------------------------------------
    # Apply country filter
    # -----------------------------------------------------

    filtered_parameters = parameters.copy()

    if lab_country != "All":
        filtered_parameters = filtered_parameters[
            filtered_parameters["country_code"] == lab_country
        ]

    # -----------------------------------------------------
    # Metrics
    # -----------------------------------------------------

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Measurements",
        f"{len(filtered_parameters):,}",
    )

    c2.metric(
        "Samples",
        filtered_parameters["sample_id"].nunique(),
    )

    c3.metric(
        "Available parameters",
        filtered_parameters["parameter_code"].nunique(),
    )

    c4.metric(
        "Country",
        lab_country,
    )

    # -----------------------------------------------------
    # Distribution plots
    # -----------------------------------------------------

    st.subheader("Element distributions")

    if not selected_elements:
        st.info("Select at least one element or parameter to display its distribution.")

    else:
        plot_data = filtered_parameters[
            filtered_parameters["parameter_code"].isin(selected_elements)
        ].copy()

        plot_data = plot_data[plot_data["result_numeric"].notna()]

        if plot_data.empty:
            st.warning("No numeric measurements are available for this selection.")

        else:
            # One histogram per selected element.
            #
            # This is preferable to putting all elements onto
            # one x-axis because their concentration ranges can
            # differ dramatically.

            import altair as alt

            chart = (
                alt.Chart(plot_data)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "result_numeric:Q",
                        bin=alt.Bin(maxbins=30),
                        title="Measured value",
                    ),
                    y=alt.Y(
                        "count():Q",
                        title="Number of measurements",
                    ),
                    tooltip=[
                        alt.Tooltip(
                            "parameter_code:N",
                            title="Parameter",
                        ),
                        alt.Tooltip(
                            "count():Q",
                            title="Measurements",
                        ),
                    ],
                )
                .properties(
                    width=700,
                    height=220,
                )
            )

            if len(selected_elements) > 1:
                chart = chart.facet(
                    row=alt.Row(
                        "parameter_code:N",
                        title=None,
                        header=alt.Header(
                            labelFontSize=14,
                            labelFontWeight="bold",
                        ),
                    )
                ).resolve_scale(x="independent")

            else:
                chart = chart.properties(title=selected_elements[0])

            st.altair_chart(
                chart,
                use_container_width=True,
            )

    # -----------------------------------------------------
    # Geographic distribution
    # -----------------------------------------------------

    st.subheader("Geographical distribution")

    # We need sample coordinates from the samples dataset
    samples_for_map = load_samples().copy()

    for column in ["lat", "lon"]:
        if column in samples_for_map.columns:
            samples_for_map[column] = pd.to_numeric(
                samples_for_map[column],
                errors="coerce",
            )

    coord_columns = [
        c for c in ["sample_id", "lat", "lon", "country_code"] if c in samples_for_map.columns
    ]

    samples_for_map = samples_for_map[coord_columns].drop_duplicates(subset=["sample_id"])

    # Element to show on the map:
    # if user selected elements above, default to those;
    # otherwise allow all parameters
    map_element_options = selected_elements if selected_elements else parameter_codes

    g1, g2 = st.columns([2, 1])

    with g1:
        geo_element = st.selectbox(
            "Element / parameter for map",
            map_element_options,
            key="lab_geo_element",
        )

    with g2:
        geo_mode = st.radio(
            "Map style",
            ["Measured points", "Heatmap"],
            horizontal=True,
            key="lab_geo_mode",
        )

    geo_data = filtered_parameters.copy()

    geo_data = geo_data[geo_data["parameter_code"] == geo_element].copy()

    geo_data = geo_data[geo_data["result_numeric"].notna()].copy()

    # Merge coordinates in
    geo_data = geo_data.merge(
        samples_for_map,
        on="sample_id",
        how="left",
        suffixes=("", "_sample"),
    )

    geo_data = geo_data[geo_data["lat"].notna() & geo_data["lon"].notna()].copy()

    if geo_data.empty:
        st.info(
            "No georeferenced laboratory measurements are available "
            "for this element and filter selection."
        )

    else:
        # Robust scaling to reduce the effect of extreme outliers
        vmin = geo_data["result_numeric"].quantile(0.05)
        vmax = geo_data["result_numeric"].quantile(0.95)

        if pd.isna(vmin) or pd.isna(vmax) or vmax <= vmin:
            vmin = float(geo_data["result_numeric"].min())
            vmax = float(geo_data["result_numeric"].max())

        denom = max(vmax - vmin, 1e-9)

        clipped = geo_data["result_numeric"].clip(vmin, vmax)
        norm = (clipped - vmin) / denom

        # Circle size and color for point view
        geo_data["radius"] = 12000 + norm * 50000

        # Low values = yellow, high values = red
        geo_data["color_r"] = 220
        geo_data["color_g"] = (210 - norm * 150).round().astype(int)
        geo_data["color_b"] = (80 - norm * 40).round().clip(lower=20).astype(int)

        import pydeck as pdk

        midpoint = {
            "latitude": float(geo_data["lat"].mean()),
            "longitude": float(geo_data["lon"].mean()),
        }

        tooltip = {
            "html": """
                <b>Sample:</b> {sample_id}<br/>
                <b>Country:</b> {country_code}<br/>
                <b>Parameter:</b> {parameter_code}<br/>
                <b>Value:</b> {result_value} {unit}
            """,
            "style": {
                "backgroundColor": "white",
                "color": "black",
            },
        }

        if geo_mode == "Measured points":
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=geo_data,
                get_position=["lon", "lat"],
                get_radius="radius",
                get_fill_color="[color_r, color_g, color_b, 180]",
                pickable=True,
                stroked=True,
                filled=True,
                radius_min_pixels=4,
                radius_max_pixels=30,
                line_width_min_pixels=1,
            )

        else:
            # Weighted heatmap
            layer = pdk.Layer(
                "HeatmapLayer",
                data=geo_data,
                get_position=["lon", "lat"],
                get_weight="result_numeric",
                pickable=True,
            )

        deck = pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=midpoint["latitude"],
                longitude=midpoint["longitude"],
                zoom=3,
                pitch=0,
            ),
            layers=[layer],
            tooltip=tooltip,
        )

        st.pydeck_chart(
            deck,
            use_container_width=True,
        )

        st.caption(
            f"Showing geographical distribution of {geo_element}. "
            f"Lower-end values are shown in lighter yellow/orange tones, "
            f"higher-end values in darker red tones."
        )

        # Optional summary table for mapped data
        st.write(
            f"Mapped measurements: {len(geo_data):,} "
            f"across {geo_data['sample_id'].nunique():,} samples"
        )
    # -----------------------------------------------------
    # Statistics for selected elements
    # -----------------------------------------------------

    if selected_elements:
        selected_data = filtered_parameters[
            filtered_parameters["parameter_code"].isin(selected_elements)
        ].copy()

        stats = (
            selected_data.groupby("parameter_code")["result_numeric"]
            .agg(
                measurements="count",
                mean="mean",
                median="median",
                std="std",
                minimum="min",
                maximum="max",
            )
            .reset_index()
        )

        st.subheader("Distribution statistics")

        st.dataframe(
            stats,
            use_container_width=True,
            hide_index=True,
        )

    # -----------------------------------------------------
    # Laboratory data table
    # -----------------------------------------------------

    st.subheader("Laboratory data")

    table_data = filtered_parameters.copy()

    # If elements were selected, make the table follow that
    # selection too.
    if selected_elements:
        table_data = table_data[table_data["parameter_code"].isin(selected_elements)]

    lab_columns = [
        c
        for c in [
            "sample_id",
            "country_code",
            "parameter_code",
            "result_value",
            "unit",
            "method_code",
            "analysis_datetime_utc",
            "lab_id",
            "licence",
        ]
        if c in table_data.columns
    ]

    st.dataframe(
        table_data[lab_columns],
        use_container_width=True,
        hide_index=True,
    )

    st.download_button(
        "Download filtered laboratory data",
        data=csv_download(table_data),
        file_name="echorepo_laboratory.csv",
        mime="text/csv",
    )


# =========================================================
# BIODIVERSITY
# =========================================================

with tab_biodiversity:
    st.header("Biodiversity")

    biodiversity = load_biodiversity().copy()

    if "read_count" in biodiversity.columns:
        biodiversity["read_count"] = pd.to_numeric(
            biodiversity["read_count"],
            errors="coerce",
        )

    if "relative_abundance_pct" in biodiversity.columns:
        biodiversity["relative_abundance_pct"] = pd.to_numeric(
            biodiversity["relative_abundance_pct"],
            errors="coerce",
        )

    # -------------------------
    # Filters
    # -------------------------

    b1, b2, b3 = st.columns(3)

    with b1:
        bio_country = st.selectbox(
            "Country",
            ["All"] + countries_for(biodiversity),
            key="bio_country",
        )

    markers = sorted(biodiversity["marker"].dropna().astype(str).unique().tolist())

    with b2:
        bio_marker = st.selectbox(
            "Marker",
            ["All"] + markers,
            key="bio_marker",
        )

    ranks = sorted(biodiversity["taxon_rank"].dropna().astype(str).unique().tolist())

    default_rank_index = ranks.index("Phylum") + 1 if "Phylum" in ranks else 0

    with b3:
        bio_rank = st.selectbox(
            "Taxonomic rank",
            ["All"] + ranks,
            index=default_rank_index,
            key="bio_rank",
        )

    taxon_search = st.text_input(
        "Search taxon",
        "",
        key="bio_taxon",
    )

    filtered_bio = biodiversity.copy()

    if bio_country != "All":
        filtered_bio = filtered_bio[filtered_bio["country_code"] == bio_country]

    if bio_marker != "All":
        filtered_bio = filtered_bio[filtered_bio["marker"] == bio_marker]

    if bio_rank != "All":
        filtered_bio = filtered_bio[filtered_bio["taxon_rank"] == bio_rank]

    # -----------------------------------------------------
    # Scientific-name / taxon selection
    # -----------------------------------------------------

    available_taxa = sorted(filtered_bio["scientific_name"].dropna().astype(str).unique().tolist())

    # Search narrows the dropdown options,
    # but does NOT alter the underlying dataset.
    if taxon_search.strip():
        search = taxon_search.strip().lower()

        available_taxa = [taxon for taxon in available_taxa if search in taxon.lower()]

    default_taxa = available_taxa[:3]

    selected_taxa = st.multiselect(
        "Scientific names / taxa",
        available_taxa,
        default=default_taxa,
        key="bio_selected_taxa",
        help=(
            "Select one or several taxa to compare their "
            "relative-abundance distributions across samples."
        ),
    )

    # -------------------------
    # Metrics
    # -------------------------

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Records",
        f"{len(filtered_bio):,}",
    )

    c2.metric(
        "Samples",
        filtered_bio["sample_id"].nunique(),
    )

    c3.metric(
        "Taxa",
        filtered_bio["scientific_name"].nunique(),
    )

    c4.metric(
        "Countries",
        filtered_bio["country_code"].nunique(),
    )

    # -------------------------
    # Top taxa
    # -------------------------

    st.subheader("Top taxa by mean relative abundance")

    if not filtered_bio.empty and "relative_abundance_pct" in filtered_bio.columns:
        top_taxa = (
            filtered_bio.groupby(
                "scientific_name",
                dropna=True,
            )["relative_abundance_pct"]
            .mean()
            .sort_values(ascending=False)
            .head(20)
        )

        st.bar_chart(top_taxa)

    # -----------------------------------------------------
    # Relative-abundance distributions
    # -----------------------------------------------------

    st.subheader("Taxon abundance distributions")

    if not selected_taxa:
        st.info(
            "Select at least one scientific name / taxon to display its abundance distribution."
        )

    else:
        abundance_plot = filtered_bio[filtered_bio["scientific_name"].isin(selected_taxa)].copy()

        abundance_plot["relative_abundance_pct"] = pd.to_numeric(
            abundance_plot["relative_abundance_pct"],
            errors="coerce",
        )

        abundance_plot = abundance_plot[abundance_plot["relative_abundance_pct"].notna()]

        if abundance_plot.empty:
            st.warning("No relative-abundance measurements are available for this selection.")

        else:
            import altair as alt

            chart = (
                alt.Chart(abundance_plot)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "relative_abundance_pct:Q",
                        bin=alt.Bin(maxbins=25),
                        title="Relative abundance (%)",
                    ),
                    y=alt.Y(
                        "count():Q",
                        title="Number of samples",
                    ),
                    tooltip=[
                        alt.Tooltip(
                            "scientific_name:N",
                            title="Taxon",
                        ),
                        alt.Tooltip(
                            "count():Q",
                            title="Samples",
                        ),
                    ],
                )
                .properties(
                    width=700,
                    height=220,
                )
            )

            if len(selected_taxa) > 1:
                chart = chart.facet(
                    row=alt.Row(
                        "scientific_name:N",
                        title=None,
                        header=alt.Header(
                            labelFontSize=14,
                            labelFontWeight="bold",
                        ),
                    )
                ).resolve_scale(x="independent")

            else:
                chart = chart.properties(title=selected_taxa[0])

            st.altair_chart(
                chart,
                use_container_width=True,
            )

    if selected_taxa:
        selected_bio = filtered_bio[filtered_bio["scientific_name"].isin(selected_taxa)].copy()

        selected_bio["relative_abundance_pct"] = pd.to_numeric(
            selected_bio["relative_abundance_pct"],
            errors="coerce",
        )

        abundance_stats = (
            selected_bio.groupby("scientific_name")["relative_abundance_pct"]
            .agg(
                samples="count",
                mean="mean",
                median="median",
                std="std",
                minimum="min",
                maximum="max",
            )
            .reset_index()
        )

        st.subheader("Abundance statistics")

        st.dataframe(
            abundance_stats,
            use_container_width=True,
            hide_index=True,
        )

    # -----------------------------------------------------
    # Geographical distribution
    # -----------------------------------------------------

    st.subheader("Geographical abundance distribution")

    # Coordinates live in samples, not biodiversity
    samples_for_map = load_samples().copy()

    for column in ["lat", "lon"]:
        if column in samples_for_map.columns:
            samples_for_map[column] = pd.to_numeric(
                samples_for_map[column],
                errors="coerce",
            )

    coord_columns = [
        c
        for c in [
            "sample_id",
            "lat",
            "lon",
            "country_code",
        ]
        if c in samples_for_map.columns
    ]

    samples_for_map = samples_for_map[coord_columns].drop_duplicates(subset=["sample_id"])

    # -----------------------------------------------------
    # Taxon and map controls
    # -----------------------------------------------------

    geo_taxa_options = selected_taxa if selected_taxa else available_taxa

    if not geo_taxa_options:
        st.info("Select a taxon to display its geographical distribution.")

    else:
        g1, g2 = st.columns([2, 1])

        with g1:
            geo_taxon = st.selectbox(
                "Taxon for map",
                geo_taxa_options,
                key="bio_geo_taxon",
            )

        with g2:
            bio_geo_mode = st.radio(
                "Map style",
                [
                    "Measured points",
                    "Heatmap",
                ],
                horizontal=True,
                key="bio_geo_mode",
            )

        # -------------------------------------------------
        # Data for selected taxon
        # -------------------------------------------------

        geo_bio = filtered_bio[filtered_bio["scientific_name"] == geo_taxon].copy()

        geo_bio["relative_abundance_pct"] = pd.to_numeric(
            geo_bio["relative_abundance_pct"],
            errors="coerce",
        )

        geo_bio = geo_bio[geo_bio["relative_abundance_pct"].notna()].copy()

        # Add sample coordinates
        geo_bio = geo_bio.merge(
            samples_for_map,
            on="sample_id",
            how="left",
            suffixes=("", "_sample"),
        )

        geo_bio = geo_bio[geo_bio["lat"].notna() & geo_bio["lon"].notna()].copy()

        if geo_bio.empty:
            st.info(
                "No georeferenced abundance measurements "
                "are available for this taxon and filter selection."
            )

        else:
            import pydeck as pdk

            # ---------------------------------------------
            # Robust colour / size scaling
            # ---------------------------------------------

            values = geo_bio["relative_abundance_pct"]

            vmin = values.quantile(0.05)
            vmax = values.quantile(0.95)

            if pd.isna(vmin) or pd.isna(vmax) or vmax <= vmin:
                vmin = float(values.min())
                vmax = float(values.max())

            denom = max(vmax - vmin, 1e-9)

            clipped = values.clip(
                lower=vmin,
                upper=vmax,
            )

            norm = (clipped - vmin) / denom

            # Circle radius
            geo_bio["radius"] = 12000 + norm * 50000

            # Yellow/orange -> red
            geo_bio["color_r"] = 220

            geo_bio["color_g"] = (210 - norm * 150).round().astype(int)

            geo_bio["color_b"] = (80 - norm * 40).round().clip(lower=20).astype(int)

            # ---------------------------------------------
            # Map centre
            # ---------------------------------------------

            midpoint = {
                "latitude": float(geo_bio["lat"].mean()),
                "longitude": float(geo_bio["lon"].mean()),
            }

            map_zoom = 3 if bio_country == "All" else 5

            tooltip = {
                "html": """
                    <b>Sample:</b> {sample_id}<br/>
                    <b>Country:</b> {country_code}<br/>
                    <b>Taxon:</b> {scientific_name}<br/>
                    <b>Rank:</b> {taxon_rank}<br/>
                    <b>Marker:</b> {marker}<br/>
                    <b>Relative abundance:</b>
                    {relative_abundance_pct}%<br/>
                    <b>Read count:</b> {read_count}
                """,
                "style": {
                    "backgroundColor": "white",
                    "color": "black",
                },
            }

            # ---------------------------------------------
            # Layer
            # ---------------------------------------------

            if bio_geo_mode == "Measured points":
                layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=geo_bio,
                    get_position=[
                        "lon",
                        "lat",
                    ],
                    get_radius="radius",
                    get_fill_color=("[color_r, color_g, color_b, 180]"),
                    pickable=True,
                    stroked=True,
                    filled=True,
                    radius_min_pixels=4,
                    radius_max_pixels=30,
                    line_width_min_pixels=1,
                )

            else:
                layer = pdk.Layer(
                    "HeatmapLayer",
                    data=geo_bio,
                    get_position=[
                        "lon",
                        "lat",
                    ],
                    get_weight=("relative_abundance_pct"),
                    pickable=True,
                )

            deck = pdk.Deck(
                map_style=None,
                initial_view_state=pdk.ViewState(
                    latitude=midpoint["latitude"],
                    longitude=midpoint["longitude"],
                    zoom=map_zoom,
                    pitch=0,
                ),
                layers=[layer],
                tooltip=tooltip,
            )

            st.pydeck_chart(
                deck,
                use_container_width=True,
            )

            st.caption(
                f"Geographical distribution of "
                f"{geo_taxon}. Circle size and colour "
                f"represent relative abundance."
            )

            m1, m2, m3 = st.columns(3)

            m1.metric(
                "Mapped samples",
                f"{geo_bio['sample_id'].nunique():,}",
            )

            m2.metric(
                "Mean abundance",
                (f"{geo_bio['relative_abundance_pct'].mean():.2f}%"),
            )

            m3.metric(
                "Median abundance",
                (f"{geo_bio['relative_abundance_pct'].median():.2f}%"),
            )

    # -------------------------
    # Table
    # -------------------------

    st.subheader("Biodiversity data")

    bio_columns = [
        c
        for c in [
            "sample_id",
            "country_code",
            "marker",
            "taxon_rank",
            "scientific_name",
            "read_count",
            "relative_abundance_pct",
            "ingested_datetime_utc",
            "source_file",
            "licence",
        ]
        if c in filtered_bio.columns
    ]

    st.dataframe(
        filtered_bio[bio_columns],
        use_container_width=True,
        hide_index=True,
    )

    st.download_button(
        "Download filtered biodiversity data",
        data=csv_download(filtered_bio),
        file_name="echorepo_biodiversity.csv",
        mime="text/csv",
    )


# =========================================================
# IMAGES
# =========================================================

with tab_images:
    st.header("Sample images")

    images = load_images().copy()

    i1, i2 = st.columns(2)

    with i1:
        image_country = st.selectbox(
            "Country",
            ["All"] + countries_for(images),
            key="image_country",
        )

    with i2:
        sample_search = st.text_input(
            "Sample ID",
            "",
            key="image_sample",
        )

    filtered_images = images.copy()

    if image_country != "All":
        filtered_images = filtered_images[filtered_images["country_code"] == image_country]

    if sample_search.strip():
        filtered_images = filtered_images[
            filtered_images["sample_id"]
            .fillna("")
            .str.contains(
                sample_search.strip(),
                case=False,
                regex=False,
            )
        ]

    c1, c2 = st.columns(2)

    c1.metric(
        "Images",
        f"{len(filtered_images):,}",
    )

    c2.metric(
        "Samples with images",
        filtered_images["sample_id"].nunique(),
    )

    # -------------------------
    # Image gallery
    # -------------------------

    st.subheader("Preview")

    if "image_url" in filtered_images.columns:
        preview = filtered_images.head(12)

        gallery_cols = st.columns(4)

        for n, (_, row) in enumerate(preview.iterrows()):
            with gallery_cols[n % 4]:
                url = row.get("image_url")

                if pd.notna(url) and str(url).strip():
                    try:
                        st.image(
                            str(url),
                            use_container_width=True,
                        )
                    except Exception:
                        st.caption("Preview unavailable")

                st.caption(
                    str(
                        row.get(
                            "sample_id",
                            "",
                        )
                    )
                )

    # -------------------------
    # Metadata
    # -------------------------

    st.subheader("Image metadata")

    image_columns = [
        c
        for c in [
            "sample_id",
            "country_code",
            "image_id",
            "image_url",
            "image_description_en",
            "image_description_orig",
            "timestamp_utc",
            "licence",
        ]
        if c in filtered_images.columns
    ]

    st.dataframe(
        filtered_images[image_columns],
        use_container_width=True,
        hide_index=True,
    )

    st.download_button(
        "Download image metadata",
        data=csv_download(filtered_images),
        file_name="echorepo_images.csv",
        mime="text/csv",
    )
