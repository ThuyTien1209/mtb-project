import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import gspread 
    import plotly.express as px
    import pandas as pd
    import numpy as np
    import altair as alt
    import matplotlib.pyplot as plt
    import plotly.graph_objects as go

    from datetime import datetime, date


    return date, go, gspread, mo, pd, px


@app.cell
def _(gspread):
    gc = gspread.service_account(filename="D:\DKSH\metabase_project\src\metabase_project\configs\key.json")
    return (gc,)


@app.cell
def _(gc):
    GDATA = gc.open_by_key('139VrY0tzOXxmmMhZflIq07M-qAh5zeo7O-F1LijfjME')
    return (GDATA,)


@app.cell
def _(GDATA):
    worksheet = GDATA.get_worksheet(0)
    data = worksheet.get_all_values()
    return (data,)


@app.cell
def _(data, pd):
    df = pd.DataFrame(data[1:], columns=data[0])
    return (df,)


@app.cell
def _(df, pd):
    df[['gross_sales', 'tax', 'net_sales']]=df[['gross_sales', 'tax', 'net_sales']].astype('float')
    df['quantity']=df['quantity'].astype('int64')
    df['updated_at']=pd.to_datetime(df['updated_at'])
    return


@app.cell
def _(mo):
    ###################
    # HEADER
    ###################

    header = mo.md("# Sales Performance Dashboard").center()
    return (header,)


@app.cell
def _(df, mo):
    city_filter = mo.ui.dropdown(options=df["customer_city"].unique(), label="City")
    state_filter = mo.ui.dropdown(options=df['customer_state'].unique(), label='State')
    product_filter = mo.ui.dropdown(options=df['product_name'].unique(), label='Product')
    return


@app.cell
def _(date, df, mo, pd):
    df["created_at"] = pd.to_datetime(df["created_at"])
    min_date = df["created_at"].min().date()
    max_date = date.today()

    # Tạo widget
    date_filter = mo.ui.date_range(
        start=min_date,
        stop=max_date,
        value=[min_date, max_date],
        label="Date Range"
    )
    return (date_filter,)


@app.cell
def _(df, mo):
    days_options=df["created_at"].dt.day_name().unique()

    day_filter = mo.ui.multiselect(
        options=days_options,
        value=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
        label="Day Name"
    )
    return (day_filter,)


@app.cell
def _(df, mo):
    month_options=df['created_at'].dt.month_name().unique()

    months_ordered = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    month_filter = mo.ui.multiselect(
        options=months_ordered,
        value=["January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"],
        label='Month'
    )
    return (month_filter,)


@app.cell
def _(df, mo):
    year_options = sorted(df['created_at'].dt.year.unique().tolist(), reverse=True)

    year_filter = mo.ui.multiselect(
        options=year_options, 
        value=year_options, 
        label="Year"
    )
    return (year_filter,)


@app.cell
def _(df, mo):
    category_filter = mo.ui.multiselect(
        options=df.category.unique(),
        value=df.category.unique(),
        label='Category'
    )

    source_filter = mo.ui.multiselect(
        options=df.customer_source.unique(),
        value=df.customer_source.unique(),
        label='Source'
    )
    return


@app.cell
def _(date_filter, day_filter, mo, month_filter, year_filter):
    ########################
    # FILTER
    ########################

    filters = mo.hstack([date_filter, year_filter, month_filter, day_filter], justify="space-between", gap=2)
    return (filters,)


@app.cell
def _(date_filter, day_filter, df, month_filter, year_filter):
    start_date, end_date = date_filter.value
    df_filtered = df[
        (df['created_at'].dt.date >= start_date) & 
        (df['created_at'].dt.date <= end_date) &
        (df.created_at.dt.year.isin(year_filter.value)) &
        (df.created_at.dt.month_name().isin(month_filter.value)) &
        (df.created_at.dt.day_name().isin(day_filter.value))]
    return (df_filtered,)


@app.cell
def _(df_filtered):
    total_net_sales = df_filtered.net_sales.sum()
    total_orders = df_filtered.sales_id.count()
    total_quantity = df_filtered.quantity.sum()

    aov = total_net_sales/total_orders if total_orders > 0 else 0
    upt = total_quantity/total_orders if total_orders > 0 else 0
    return aov, upt


@app.cell
def _(aov, df_filtered, mo, upt):
    ###################
    # KPI CARDS
    ###################

    metrics = mo.hstack([
        mo.stat(label="Total Revenue", value=f"${df_filtered.net_sales.sum():,.0f}"),
        mo.stat(label="Total Orders", value=f"${df_filtered.sales_id.count():,.0f}"),
        mo.stat(label="Total Units Sold", value=f"${df_filtered.quantity.sum():,.0f}"),
        mo.stat(label='Total Tax Spent', value=f"${df_filtered.tax.sum():,.0f}"),
        mo.stat(label='AOV', value=f"${aov:,.2f}"),
        mo.stat(label='UPT', value=f"${upt:,.2f}")

    ], justify="space-around")
    return (metrics,)


@app.cell
def _(df_filtered):
    df_filtered["quarter_year"] = df_filtered["created_at"].dt.to_period("Q").astype(str)
    df_filtered['net_sales'] = df_filtered['net_sales'].astype(float)

    revenue_trend = df_filtered.groupby("quarter_year")["net_sales"].sum().reset_index()
    revenue_trend = revenue_trend.sort_values("quarter_year")
    revenue_trend.net_sales = revenue_trend.net_sales.round(2)
    return (revenue_trend,)


@app.cell
def _(revenue_trend):
    all_quarters = revenue_trend['quarter_year'].unique().tolist()
    visible_ticks = [q for q in all_quarters if ('Q1' in q or 'Q3' in q)]
    return (visible_ticks,)


@app.cell
def _(px, revenue_trend, visible_ticks):
    ######################
    # CHART 1
    ######################

    revenue_chart = px.line(
        revenue_trend, 
        x="quarter_year", 
        y="net_sales",
        title='Net sales over time',
        markers=True
    )

    revenue_chart = revenue_chart.update_xaxes(
        tickmode='array',
        tickvals=visible_ticks,
        ticktext=visible_ticks
    )
    return (revenue_chart,)


@app.cell
def _(df_filtered, pd):
    ######################
    # CHART 2
    ######################

    df_filtered["weekday"] = df_filtered["created_at"].dt.day_name()
    weekday_data = df_filtered.groupby("weekday").agg({
        "net_sales": "sum",
        "quantity": "sum" 
    }).reset_index()

    days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_data['weekday'] = pd.Categorical(weekday_data['weekday'], categories=days_order, ordered=True)
    weekday_data = weekday_data.sort_values("weekday")
    return days_order, weekday_data


@app.cell
def _(go, weekday_data):
    fig_weekday = go.Figure()

    fig_weekday.add_trace(go.Bar(
        x=weekday_data["weekday"],
        y=weekday_data["quantity"],
        name="Quantity Sold",
        marker_color='#000000', # Màu đỏ cho số lượng
        yaxis="y1"
    ))

    fig_weekday.add_trace(go.Scatter(
        x=weekday_data["weekday"],
        y=weekday_data["net_sales"],
        name="Net Sales",
        mode="lines+markers",
        line=dict(color='#FFA500', width=3), # Màu xanh cho doanh thu
        yaxis="y2"
    ))

    fig_weekday.update_layout(
        title="Total Net Sales and Quantity Sold by Weekday",
        xaxis=dict(title="Day of week"),

        # Trục Y bên trái cho Quantity
        yaxis=dict(
            title="Quantity Sold",
            rangemode="tozero",
            title_font=dict(color="#e74c3c"),
            tickfont=dict(color="#e74c3c")
        ),

        # Trục Y bên phải cho Net Sales
        yaxis2=dict(
            title="Net Sales ($)",
            rangemode="tozero",
            title_font=dict(color="#3498db"),
            tickfont=dict(color="#3498db"),
            tickformat=",.0f", # Định dạng số có dấu phẩy
            overlaying="y",
            side="right"
        ),

        legend=dict(x=1.1, y=1),
        template="plotly_white",
        hovermode="x unified"
    )

    weekday_chart=fig_weekday
    return (weekday_chart,)


@app.cell
def _():
    return


@app.cell
def _(days_order, df_filtered, pd):
    #####################
    # CHART 3
    #####################

    df_filtered["hour"] = df_filtered["created_at"].dt.hour

    heatmap_data = df_filtered.groupby(["weekday", "hour"])["net_sales"].sum().reset_index()

    heatmap_data['weekday'] = pd.Categorical(heatmap_data['weekday'], categories=days_order, ordered=True)
    heatmap_data = heatmap_data.sort_values(["weekday", "hour"])
    return (heatmap_data,)


@app.cell
def _(heatmap_data, px):
    fig_heatmap = px.density_heatmap(
        heatmap_data,
        x="hour",
        y="weekday",
        z="net_sales",
        title="Revenue by Weekday and Hour",
        labels={'hour': 'Hour', 'weekday': 'Day', 'net_sales': 'Revenue'},
        color_continuous_scale="Viridis", # Hoặc "RdBu", "Magma", "Plasma"
        text_auto=".2s", # Hiển thị số rút gọn trên các ô
        template="plotly_white"
    )

    # Tùy chỉnh để trục X hiện đủ 24 giờ
    fig_heatmap.update_layout(
        xaxis=dict(dtick=1),
    )

    heatmap_chart = fig_heatmap
    return (heatmap_chart,)


@app.cell
def _(heatmap_chart, mo, revenue_chart, weekday_chart):
    charts = mo.vstack([
        mo.as_html(revenue_chart),
        mo.hstack([weekday_chart, heatmap_chart], justify='center')
    ])
    return (charts,)


@app.cell
def _():
    #mo.sidebar(
        #item=mo.vstack([
            #city_filter,
            #state_filter,
            #product_filter,
            #category_filter,
            #source_filter,
    #])
    #)
    return


@app.cell
def _(charts, df, filters, header, metrics, mo):
    mo.vstack([
        header,
        metrics,
        filters,
        charts,
        mo.ui.dataframe(df)
    ], gap=2)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
