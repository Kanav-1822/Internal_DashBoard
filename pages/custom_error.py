import pandas as pd
from dash import dcc, html, Input, Output, dash_table , callback , State
import dash
from exec import execute_postgres_query
import pytz

dash.register_page(__name__,path='/custom-error')

layout = html.Div(
    [
        html.H1(
            "Error Filtered Tenants",
            style={
                "textAlign": "center",
                "color": "#ea0d13",
                "fontSize": "28px",
                "fontWeight": "bold",
                "marginBottom": "20px",
            },
        ),
        dcc.Dropdown(
            id="error-threshold-dropdown",
            options=[
                {"label": "10%", "value": 10},
                {"label": "20%", "value": 20},
                {"label": "30%", "value": 30},
                {"label": "50%", "value": 50},
            ],
            value=20,
            placeholder="Select error rate threshold",
            style={
                "margin": "10px auto",
                "width": "50%",
                "padding": "8px",
                "borderRadius": "5px",
                "border": "1px solid #ddd",
                "boxShadow": "0 2px 4px rgba(0, 0, 0, 0.1)",
            },
        ),
        html.Button(
            "Search",
            id="search-button",
            n_clicks=0,
            style={
                "margin": "10px auto",
                "padding": "10px 20px",
                "backgroundColor": "#ea0d13",
                "color": "white",
                "border": "none",
                "borderRadius": "5px",
                "cursor": "pointer",
                "fontWeight": "bold",
                "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.2)",
            },
        ),
        dash_table.DataTable(
            id="high-error-table",
            columns=[
                {"name": "Tenant ID", "id": "tenant_id"},
                {"name": "Base Domain", "id": "base_domain"},
                {"name": "Error Count", "id": "error_count"},
                {"name": "Total Events", "id": "records_count"},
            ],
            page_size=10,
            style_table={
                "overflowX": "auto",
                "margin": "15px",
                "border": "1px solid #ddd",
                "borderRadius": "5px",
                "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.1)",
            },
            style_header={
                "backgroundColor": "#ea0d13",
                "color": "white",
                "fontWeight": "bold",
                "textAlign": "center",
            },
            style_data={
                "backgroundColor": "#f9f9f9",
                "color": "#333",
                "border": "1px solid #ddd",
            },
            style_data_conditional=[
                {
                    "if": {"row_index": "even"},
                    "backgroundColor": "#f1f1f1",
                },
                {
                    "if": {"state": "active"},
                    "backgroundColor": "#d1d1d1",
                    "border": "1px solid #ea0d13",
                },
                {
                    "if": {"state": "selected"},
                    "backgroundColor": "#ea0d13",
                    "color": "white",
                },
            ],
            style_cell={
                "textAlign": "center",
                "padding": "10px",
                "border": "1px solid #ddd",
            },
            style_as_list_view=True,
        ),
        html.Div(
            id="tenant-details-output",
            style={
                "margin": "20px",
                "padding": "15px",
                "border": "1px solid #ddd",
                "borderRadius": "5px",
                "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.1)",
                "backgroundColor": "white",
            },
        ),
    ],
    style={
        "fontFamily": "Arial, sans-serif",
        "backgroundColor": "#f5f5f5",
        "padding": "20px",
        "borderRadius": "10px",
        "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.2)",
    },
)


@callback(
    Output("high-error-table", "data"),
    [Input("error-threshold-dropdown", "value")]
)
def update_high_error_table(error_threshold):
    query = """
    SELECT 
    cwh.*, 
    TO_TIMESTAMP(cwh.last_event_received_at/1000) AS last_event_timestamp,
    TO_TIMESTAMP(cwh.first_event_received_at/1000) AS first_event_timestamp,
    tenant.base_domain
FROM core_master.clickhouse_write_history cwh
LEFT JOIN core_master.tenant tenant
ON cwh.tenant_id = tenant.id
WHERE cwh.errors_count >= (cwh.records_count * %(error_threshold)s / 100.0)
ORDER BY cwh.finished_at DESC
LIMIT 20;
    """
    params = {"error_threshold": error_threshold}

    try:
        pg_result = execute_postgres_query(
            query=query,
            params=params,
            tenant_id="public",
            echo_query=False,
            echo_params=False,
        )

        if pg_result:
            columns = [
                "id",
                "tenant_id",
                "last_event_recieved_at",
                "finished_at",
                "records_count",
                "error_count",
                "duration",
                "db_persist_duration",
                "run_id",
                "first_event_recieved_at",
                "last_event_timestamp",
                "first_event_timestamp",
                "base_domain"
            ]
            df = pd.DataFrame(pg_result, columns=columns)
            

            return df.to_dict("records")

        return []

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return []


@callback(
    Output("tenant-details-output", "children"),
    [Input("high-error-table", "active_cell")],
    [State("high-error-table", "data")]
)
def display_tenant_details(active_cell, table_data):
    if active_cell:
        tenant_id = table_data[active_cell["row"]]["tenant_id"]
        query = """
        SELECT 
        cwh.*, 
        TO_TIMESTAMP(cwh.last_event_received_at/1000) AS last_event_timestamp,
        TO_TIMESTAMP(cwh.first_event_received_at/1000) AS first_event_timestamp,
        tenant.base_domain
        FROM core_master.clickhouse_write_history cwh
        LEFT JOIN core_master.tenant tenant
        ON cwh.tenant_id = tenant.id
        WHERE cwh.tenant_id = %(tenant_id)s
        LIMIT 20;
        """
        params = {"tenant_id": tenant_id}

        try:
            pg_result = execute_postgres_query(
                query=query,
                params=params,
                tenant_id="public",
                echo_query=False,
                echo_params=False,
            )
            columns = [
                "id",
                "tenant_id",
                "last_event_recieved_at",
                "finished_at",
                "records_count",
                "error_count",
                "duration",
                "db_persist_duration",
                "run_id",
                "first_event_recieved_at",
                "last_event_timestamp",
                "first_event_timestamp",
                "base_domain"
            ]

            if pg_result:

                df = pd.DataFrame(pg_result,columns=columns)
                df["finished_at"] = pd.to_datetime(df["finished_at"])
                df["last_event_timestamp"] = pd.to_datetime(df["last_event_timestamp"])
                df["first_event_timestamp"] = pd.to_datetime(df["first_event_timestamp"])

                def convert_to_ist(dt):
                    if dt.tzinfo is None:
                        return dt.tz_localize('UTC').tz_convert(ist)
                    else:
                        return dt.tz_convert(ist)

                ist = pytz.timezone('Asia/Kolkata')

                df["finished_at"] = df["finished_at"].apply(convert_to_ist)
                df["last_event_timestamp"] = df["last_event_timestamp"].apply(convert_to_ist)
                df["first_event_timestamp"] = df["first_event_timestamp"].apply(convert_to_ist)

                df["finished_at"] = df["finished_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
                df["last_event_timestamp"] = df["last_event_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
                df["first_event_timestamp"] = df["first_event_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
                df.columns = df.columns.astype(str)

                return dash_table.DataTable(
                    data=df.to_dict("records"),
                    columns=[{"name": col, "id": col} for col in df.columns],
                    style_table={"overflowX": "auto", "margin": "15px"},
                    page_size=5,
                )

        except Exception as e:
            return html.Div(f"Error fetching details for tenant ID {tenant_id}: {str(e)}")

    return html.Div("Click on a Tenant ID to view details.")

