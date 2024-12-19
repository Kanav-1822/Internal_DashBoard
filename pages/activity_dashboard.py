import dash
import pytz
from dash import dcc, html, Input, Output, dash_table, callback
import pandas as pd
from exec import execute_postgres_query

dash.register_page(__name__, path='/activity-dashboard')

layout = html.Div(
    [
        html.Div(
            [
                html.H1(
                    "Tenant Activity Dashboard",
                    style={
                        "textAlign": "center",
                        "color": "#ffffff",
                        "font-family": "Arial, sans-serif",
                        "font-size": "24px",
                    },
                ),
            ],
            style={
                "padding": "20px",
                "backgroundColor": "#0f082d",
                "borderBottom": "4px solid #ea0d13",
            },
        ),
        html.Div([
            dcc.Input(
                id="tenant-id-input",
                type="text",
                placeholder="Enter Tenant ID",
                style={
                    "margin-bottom": "10px",
                    "padding": "10px",
                    "font-size": "16px",
                    "borderRadius": "5px",
                    "border": "1px solid #ea0d13",
                    "margin-right": "10px",
                },
            ),
            html.Button(
                "Search",
                id="search-button",
                n_clicks=0,
                style={
                    "padding": "10px 20px",
                    "background-color": "#0056b3",
                    "color": "white",
                    "border": "none",
                    "borderRadius": "5px",
                    "font-size": "16px",
                    "margin-right": "10px",
                },
            ),
            html.Button(
                "ACTIVE",
                id="active-button",
                n_clicks=0,
                style={
                    "padding": "10px 20px",
                    "background-color": "#FFC0CB",
                    "color": "black",
                    "border": "none",
                    "borderRadius": "5px",
                    "font-size": "16px",
                },
            ),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "20px"}),
        dash_table.DataTable(
            id="data-table",
            columns=[
                {"name": "Tenant ID", "id": "tenant_id"},
                {"name": "Base Domain", "id": "base_domain"},
                {"name": "Name", "id": "name"}, 
                {"name": "Contact Email", "id": "contact_email"},  
                {"name": "Finished At (IST)", "id": "finished_at"},
                {"name": "Last Event Time (IST)", "id": "last_event_timestamp"},
            ],
            page_size=20,
            style_table={
                "overflowX": "auto",
                "border": "1px solid #007BFF",
                "font-size": "14px",
                "backgroundColor": "#0f082d",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                "borderRadius": "5px",
            },
            style_header={
                "backgroundColor": "#0f082d",
                "color": "white",
                "fontWeight": "bold",
                "border": "1px solid #0056b3",
            },
            style_data_conditional=[
                {
                    "if": {"row_index": "even"},
                    "backgroundColor": "#f1f1f1",
                    "border": "1px solid #007BFF",
                }
            ],
        ),
        html.Div(
            id="tenant-summary",
            style={
                "margin-top": "20px",
                "font-weight": "bold",
                "text-align": "center",
                "color": "#0056b3",
                "font-size": "18px",
            },
        ),
    ]
)

@callback(
    [Output("data-table", "data"), Output("tenant-summary", "children")],
    [Input("search-button", "n_clicks"), Input("active-button", "n_clicks")],
    [Input("tenant-id-input", "value")]
)
def update_table(search_clicks, active_clicks, tenant_id):
    ctx = dash.callback_context
    if not ctx.triggered:
        button_id = None
    else:
        button_id = ctx.triggered[ 0]['prop_id'].split('.')[0]

    if button_id == "active-button":
        query = """
        SELECT 
            cwh.*, 
            TO_TIMESTAMP(cwh.last_event_received_at / 1000) AS last_event_timestamp,
            TO_TIMESTAMP(cwh.first_event_received_at / 1000) AS first_event_timestamp,
            tenant.base_domain,
            tenant.name,
            tenant.contact_email 
        FROM core_master.clickhouse_write_history cwh
        LEFT JOIN core_master.tenant tenant
        ON cwh.tenant_id = tenant.id
        WHERE tenant.deleted_date IS NULL
        ORDER BY cwh.finished_at DESC
        LIMIT 20;
        """
        params = {}
    else:
        if tenant_id:
            query = """
            SELECT 
                cwh.*, 
                TO_TIMESTAMP(cwh.last_event_received_at / 1000) AS last_event_timestamp,
                TO_TIMESTAMP(cwh.first_event_received_at / 1000) AS first_event_timestamp,
                tenant.base_domain,
                tenant.name,
                tenant.contact_email 
            FROM core_master.clickhouse_write_history cwh
            LEFT JOIN core_master.tenant tenant
            ON cwh.tenant_id = tenant.id
            WHERE cwh.tenant_id = %(tenant_id)s
            ORDER BY cwh.finished_at DESC
            LIMIT 20;
            """
            params = {"tenant_id": tenant_id}
        else:
            query = """
            SELECT 
                cwh.*, 
                TO_TIMESTAMP(cwh.last_event_received_at / 1000) AS last_event_timestamp,
                TO_TIMESTAMP(cwh.first_event_received_at / 1000) AS first_event_timestamp,
                tenant.base_domain,
                tenant.name,
                tenant.contact_email  
            FROM core_master.clickhouse_write_history cwh
            LEFT JOIN core_master.tenant tenant
            ON cwh.tenant_id = tenant.id
            ORDER BY cwh.finished_at DESC 
            LIMIT 20;
            """
            params = {}

    try:
        pg_result = execute_postgres_query(
            query=query,
            params=params,
            tenant_id=tenant_id if tenant_id else "public",
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
                "base_domain",
                "name",  
                "contact_email"  
            ]
            df = pd.DataFrame(pg_result, columns=columns)
            
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

            row_count = len(df)
            error_count = df["error_count"].sum()
            summary = f"Row Count: {row_count}, Error Count: {error_count}"

            return df.to_dict("records"), summary

        return [], "No data found for the specified criteria."

    except Exception as e:
        return [], f"An error occurred: {str(e)}"