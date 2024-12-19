import pandas as pd
from dash import dcc, html, Input, Output, dash_table, callback, State
import dash
from exec import execute_postgres_query
import pytz

dash.register_page(__name__, path='/custom-error')

layout = html.Div(
    [
        html.Div(
            [
                html.H1(
                    "Error Filtered Tenants Dashboard",
                    style={
                        "textAlign": "center",
                        "color": "#ea0d13",
                        "fontSize": "32px",
                        "fontWeight": "bold",
                        "marginBottom": "20px",
                        "textShadow": "2px 2px 4px rgba(0,0,0,0.1)"
                    },
                ),
            ],
            style={
                "backgroundColor": "white",
                "padding": "20px",
                "borderRadius": "10px",
                "marginBottom": "20px",
                "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.1)"
            }
        ),

        html.Div(
            [
                html.Div(
                    [
                        html.Label(
                            "Error Rate Threshold:", 
                            style={"marginRight": "10px", "fontWeight": "bold"}
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
                                "width": "200px",
                                "display": "inline-block",
                                "verticalAlign": "middle"
                            },
                        ),
                    ],
                    style={
                        "display": "flex", 
                        "justifyContent": "center", 
                        "alignItems": "center",
                        "marginBottom": "20px"
                    }
                )
            ],
            style={
                "backgroundColor": "white",
                "padding": "15px",
                "borderRadius": "10px",
                "marginBottom": "20px",
                "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.1)"
            }
        ),

        html.Div(
            [
                dash_table.DataTable(
                    id="high-error-table",
                    columns=[
                        {"name": "Tenant ID", "id": "tenant_id"},
                        {"name": "Tenant Name", "id": "tenant_name"},
                        {"name": "Base Domain", "id": "base_domain"},
                        {"name": "Contact Email", "id": "contact_email"},  
                        {"name": "Error Count", "id": "error_count"},
                        {"name": "Total Events", "id": "records_count"},
                    ],
                    page_size=20,
                    style_table={
                        "overflowX": "auto",
                        "border": "1px solid #ddd",
                        "borderRadius": "5px",
                    },
                    style_header={
                        "backgroundColor": "#ea0d13",
                        "color": "white",
                        "fontWeight": "bold",
                        "textAlign": "center",
                        "padding": "12px",
                        "textTransform": "uppercase"
                    },
                    style_data={
                        "backgroundColor": "#f9f9f9",
                        "color": "#333",
                        "border": "1px solid #ddd",
                        "padding": "10px"
                    },
                    style_data_conditional=[
                        {
                            "if": {"row_index": "even"},
                            "backgroundColor": "#f1f1f1",
                        },
                        {
                            "if": {"state": "active"},
                            "backgroundColor": "rgba(234, 13, 19, 0.1)",
                            "border": "2px solid #ea0d13",
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
                        "maxWidth": "200px",
                        "overflow": "hidden",
                        "textOverflow": "ellipsis"
                    },
                    style_as_list_view=True,
                )
            ],
            style={
                "backgroundColor": "white",
                "padding": "15px",
                "borderRadius": "10px",
                "marginBottom": "20px",
                "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.1)"
            }
        ),
        html.Div(
            [
                html.Div(id="page-info", style={"textAlign": "center", "marginBottom": "10px"}),
                html.Div(
                    id="tenant-details-output",
                    style={
                        "border": "1px solid #ddd",
                        "borderRadius": "5px",
                        "padding": "15px",
                        "backgroundColor": "white",
                    }
                ),
                html.Div(
                    [
                        html.Button(
                            "Previous", 
                            id="prev-page-btn", 
                            style={
                                "margin": "10px",
                                "padding": "10px 20px",
                                "backgroundColor": "#f1f1f1",
                                "color": "#333",
                                "border": "1px solid #ddd",
                                "borderRadius": "5px",
                                "cursor": "pointer"
                            }
                        ),
                        html.Button(
                            "Next", 
                            id="next-page-btn", 
                            style={
                                "margin": "10px",
                                "padding": "10px 20px",
                                "backgroundColor": "#ea0d13",
                                "color": "white",
                                "border": "none",
                                "borderRadius": "5px",
                                "cursor": "pointer"
                            }
                        )
                    ],
                    style={
                        "display": "flex", 
                        "justifyContent": "center", 
                        "alignItems": "center"
                    }
                )
            ],
            style={
                "backgroundColor": "white",
                "padding": "15px",
                "borderRadius": "10px",
                "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.1)"
            }
        ),

        dcc.Store(id='tenant-details-tenant-id'),
        dcc.Store(id='tenant-details-current-page')
    ],
    style={
        "fontFamily": "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
        "backgroundColor": "#f5f5f5",
        "padding": "20px",
        "maxWidth": "1200px",
        "margin": "0 auto",
        "borderRadius": "10px",
        "boxShadow": "0 8px 16px rgba(0, 0, 0, 0.2)"
    }
)


@callback(
    Output("high-error-table", "data"),
    [Input("error-threshold-dropdown", "value"),
     Input("high-error-table", "page_current")],
    prevent_initial_call=True
)
def update_high_error_table(error_threshold, page_current):
    if page_current is None:
        page_current = 0

    offset = page_current * 20
    query = """
    SELECT 
        cwh.*, 
        TO_TIMESTAMP(cwh.last_event_received_at/1000) AS last_event_timestamp,
        TO_TIMESTAMP(cwh.first_event_received_at/1000) AS first_event_timestamp,
        tenant.base_domain,
        tenant.name,
        tenant.contact_email  
    FROM core_master.clickhouse_write_history cwh
    LEFT JOIN core_master.tenant tenant
    ON cwh.tenant_id = tenant.id
    WHERE cwh.errors_count >= (cwh.records_count * %(error_threshold)s / 100.0)
    ORDER BY cwh.finished_at DESC
    LIMIT 20 OFFSET %(offset)s;
"""
    params = {"error_threshold": error_threshold, "offset": offset}

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
                "base_domain",
                "tenant_name",
                "contact_email"  
            ]
            df = pd.DataFrame(pg_result, columns=columns)
            return df.to_dict("records")

        return []

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return []
@callback(
    Output("tenant-details-output", "children"),
    [Input("high-error-table", "active_cell"), Input("high-error-table", "page_current")],
    [State("high-error-table", "data")],
)
def display_tenant_details(active_cell, page_current=0, table_data=None):
    if active_cell:
        tenant_id = table_data[active_cell["row"]]["tenant_id"]
        if page_current is None:
            page_current = 0
        offset = page_current * 20
        query = """
        SELECT 
            cwh.*, 
            tenant.base_domain, 
            tenant.name AS tenant_name,
            TO_TIMESTAMP(cwh.last_event_received_at/1000) AS last_event_timestamp,
            TO_TIMESTAMP(cwh.first_event_received_at/1000) AS first_event_timestamp
        FROM core_master.clickhouse_write_history cwh
        LEFT JOIN core_master.tenant tenant
        ON cwh.tenant_id = tenant.id
        WHERE cwh.tenant_id = %(tenant_id)s
        ORDER BY cwh.finished_at DESC
        LIMIT 20 OFFSET %(offset)s;
        """
        params = {"tenant_id": tenant_id, "offset": offset}

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
                "base_domain",
                "tenant_name",
                "last_event_timestamp",
                "first_event_timestamp",
            ]

            if pg_result:
                df = pd.DataFrame(pg_result, columns=columns)

                ist = pytz.timezone('Asia/Kolkata')

                def convert_to_ist(dt):
                    if dt.tzinfo is None:
                        return dt.tz_localize('UTC').tz_convert(ist)
                    else:
                        return dt.tz_convert(ist)

                df["finished_at"] = pd.to_datetime(df["finished_at"]).apply(convert_to_ist)
                df["last_event_timestamp"] = pd.to_datetime(df["last_event_timestamp"]).apply(convert_to_ist)
                df["first_event_timestamp"] = pd.to_datetime(df["first_event_timestamp"]).apply(convert_to_ist)

                df["finished_at"] = df["finished_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
                df["last_event_timestamp"] = df["last_event_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
                df["first_event_timestamp"] = df["first_event_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

                return dash_table.DataTable(
                    data=df.to_dict("records"),
                    columns=[{"name": col, "id": col} for col in df.columns],
                    style_table={"overflowX": "auto", "margin": "15px"},
                    page_size=20,
                )

        except Exception as e:
            return html.Div(f"Error fetching details for tenant ID {tenant_id}: {str(e)}")

    return html.Div("Click on a Tenant ID to view details.")
