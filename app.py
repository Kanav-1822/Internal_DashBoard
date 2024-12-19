import dash
from dash import Dash, html, dcc, Input, Output, State
import json


with open('users.json', 'r') as f:
    users = json.load(f)


app = Dash(__name__, use_pages=True, suppress_callback_exceptions=True)

login_layout = html.Div(
    style={
        "display": "flex",
        "flexDirection": "column",
        "alignItems": "center",
        "justifyContent": "center",
        "height": "100vh",
        "backgroundColor": "#f4f4f4",
        "fontFamily": "Arial, sans-serif"
    },
    children=[
        html.Div(
            style={
                "backgroundColor": "#ffffff",
                "padding": "30px",
                "borderRadius": "10px",
                "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.1)",
                "textAlign": "center",
                "maxWidth": "400px",
                "width": "100%"
            },
            children=[
                html.Img(
                    src="/assets/logo.jpeg",  
                    style={
                        "width": "100px",
                        "marginBottom": "20px"
                    }
                ),
                html.H1(
                    "Login to We360.ai Internal Portal",
                    style={
                        "color": "#333",
                        "fontSize": "24px",
                        "marginBottom": "20px"
                    }
                ),
                dcc.Input(
                    id='username',
                    type='text',
                    placeholder='Enter username',
                    style={
                        "width": "100%",
                        "padding": "10px",
                        "marginBottom": "10px",
                        "border": "1px solid #ccc",
                        "borderRadius": "5px"
                    }
                ),
                dcc.Input(
                    id='password',
                    type='password',
                    placeholder='Enter password',
                    style={
                        "width": "100%",
                        "padding": "10px",
                        "marginBottom": "20px",
                        "border": "1px solid #ccc",
                        "borderRadius": "5px"
                    }
                ),
                html.Button(
                    'Login',
                    id='login-button',
                    n_clicks=0,
                    style={
                        "backgroundColor": "#4CAF50",
                        "color": "white",
                        "padding": "10px 20px",
                        "border": "none",
                        "borderRadius": "5px",
                        "cursor": "pointer",
                        "fontSize": "16px"
                    }
                ),
                html.Div(
                    id='login-status',
                    style={
                        "color": "red",
                        "marginTop": "10px",
                        "fontSize": "14px"
                    }
                )
            ]
        )
    ]
)

dashboard_layout = html.Div(
    [
        html.H1("Internal Dashboard for We360.ai"),
        html.Div(
            [
                html.Div(
                    dcc.Link(
                        f"{page['name']} - {page['path']}", href=page["relative_path"]
                    )
                )
                for page in dash.page_registry.values()
                if not page.get("is_fallback", False)
            ]
        ),
        dash.page_container,
    ]
)

app.layout = html.Div(id='main-layout', children=[login_layout])


@app.callback(
    Output('main-layout', 'children'),
    [Input('login-button', 'n_clicks')],
    [State('username', 'value'), State('password', 'value')]
)
def check_credentials(n_clicks, username, password):
    if n_clicks > 0:
        if username in users and users[username] == password:
            return dashboard_layout
        else:
            return html.Div([
                login_layout,
                html.Div("Invalid username or password. Please try again.", style={'color': 'red'})
            ])
    return login_layout

if __name__ == "__main__":
    app.run(debug=True)
