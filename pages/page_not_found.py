import dash
from dash import html

dash.register_page(
    __name__,
    path='/404',
    name='Page not found',
    is_fallback=True
)

layout = html.Div([
    html.H1("404 - Page Not Found"),
    html.P("The page you are looking for does not exist."),
    html.A("Go back to Home", href="/", style={"textDecoration": "underline"})
])
