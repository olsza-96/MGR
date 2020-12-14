import dash
import dash_core_components as dcc
import dash_html_components as html

import base64
import plotly.express as px

import dash_data_processing as extra


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

voivodeships = extra.read_voivodeship_data("/Users/Olga/PycharmProjects/MGR_Project/MGR/wojewodztwa.txt")



def generate_table(dataframe, max_rows=10):
    return html.Table([
        html.Thead(
            html.Tr([html.Th('Feature') for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(col) for col in dataframe.columns
            ]) for i in range(1)])
    ])

def histogram(dff, value):
    return {
            'data': [
                {
                    'x': dff,
                    'name': value,
                    'type': 'histogram'
                },
            ],
            'layout': {
                'title': 'Histogram for chosen feature',
                'xaxis':{
                    'title':'Value for chosen feature'
                },
                'yaxis':{
                     'title':'Frequency'
            }
        }}


app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = html.Div(
    children=[
    html.H1(children="Master thesis results visualization"),

    html.H2(children="by Olga Olkowicz"),

    html.H3(children="Visualization of available wind plant location per voivodeship"),

    html.Div(children='''
       Choose voivodeship for which to present the resultant data 
    '''),
        html.Div([
        dcc.Dropdown(
            id='menu',
            options=[{'label': i, 'value': i} for i in voivodeships.keys()],
            value= 'województwo kujawsko-pomorskie'
        ),
    ]),
    html.Div([
        dcc.Graph(id='map_plot')
    ], style={'width': '100%', 'display': 'inline-block', 'padding': '0 20'}),

])

@app.callback(
    dash.dependencies.Output('map_plot', 'figure'),
    [dash.dependencies.Input('menu', 'value')])
def update_graph(value):
    data_to_plot = extra.get_data_voivodeship(value, voivodeships)
    return plot_data_on_map(data_to_plot)

def plot_data_on_map(data):

    lon, lat = data['lon'].values, data['lat'].values
    fig = px.scatter_mapbox(lat = lat, lon = lon, color_discrete_sequence= ["cadetblue"],
                            zoom=5.2, height=600)
    fig.update_layout(mapbox_style = "carto-positron")
    fig.update_layout(margin={"r": 200, "t": 20, "l": 200, "b": 20})
    fig.update_layout(autosize = True,
                      hovermode='closest',
                    mapbox = dict (
                            bearing = 0,
                            center = dict( lat = 52.05,
                                           lon = 19.08),
                            pitch = 0))

    return fig
if __name__ == '__main__':

    app.run_server(debug=True, port= 8000)