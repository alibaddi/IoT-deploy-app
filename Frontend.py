################ Imports ######################
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State
from dash_iconify import DashIconify
from datetime import datetime
import psycopg2
import pandas as pd
import plotly.graph_objs as go
from dash_bootstrap_templates import load_figure_template

################ initialization ######################
external_stylesheets = [dbc.themes.CERULEAN, dbc.icons.BOOTSTRAP]
app = dash.Dash(__name__, suppress_callback_exceptions=True,
                external_stylesheets=external_stylesheets)
load_figure_template('LUX')
server = app.server

options_list = []
ac_graphs = []
all_data = []
sorted_df_battery = pd.DataFrame()
sorted_df_network = pd.DataFrame()


def get_add_device_modal():
    return html.Div(
        [
            dbc.Modal([
                dbc.ModalHeader(dbc.ModalTitle("Devices", style={'color': 'black'})),
                dbc.ModalBody(
                    dbc.Form(
                        [
                            dbc.Label("Device name", className="mr-2"),
                            dbc.Input(id='device-name',
                                      type="text",
                                      placeholder="Enter device name"),
                            html.Br(),
                            dbc.Label("Topic", className="mr-2"),
                            dbc.Input(id='device-topic',
                                      type="text",
                                      placeholder="Enter device topic"),
                        ]
                    )
                ),
                dbc.ModalFooter(
                    [
                        dbc.Button("Close", id="close-btn_device", color="secondary"),
                        dbc.Button("Save changes", id="save-device-btn", color="warning"),
                    ]
                ),
            ],
                id="add-device-modal",
                is_open=False,
                backdrop=True,
                scrollable=True,
                centered=True,
                fade=True
            ),
        ]
    )


def get_alert_modal():
    return html.Div(
        [
            dbc.Modal(
                [
                    dbc.ModalFooter(
                        [
                            dbc.Container(id='all-alerts-container'),
                            dbc.Button("Close", id='close-alert-modal', outline=True, color="danger")
                        ]

                    )
                ],
                id="all-alerts-modal",
                is_open=False,
                backdrop=True,
                size="xl",

            )
        ],

    )


################ App layout ######################


def layout():
    add_device_modal = get_add_device_modal()
    alert_modal = get_alert_modal()
    choose_icon = DashIconify(icon="carbon:choose-item", width=24, height=24)
    download_icon = DashIconify(icon="material-symbols:cloud-download-outline", width=24, height=24)
    submit_icon = DashIconify(icon="iconoir:submit-document", width=24, height=24)
    trash_icon = DashIconify(icon="bi:trash-fill", width=24, height=24)
    display_icon = DashIconify(icon="bi:display", width=24, height=24)
    battery_unkown = DashIconify(icon="ic:outline-battery-unknown", width=52, height=52)
    network_unkown = DashIconify(icon="mdi:network-strength-off-outline", width=52, height=52)

    indicator_options = {
        'LXeq': ['LAeq', 'LCeq', 'LZeq'],
        'LXY': ['LAS', 'LCS', 'LZS', 'Lpeak'],
        'LXeqT': ['LAeqT', 'LCeqT', 'LZeqT', 'LAeqTmax', 'LCeqTmax', 'LZeqTmax', 'LAeqTmin', 'LCeqTmin', 'LZeqTmin'],
        'LN': ['Ln1', 'Ln2', 'Ln3', 'Ln4', 'Ln5', 'Ln6', 'Ln7'],
        'SPECTRUMT_octave': ['octave_16', 'octave_31', 'octave_63', 'octave_125', 'octave_250', 'octave_500',
                             'octave_1000',
                             'octave_2000', 'octave_4000', 'octave_8000', 'octave_16000'],
        'SPECTRUMT_third_octave': ['third_octave_10', 'third_octave_12', 'third_octave_16', 'third_octave_20',
                                   'third_octave_25',
                                   'third_octave_31', 'third_octave_40', 'third_octave_50', 'third_octave_63',
                                   'third_octave_80',
                                   'third_octave_100', 'third_octave_125', 'third_octave_160', 'third_octave_200',
                                   'third_octave_250', 'third_octave_315', 'third_octave_400', 'third_octave_500',
                                   'third_octave_630', 'third_octave_800', 'third_octave_1000', 'third_octave_1250',
                                   'third_octave_1600', 'third_octave_2000', 'third_octave_2500', 'third_octave_3150',
                                   'third_octave_4000', 'third_octave_5000', 'third_octave_6300', 'third_octave_8000',
                                   'third_octave_10000', 'third_octave_12500', 'third_octave_16000',
                                   'third_octave_20000'],
        'LXET': ['LAET', 'LCET', 'LZET'],
        'TEMPORAL': ['LXeqTs', 'LXeqhh1', 'LXeqhh2', 'LXeqhh3']
    }

    options = [
        {'label': 'Subplot 1', 'value': 'subplot_1'},
        {'label': 'Subplot 2', 'value': 'subplot_2'},
        {'label': 'Subplot 3', 'value': 'subplot_3'}
    ]

    tab1_content = dbc.CardBody(
        [
            html.Div(id='acoustic-graph', children=[],
                     style={'display': 'inline-block', 'width': '160vh', 'height': '40vh'}),

        ],
        className="mt-3",
    )
    tab2_content = dbc.CardBody(
        [
            html.Div(id='battery-graph', children=[],
                     style={'display': 'inline-block', 'width': '160vh', 'height': '40vh'}),
        ],
        className="mt-3",
    )
    tab3_content = dbc.CardBody(
        [
            html.Div(id='network-graph', children=[],
                     style={'display': 'inline-block', 'width': '160vh', 'height': '40vh'}),
        ],
        className="mt-3",
    )

    layout = html.Div([
        dbc.NavbarSimple(
            children=[
                html.H1(
                    "IoT Platform",
                    className="navbar-title",
                    style={'display': 'flex', 'align-items': 'flex-start', "margin-right": "700px"}
                ),
                dbc.DropdownMenu(
                    children=[
                        dbc.DropdownMenuItem("Personal information", href="#"),
                        dbc.DropdownMenuItem("Settings", href="#"),
                        dbc.DropdownMenuItem("Sign out", href="#"),
                    ],
                    nav=True,
                    in_navbar=True,
                    label="User",
                    style={'position': 'absolute', 'right': '100px', 'font-size': '25px', 'line-height': '30px'}
                ),
            ],
            brand_href="#",
            color="warning",
            dark=True,
        ),
        html.Br(),

        # dbc.Offcanvas(
        #
        #     id="all-alerts-modal",
        #     is_open=False,
        #     placement="top",
        #     backdrop=False,
        #     style={"height": "180px"},
        # ),

        add_device_modal,
        alert_modal,
        dbc.Offcanvas(
            children=[
                html.P(
                    [
                        html.H2("Connected devices", style={'color': 'black'}),
                        dcc.Interval(id='interval-component', interval=100),
                        html.Br(),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.RadioItems(
                                            id="radio-items",
                                            options=[],
                                            value=" No device has been selected yet",
                                            labelCheckedClassName="text-success",
                                            inputCheckedClassName="border border-success bg-success",
                                            style={'width': '100px', 'height': '40px', 'font-size': '25px'}),
                                    ]
                                )

                            ]
                        ),
                        html.Br(),
                        html.Br(),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [
                                        dbc.Button([submit_icon, " Submit"], id='submit-device-button',
                                                   color="success",
                                                   style={'padding': '8px', 'width': '120px'}, size="lg"),
                                    ]
                                )

                            ]
                        )
                    ],

                ),

            ],
            id="offcanvas",
            is_open=False,
        ),

        html.Div(

            [
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            [
                                                dbc.Button([choose_icon, " Choose device"], color="secondary",
                                                           id="choose-device-button"
                                                           , size="lg", n_clicks=False),
                                            ]
                                        )

                                    ]
                                ),
                                html.Br(),
                                html.Br(),
                                html.Br(),
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            [
                                                html.H1(dbc.Badge(
                                                    " No device has been selected yet",
                                                    color="success",
                                                    className="bi bi-hdd",
                                                    id="badge-device"
                                                )),
                                            ], style={"margin-left": "0px", 'textAlign': 'center'}
                                        )

                                    ]
                                )

                            ], style={"margin-left": "20px"}
                        ),

                        dbc.Col([
                            dbc.Row([

                                html.I(battery_unkown, style={'font-size': '50px'}, id='battery-icon'),

                                html.H4(dbc.Badge("--", color="light", text_color="primary",
                                                  className="ms-1", id="badge-battery")),

                                dcc.Interval(id='interval-badge', interval=400),

                                html.I(network_unkown, style={'font-size': '50px'}, id='network-icon'),

                                html.H4(dbc.Badge("--", color="light", text_color="primary",
                                                  className="ms-1", id="badge-network"))
                            ])
                        ], width=1, style={'textAlign': 'center'}),

                        dbc.Col(
                            dbc.Row([
                                dbc.Label("From :", className="mr-2"),
                                dbc.Input(id='choosing-start-date',
                                          type="datetime-local",
                                          placeholder="Today",
                                          style={"width": "250px", "height": "60px"}),
                                html.Div(style={'margin': '10px 0'}),
                                dbc.Label("To :", className="mr-2"),
                                dbc.Input(id='choosing-end-date',
                                          type="datetime-local",
                                          placeholder="Today",
                                          style={"width": "250px", "height": "60px"}),
                                html.Div(style={'margin': '10px 0'}),
                                dbc.Button([submit_icon, " Submit"], id='submit-button',
                                           color="success",
                                           style={'padding': '8px', 'width': '100px',
                                                  'margin': 'auto'}),
                                html.Div(style={'margin': '10px 0'}),

                                html.Div(id='output-container')
                            ]),

                            width=1,
                            style={
                                "margin-right": "100px"}),

                    ], justify="end"
                )

            ], style={"margin-top": "30px"}),

        dbc.Row([
            dbc.Col(
                [
                    dbc.Tabs(
                        [
                            dbc.Tab(tab1_content, label="Acoustic level", tab_id="tab-1"),
                            dbc.Tab(tab2_content, label="Battery level", tab_id="tab-2"),
                            dbc.Tab(tab3_content, label="Network connection status", tab_id="tab-3"),
                        ],
                        id="tabs",
                        active_tab="tab-1",
                    ),
                    html.Div(id="content"),
                    dcc.Download(id="download-dataframe-csv"),

                ]
            ),
            dbc.Col(
                [
                    dbc.Card(
                        [
                            dbc.CardBody(
                                [
                                    html.H4("Indicators", className="card-title"),
                                    dbc.Label("Family indicator : "),
                                    dcc.Dropdown(id='indicator-family-dropdown', options=[
                                        {'label': key, 'value': key} for key in indicator_options.keys()
                                    ], placeholder='Choose your family indicator...'),
                                    html.Div(style={'margin': '10px 0'}),
                                    dbc.Label("Indicator : ", style={'display': 'none'}, id='indicator-label'),
                                    dcc.Dropdown(id='indicator-dropdown', options=[], style={'display': 'none'},
                                                 placeholder='Choose the indicator to display...'),
                                ]
                            ),
                            dbc.CardFooter(
                                [
                                    dbc.Container(id='alert-container', children=[]),
                                    dbc.Button([display_icon, "  Display result"], id="display-btn", color="warning"),

                                ]
                            ),
                        ], style={"width": "18rem", 'display': 'none'},
                        id='output-card'
                    ),
                    html.Br(),
                    dbc.Button([trash_icon, " Clear all the graphs"], color="danger",
                               id="clear-btn",
                               n_clicks=False, style={'display': 'none'}),
                    html.Br(),
                    dbc.Button([download_icon, " Download results"], color="primary",
                               id="download-results-button",
                               n_clicks=False, style={'display': 'none'}),
                ], style={"margin-right": "30px", "margin-left": "50px", "margin-top": "40px"}
            )

        ]
        ),

    ])
    return layout


app.layout = layout()


################ Callbacks ######################
@app.callback(
    Output("offcanvas", "is_open"),
    [Input("choose-device-button", "n_clicks"),
     Input('submit-device-button', 'n_clicks')],
    [State("offcanvas", "is_open")],
)
def toggle_offcanvas(n_device, n_submit_device, is_open):
    if n_device or n_submit_device:
        return not is_open
    return is_open


@app.callback(
    [Output('indicator-dropdown', 'style'),
     Output('indicator-label', 'style')],
    [Input('indicator-family-dropdown', 'value')])
def update_indicator_dropdown_visibility(value):
    if value:
        return {'display': 'block'}, {'display': 'block'}
    else:
        return {'display': 'none'}, {'display': 'none'}


@app.callback(
    Output('indicator-dropdown', 'options'),
    [Input('indicator-family-dropdown', 'value')])
def update_indicator_options(indicator_family):
    if indicator_family == 'LXeq':
        return [
            {'label': 'LAeq', 'value': 'LAeq'},
            {'label': 'LCeq', 'value': 'LCeq'},
            {'label': 'LZeq', 'value': 'LZeq'},
        ]
    elif indicator_family == 'LXY':
        return [
            {'label': 'LAS', 'value': 'LAS'},
            {'label': 'LCS', 'value': 'LCS'},
            {'label': 'LZS', 'value': 'LZS'},
            {'label': 'Lpeak', 'value': 'Lpeak'}
        ]
    elif indicator_family == 'LXeqT':
        return [
            {'label': 'LAeqT', 'value': 'LAeqT'},
            {'label': 'LCeqT', 'value': 'LCeqT'},
            {'label': 'LZeqT', 'value': 'LZeqT'},
            {'label': 'LAeqTmax', 'value': 'LAeqTmax'},
            {'label': 'LCeqTmax', 'value': 'LCeqTmax'},
            {'label': 'LZeqTmax', 'value': 'LZeqTmax'},
            {'label': 'LAeqTmin', 'value': 'LAeqTmin'},
            {'label': 'LCeqTmin', 'value': 'LCeqTmin'},
            {'label': 'LZeqTmin', 'value': 'LZeqTmin'}
        ]
    elif indicator_family == 'LN':
        return [
            {'label': 'Ln1', 'value': 'Ln1'},
            {'label': 'Ln2', 'value': 'Ln2'},
            {'label': 'Ln3', 'value': 'Ln3'},
            {'label': 'Ln4', 'value': 'Ln4'},
            {'label': 'Ln5', 'value': 'Ln5'},
            {'label': 'Ln6', 'value': 'Ln6'},
            {'label': 'Ln7', 'value': 'Ln7'}
        ]
    elif indicator_family == 'SPECTRUMT_octave':
        return [
            {'label': 'octave_16', 'value': 'octave_16'},
            {'label': 'octave_31', 'value': 'octave_31'},
            {'label': 'octave_63', 'value': 'octave_63'},
            {'label': 'octave_125', 'value': 'octave_125'},
            {'label': 'octave_250', 'value': 'octave_250'},
            {'label': 'octave_500', 'value': 'octave_500'},
            {'label': 'octave_1000', 'value': 'octave_1000'},
            {'label': 'octave_2000', 'value': 'octave_2000'},
            {'label': 'octave_4000', 'value': 'octave_4000'},
            {'label': 'octave_8000', 'value': 'octave_8000'},
            {'label': 'octave_16000', 'value': 'octave_16000'}
        ]
    elif indicator_family == 'SPECTRUMT_third_octave':
        return [
            {'label': 'third_octave_10', 'value': 'third_octave_10'},
            {'label': 'third_octave_12', 'value': 'third_octave_12'},
            {'label': 'third_octave_16', 'value': 'third_octave_16'},
            {'label': 'third_octave_20', 'value': 'third_octave_20'},
            {'label': 'third_octave_25', 'value': 'third_octave_25'},
            {'label': 'third_octave_31', 'value': 'third_octave_31'},
            {'label': 'third_octave_40', 'value': 'third_octave_40'},
            {'label': 'third_octave_50', 'value': 'third_octave_50'},
            {'label': 'third_octave_63', 'value': 'third_octave_63'},
            {'label': 'third_octave_80', 'value': 'third_octave_80'},
            {'label': 'third_octave_100', 'value': 'third_octave_100'},
            {'label': 'third_octave_125', 'value': 'third_octave_125'},
            {'label': 'third_octave_160', 'value': 'third_octave_160'},
            {'label': 'third_octave_200', 'value': 'third_octave_200'},
            {'label': 'third_octave_250', 'value': 'third_octave_250'},
            {'label': 'third_octave_315', 'value': 'third_octave_315'},
            {'label': 'third_octave_400', 'value': 'third_octave_400'},
            {'label': 'third_octave_500', 'value': 'third_octave_500'},
            {'label': 'third_octave_630', 'value': 'third_octave_630'},
            {'label': 'third_octave_800', 'value': 'third_octave_800'},
            {'label': 'third_octave_1000', 'value': 'third_octave_1000'},
            {'label': 'third_octave_1250', 'value': 'third_octave_1250'},
            {'label': 'third_octave_1600', 'value': 'third_octave_1600'},
            {'label': 'third_octave_2000', 'value': 'third_octave_2000'},
            {'label': 'third_octave_2500', 'value': 'third_octave_2500'},
            {'label': 'third_octave_3150', 'value': 'third_octave_3150'},
            {'label': 'third_octave_4000', 'value': 'third_octave_4000'},
            {'label': 'third_octave_5000', 'value': 'third_octave_5000'},
            {'label': 'third_octave_6300', 'value': 'third_octave_6300'},
            {'label': 'third_octave_8000', 'value': 'third_octave_8000'},
            {'label': 'third_octave_10000', 'value': 'third_octave_10000'},
            {'label': 'third_octave_12500', 'value': 'third_octave_12500'},
            {'label': 'third_octave_16000', 'value': 'third_octave_16000'},
            {'label': 'third_octave_20000', 'value': 'third_octave_20000'},
        ]
    elif indicator_family == 'LXET':
        return [
            {'label': 'LAET', 'value': 'LAET'},
            {'label': 'LCET', 'value': 'LCET'},
            {'label': 'LZET', 'value': 'LZET'},
        ]
    elif indicator_family == 'TEMPORAL':
        return [
            {'label': 'LXeqTs', 'value': 'LXeqTs'},
            {'label': 'LXeqhh1', 'value': 'LXeqhh1'},
            {'label': 'LXeqhh2', 'value': 'LXeqhh2'},
            {'label': 'LXeqhh3', 'value': 'LXeqhh3'}
        ]
    else:
        return []


@app.callback(
    Output('radio-items', 'options'),
    [Input('interval-component', 'n_intervals')]
)
def update_radio_options(n_intervals):
    global options_list, conn, cur
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='IoT_Messages',
            user='postgres',
            password='delhom!12'
        )

        cur = conn.cursor()

        cur.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema='public'
                        AND table_type='BASE TABLE'
                        AND table_name LIKE '%_ac';
                    """)

        tables = cur.fetchall()

        for table in tables:
            value = " " + str(table[0]).upper()[:3]
            exists = False
            for option in options_list:
                if option['value'] == value:
                    exists = True
                    break
            if not exists:
                options_list.append({'label': str(table[0]).upper()[:-3], 'value': value})

    except psycopg2.Error as e:
        print(f'Error: {e}')

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()

    result = sorted(options_list, key=str)
    return result


@app.callback(
    [Output('badge-battery', "children"),
     Output('badge-battery', "color"),
     Output('badge-battery', "text_color"),
     Output('battery-icon', 'children')],
    Input('interval-badge', 'n_intervals'),
    State('radio-items', 'value')
)
def refresh_battery(n_intervals, radio_item):
    global result, conn, cur
    battery_unkown = DashIconify(icon="ic:outline-battery-unknown", width=52, height=52)
    battery_91_100 = DashIconify(icon="ic:outline-battery-full", width=52, height=52, style={"color": "green"})
    battery_81_90 = DashIconify(icon="ic:outline-battery-90", width=52, height=52, style={"color": "green"})
    battery_71_80 = DashIconify(icon="ic:outline-battery-80", width=52, height=52, style={"color": "green"})
    battery_60_70 = DashIconify(icon="ic:outline-battery-60", width=52, height=52, style={"color": "green"})
    battery_45_59 = DashIconify(icon="ic:outline-battery-50", width=52, height=52, style={"color": "green"})
    battery_30_44 = DashIconify(icon="ic:outline-battery-30", width=52, height=52, style={"color": "green"})
    battery_20_29 = DashIconify(icon="ic:outline-battery-20", width=52, height=52, style={"color": "green"})
    battery_0_19 = DashIconify(icon="ic:outline-battery-alert", width=52, height=52, style={"color": "red"})

    try:
        conn = psycopg2.connect(
            host='localhost',
            database='IoT_Messages',
            user='postgres',
            password='delhom!12'
        )

        # Create a cursor object
        cur = conn.cursor()
        # Execute a query
        if radio_item != ' No devise has been selected yet':
            cur.execute(
                "SELECT battery FROM {}_battery ORDER BY timestamp DESC LIMIT 1".format(radio_item))
            result = cur.fetchall()
            battery_value = int(result[0][0])
            # battery_value = float(result[0][0])

            if battery_value < 20:
                battery_color = "light"
                text_color = "danger"
            else:
                battery_color = "light"
                text_color = "success"

            if (battery_value >= 91) and (battery_value <= 100):
                return result[0][0] + '%', battery_color, text_color, battery_91_100

            elif (battery_value >= 81) and (battery_value <= 90):
                return result[0][0] + '%', battery_color, text_color, battery_81_90

            elif (battery_value >= 71) and (battery_value <= 80):
                return result[0][0] + '%', battery_color, text_color, battery_71_80

            elif (battery_value >= 60) and (battery_value <= 70):
                return result[0][0] + '%', battery_color, text_color, battery_60_70

            elif (battery_value >= 45) and (battery_value <= 59):
                return result[0][0] + '%', battery_color, text_color, battery_45_59

            elif (battery_value >= 30) and (battery_value <= 44):
                return result[0][0] + '%', battery_color, text_color, battery_30_44

            elif (battery_value >= 20) and (battery_value <= 29):
                return result[0][0] + '%', battery_color, text_color, battery_20_29

            elif (battery_value >= 0) and (battery_value < 20):
                return result[0][0] + '%', battery_color, text_color, battery_0_19

            else:
                return result[0][0] + '%', battery_color, text_color, battery_unkown

        else:
            return '--', dash.no_update, dash.no_update, battery_unkown

    except psycopg2.Error as e:
        # print(f'Error: {e}')
        pass
    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
    return dash.no_update, dash.no_update, dash.no_update, dash.no_update


@app.callback(
    [
     Output('badge-network', "children"),
     Output('badge-network', "text_color"),
     Output('network-icon', 'children')],
    Input('interval-badge', 'n_intervals'),
    State('radio-items', 'value')
)
def refresh_network(n_intervals, radio_item):
    global conn, cur, network_label, network_text_color

    network_unkown = DashIconify(icon="mdi:network-strength-off-outline", width=52, height=52)
    network_offline = DashIconify(icon="mdi:network-strength-off-outline", width=52, height=52, style={"color": "red"})
    network_unstable = DashIconify(icon="mdi:network-strength-outline", width=52, height=52, style={"color": "red"})
    network_poor = DashIconify(icon="mdi:network-strength-1", width=52, height=52, style={"color": "red"})
    network_fair = DashIconify(icon="mdi:network-strength-2", width=52, height=52, style={"color": "orange"})
    network_good = DashIconify(icon="mdi:network-strength-3", width=52, height=52, style={"color": "green"})
    network_excellent = DashIconify(icon="mdi:network-strength-4", width=52, height=52, style={"color": "green"})

    try:
        conn = psycopg2.connect(
            host='localhost',
            database='IoT_Messages',
            user='postgres',
            password='delhom!12'
        )

        # Create a cursor object
        cur = conn.cursor()
        # Execute a query
        if radio_item != ' No devise has been selected yet':
            cur.execute(
                "SELECT network FROM {}_network ORDER BY timestamp DESC LIMIT 1".format(radio_item))
            result_network = cur.fetchall()
            if result_network[0][0] == '0':
                network_label = 'Offline'
                network_text_color = "danger"
                return network_label, network_text_color, network_offline
            elif result_network[0][0] == '1':
                network_label = 'Unstable'
                network_text_color = "danger"
                return network_label, network_text_color, network_unstable
            elif result_network[0][0] == '2':
                network_label = 'Poor'
                network_text_color = "danger"
                return network_label, network_text_color, network_poor
            elif result_network[0][0] == '3':
                network_label = 'Fair'
                network_text_color = "warning"
                return network_label, network_text_color, network_fair
            elif result_network[0][0] == '4':
                network_label = 'Good'
                network_text_color = "success"
                return network_label, network_text_color, network_good
            elif result_network[0][0] == '5':
                network_label = 'Excellent'
                network_text_color = "success"
                return network_label, network_text_color, network_excellent

        else:
            return '--', dash.no_update, network_unkown

    except psycopg2.Error as e:
        pass
        # print(f'Error: {e}')
    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()
    return dash.no_update, dash.no_update, dash.no_update

@app.callback(
    [Output('output-card', 'style'),
     Output('download-results-button', 'style'),
     Output('clear-btn', 'style'),
     Output("submit-button", "n_clicks"),
     Output('display-btn', 'n_clicks'),
     Output('acoustic-graph', 'children'),
     Output('badge-device', "children"),
     Output('battery-graph', 'children'),
     Output('network-graph', 'children'),
     Output("download-dataframe-csv", "data"),
     Output('all-alerts-container', 'children'),
     Output('download-results-button', 'n_clicks'),
     Output('indicator-family-dropdown', 'value'),
     Output('clear-btn', 'n_clicks'),
     Output("all-alerts-modal", "is_open"),
     Output('close-alert-modal', 'n_clicks')],
    [Input('submit-button', 'n_clicks'),
     Input('display-btn', 'n_clicks'),
     Input('radio-items', 'value'),
     Input('download-results-button', 'n_clicks'),
     Input('clear-btn', 'n_clicks'),
     Input('close-alert-modal', 'n_clicks')],
    [State('choosing-start-date', 'value'),
     State('choosing-end-date', 'value'),
     State('indicator-family-dropdown', 'value'),
     State('indicator-dropdown', 'value'),
     State("all-alerts-modal", "is_open")]

)
def display_output(n_submit, n_display, radio_item, n_download, n_clear, n_close, start_time, end_time,
                   family_indicator,
                   indicator, is_open):
    global cur, conn, fig_battery, fig, fig_network, sorted_df, merged_df2, ac_graphs, all_data, sorted_df_battery, sorted_df_network
    indicator_value = []
    timestamp_value = []
    battery_level = []
    timestamp_battery_value = []
    network_level = []
    timestamp_network_value = []

    if n_close:
        # if None in [start_time, end_time]:
        return dash.no_update, dash.no_update, dash.no_update, None, None, dash.no_update, dash.no_update, dash.no_update, \
               dash.no_update, dash.no_update, dash.no_update, None, dash.no_update, None, not is_open, None
    if n_submit:
        if None in [start_time, end_time]:
            return {'display': 'none'}, {'display': 'none'}, {
                'display': 'none'}, None, None, dash.no_update, radio_item, dash.no_update, \
                   dash.no_update, dash.no_update, dbc.Alert([html.P("Error adding dates. Please Enter time slot "
                                                                     "above on the right.",
                                                                     className="alert-heading"),
                                                              ],
                                                             color="danger",
                                                             fade=True), None, dash.no_update, None, not is_open, None
        else:
            try:
                datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
                datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
            except ValueError:
                return {'display': 'none'}, {'display': 'none'}, {
                    'display': 'none'}, None, None, dash.no_update, radio_item, dash.no_update, \
                       dash.no_update, dash.no_update, dbc.Alert([html.P("Error adding dates. Please Enter time slot "
                                                                         "above on the right.",
                                                                         className="alert-heading"),
                                                                  ],
                                                                 color="danger",
                                                                 fade=True), None, dash.no_update, None, not is_open, None
        return {'display': 'block'}, {'display': 'block'}, {
            'display': 'block'}, None, None, dash.no_update, radio_item, dash.no_update, \
               dash.no_update, dash.no_update, dash.no_update, None, dash.no_update, None, dash.no_update, None

    elif n_display:
        if "" or None in [family_indicator, indicator]:
            return {'display': 'block'}, {'display': 'block'}, {
                'display': 'block'}, None, None, dash.no_update, radio_item, \
                   dash.no_update, dash.no_update, dash.no_update, dbc.Alert(
                [html.P("Error adding indicators. Please enter an "
                        "indicator to be able to visualize the data.",
                        className="alert-heading"),
                 ],
                color="danger",
                fade=True), None, dash.no_update, None, not is_open, None

        if " No device has been selected yet" in radio_item:
            return {'display': 'block'}, {'display': 'block'}, {
                'display': 'block'}, None, None, dash.no_update, radio_item, \
                   dash.no_update, dash.no_update, None, dbc.Alert([html.P("Please choose device using the button "
                                                                           "left on the top",
                                                                           className="alert-heading"),
                                                                    ],
                                                                   color="danger",
                                                                   fade=True), None, dash.no_update, None, not is_open, None

        else:
            try:

                start_time_object = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
                start_timestamp = start_time_object.timestamp()
                end_time_object = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
                end_timestamp = end_time_object.timestamp()

                # Connect to the database
                try:
                    conn = psycopg2.connect(
                        host='localhost',
                        database='IoT_Messages',
                        user='postgres',
                        password='delhom!12'
                    )

                    # Create a cursor object
                    cur = conn.cursor()
                    # Execute a query

                    ######################### Acoustic #########################

                    cur.execute(
                        "SELECT {}, TimeStamp FROM {}_ac WHERE timestamp BETWEEN {} AND {}".format(indicator,
                                                                                                   radio_item,
                                                                                                   start_timestamp,
                                                                                                   end_timestamp))

                    # Fetch all rows
                    rows = cur.fetchall()

                    # Iterate through the rows and print the data
                    for row in rows:
                        indicator_value.append(row[0])
                        timestamp_value.append(datetime.fromtimestamp(row[1]))

                    df = pd.DataFrame({
                        'Time': timestamp_value,
                        'Acoustic level': indicator_value
                    })

                    sorted_df = df.sort_values('Time')

                    indicator_value_sorted = sorted_df['Acoustic level'].tolist()

                    last_non_none = None
                    for i, item in enumerate(indicator_value_sorted):
                        if item is not None:
                            last_non_none = item
                            for j in range(0, i):
                                if indicator_value_sorted[j] is None:
                                    indicator_value_sorted[j] = last_non_none

                    df_without_none = pd.DataFrame({
                        'Time': sorted_df['Time'],
                        'Acoustic level ({})'.format(indicator): indicator_value_sorted
                    })

                    df_without_none.fillna(0, inplace=True)
                    df_without_none['Acoustic level ({})'.format(indicator)] = df_without_none[
                        'Acoustic level ({})'.format(indicator)].apply(float)

                    aggregated_df = df_without_none.groupby('Time').mean()

                    fig = go.Figure(data=[go.Scatter(x=df_without_none['Time'],
                                                     y=df_without_none['Acoustic level ({})'.format(indicator)])])
                    fig.update_layout(
                        title={
                            'text': "Acoustic level of the {} indicator at station{} from {} to {} ".format(indicator,
                                                                                                            radio_item,
                                                                                                            datetime.fromtimestamp(
                                                                                                                start_timestamp),
                                                                                                            datetime.fromtimestamp(
                                                                                                                end_timestamp))},
                        title_font_size=30, title_x=0.5,
                        xaxis_title="Time",
                        yaxis_title="Acoustic level",
                        yaxis=dict(tickformat='%d%%', ticksuffix='db', tickfont=dict(size=20),
                                   title=dict(font=dict(size=20,
                                                        family=
                                                        'Arial '
                                                        'Black'))),
                        xaxis=dict(tickfont=dict(size=20), title=dict(font=dict(size=20, family='Arial Black'))))

                    found = False
                    for element in ac_graphs:
                        if indicator in str(element):
                            found = True
                            break

                    if not found:
                        ac_graphs.append(dcc.Graph(figure=fig, style={'display': 'inline'
                                                                                 '-block',
                                                                      'width': '160vh',
                                                                      'height': '50vh', 'margin-left': '30px'}))

                        all_data.append(aggregated_df)

                    else:
                        return dash.no_update, dash.no_update, dash.no_update, None, None, \
                               dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, \
                               dbc.Alert(
                                   [html.P("The '{}' indicator is already displayed on the screen.".format(indicator),
                                           className="alert-heading"),
                                    ],
                                   color="warning",
                                   fade=True), None, dash.no_update, None, not is_open, None

                    ######################### Battery #########################

                    cur.execute(
                        "SELECT battery, TimeStamp FROM {}_battery WHERE timestamp BETWEEN {} AND {}".format(radio_item,
                                                                                                             start_timestamp,
                                                                                                             end_timestamp))

                    # Fetch all rows
                    rows_battery = cur.fetchall()

                    # Iterate through the rows and print the data
                    for row_battery in rows_battery:
                        battery_level.append(row_battery[0])
                        timestamp_battery_value.append(datetime.fromtimestamp(row_battery[1]))

                    df_battery = pd.DataFrame({
                        'Time': timestamp_battery_value,
                        'Battery level': battery_level
                    })

                    sorted_df_battery = df_battery.sort_values('Time')
                    sorted_df_battery.fillna(0, inplace=True)
                    sorted_df_battery['Battery level'] = sorted_df_battery['Battery level'].apply(float)

                    aggregated_df_battery = sorted_df_battery.groupby('Time').mean()

                    # Create the bar chart
                    fig_battery = go.Figure(
                        data=[go.Bar(x=aggregated_df_battery.index, y=aggregated_df_battery['Battery level'],
                                     marker=dict(color=['red' if x < 20 else 'green' for x in
                                                        aggregated_df_battery['Battery level']]))])

                    fig_battery.update_layout(
                        title={
                            'text': "Battery level of station{} from {} to {} ".format(radio_item,
                                                                                       datetime.fromtimestamp(
                                                                                           start_timestamp),
                                                                                       datetime.fromtimestamp(
                                                                                           end_timestamp))},
                        title_font_size=30, title_x=0.5,
                        xaxis_title="Time",
                        yaxis_title="Battery level",
                        yaxis_side="right",
                        yaxis=dict(tickformat='%d%%', ticksuffix='%', tickfont=dict(size=20),
                                   title=dict(font=dict(size=20, family='Arial Black'))),
                        xaxis=dict(tickfont=dict(size=20), title=dict(font=dict(size=20, family='Arial Black')))
                    )

                    ######################### Network #########################

                    cur.execute(
                        "SELECT network, TimeStamp FROM {}_network WHERE timestamp BETWEEN {} AND {}".format(radio_item,
                                                                                                             start_timestamp,
                                                                                                             end_timestamp))

                    # Fetch all rows
                    rows_network = cur.fetchall()

                    # Iterate through the rows and print the data
                    for row_network in rows_network:
                        network_level.append(row_network[0])
                        timestamp_network_value.append(datetime.fromtimestamp(row_network[1]))

                    df_network = pd.DataFrame({
                        'Time': timestamp_network_value,
                        'Network status': network_level
                    })

                    sorted_df_network = df_network.sort_values('Time')
                    sorted_df_network.fillna(0, inplace=True)
                    sorted_df_network['Network status'] = sorted_df_network['Network status'].apply(float)

                    aggregated_df_network = sorted_df_network.groupby('Time').mean()

                    # Define custom labels
                    network_labels = ['Offline', 'Unstable', 'Poor', 'Fair', 'Good', 'Excellent']

                    # Round the values in the 'Network status' column to the nearest integer
                    aggregated_df_network['Network status'] = aggregated_df_network['Network status'].round().astype(
                        int)

                    # Create a list of custom labels based on the rounded values
                    network_status_labels = [network_labels[x] for x in aggregated_df_network['Network status']]

                    # Create the bar chart
                    fig_network = go.Figure(
                        data=[go.Bar(x=aggregated_df_network.index, y=aggregated_df_network['Network status'],
                                     marker=dict(color=['red' if x < 3 else 'green' for x in
                                                        aggregated_df_network['Network status']]))],
                        layout=go.Layout(yaxis=dict(tickvals=aggregated_df_network['Network status'],
                                                    ticktext=network_status_labels)))

                    fig_network.update_layout(
                        title={
                            'text': "Network status of station{} from {} to {} ".format(radio_item,
                                                                                        datetime.fromtimestamp(
                                                                                            start_timestamp),
                                                                                        datetime.fromtimestamp(
                                                                                            end_timestamp))},
                        title_font_size=30, title_x=0.5,
                        xaxis_title="Time",
                        yaxis_title="Network status",
                        yaxis=dict(tickfont=dict(size=20), title=dict(font=dict(size=20, family='Arial Black'))),
                        xaxis=dict(tickfont=dict(size=20), title=dict(font=dict(size=20, family='Arial Black')))
                    )

                except psycopg2.Error as e:
                    print(f'Error: {e}')

                finally:
                    # Close the cursor and connection
                    if cur:
                        cur.close()
                    if conn:
                        conn.close()

            except Exception as e:
                print("An error occurred:", str(e))

            return {'display': 'block'}, {'display': 'block'}, {
                'display': 'block'}, None, None, ac_graphs, radio_item, dcc.Graph(
                figure=fig_battery, style={'display': 'inline'
                                                      '-block',
                                           'width': '160vh',
                                           'height': '50vh'}), dcc.Graph(
                figure=fig_network, style={'display': 'inline'
                                                      '-block',
                                           'width': '160vh',
                                           'height': '50vh', 'margin-left': '30px'}), dash.no_update, dbc.Alert(
                [html.P("You can choose more indicators if you want to.",
                        className="alert-heading"),
                 ],
                color="info",
                fade=True), None, None, None, not is_open, None

    elif n_clear:
        ac_graphs = []
        all_data = []
        fig = go.Figure(data=[go.Scatter()])
        fig_battery = go.Figure(data=[go.Scatter()])
        fig_network = go.Figure(data=[go.Scatter()])
        fig.update_layout(
            title={'text': "No data has been added yet. Please choose a time slot and indicators to be able to "
                           "visualize the data.", 'font': dict(color='red')}, title_font_size=30, title_x=0.5,
            xaxis_title="Time",
            yaxis_title="Acoustic level")
        fig_battery.update_layout(
            title={
                'text': "No data has been added yet. Please choose a time slot and indicators to be able to "
                        "visualize the data.", 'font': dict(color='red')}, title_font_size=30, title_x=0.5,
            xaxis_title="Time",
            yaxis_title="Battery level")
        fig_network.update_layout(
            title={
                'text': "No data has been added yet. Please choose a time slot and indicators to be able to "
                        "visualize the data.", 'font': dict(color='red')}, title_font_size=30, title_x=0.5,
            xaxis_title="Time",
            yaxis_title="Network status")

        return {'display': 'block'}, {'display': 'block'}, {'display': 'block'}, None, None, dcc.Graph(figure=fig,
                                                                                                       style={
                                                                                                           'display': 'inline'
                                                                                                                      '-block',
                                                                                                           'width': '160vh',
                                                                                                           'height': '50vh',
                                                                                                           'margin-left': '30px'}), radio_item, dcc.Graph(
            figure=fig_battery,
            style={'display': 'inline'
                              '-block',
                   'width': '160vh',
                   'height': '50vh'}), dcc.Graph(
            figure=fig_network,
            style={'display': 'inline'
                              '-block',
                   'width': '160vh',
                   'height': '50vh',
                   'margin-left': '30px'}), dash.no_update, dash.no_update, None, dash.no_update, None, dash.no_update, None

    elif n_download:
        if radio_item == " No device has been selected yet":
            return {'display': 'block'}, {'display': 'block'}, {
                'display': 'block'}, None, None, dash.no_update, radio_item, dash.no_update, \
                   dash.no_update, dash.no_update, dbc.Alert([html.P("Error choosing device. Please select a device "
                                                                     "by clicking on the button left on the top.",
                                                                     className="alert-heading"),
                                                              ],
                                                             color="danger",
                                                             fade=True), None, dash.no_update, None, not is_open, None

        if None in [start_time, end_time]:
            return {'display': 'block'}, {'display': 'block'}, {
                'display': 'block'}, None, None, dash.no_update, radio_item, dash.no_update, \
                   dash.no_update, dash.no_update, dbc.Alert([html.P("Error adding dates. Please Enter time slot "
                                                                     "above on the right.",
                                                                     className="alert-heading"),
                                                              ],
                                                             color="danger",
                                                             dismissable=True,
                                                             fade=True), None, dash.no_update, None, not is_open, None

        if "" or None in [indicator]:
            return {'display': 'block'}, {'display': 'block'}, {
                'display': 'block'}, None, None, dash.no_update, radio_item, dash.no_update, \
                   dash.no_update, dash.no_update, dbc.Alert([html.P("Error adding indicators. Please enter the "
                                                                     "indicator on the right to be able to download "
                                                                     "the results.",
                                                                     className="alert-heading"),
                                                              ],
                                                             color="danger",
                                                             fade=True), None, dash.no_update, None, not is_open, None

        else:
            try:
                datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
                datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
            except ValueError:
                return {'display': 'block'}, {
                    'display': 'block'}, {'display': 'block'}, None, None, dash.no_update, radio_item, dash.no_update, \
                       dash.no_update, dash.no_update, dbc.Alert([html.P("Error adding dates. Please Enter time slot "
                                                                         "above on the right.",
                                                                         className="alert-heading"),
                                                                  ],
                                                                 color="danger",
                                                                 fade=True), None, dash.no_update, None, not is_open, None

            start_time_object = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
            start_timestamp = start_time_object.timestamp()
            end_time_object = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
            end_timestamp = end_time_object.timestamp()

            if not all_data:
                return {'display': 'block'}, {'display': 'block'}, {
                    'display': 'block'}, None, None, dash.no_update, radio_item, dash.no_update, dash.no_update, dash.no_update, dbc.Alert(
                    [html.P("No data has been added yet. Please add data to be able to download it",
                            className="alert-heading"),
                     ],
                    color="danger",
                    fade=True), None, dash.no_update, None, not is_open, None
            else:
                merged_df = all_data[0]

                for i in range(1, len(all_data)):
                    merged_df = pd.merge(merged_df, all_data[i], on='Time', how='outer')
                print(sorted_df_battery)
                aggregated_df_battery = sorted_df_battery.groupby('Time').mean()
                merged_df1 = merged_df.merge(aggregated_df_battery, on='Time', how='left')

                # print(sorted_df_network)
                aggregated_df_network = sorted_df_network.groupby('Time').mean()
                aggregated_df_network['Network status'] = aggregated_df_network['Network status'].round().astype(int)
                replacement_values = {0: 'Offline', 1: 'Unstable', 2: 'Poor', 3: 'Fair', 4: 'Good', 5: 'Excellent'}
                aggregated_df_network['Network status'] = aggregated_df_network['Network status'].map(
                    replacement_values)

                merged_df2 = merged_df1.merge(aggregated_df_network, on='Time', how='left')

                return {'display': 'block'}, {'display': 'block'}, {
                    'display': 'block'}, None, None, dash.no_update, radio_item, dash.no_update, dash.no_update, dcc.send_data_frame(
                    merged_df2.to_csv, "{} from {} to {}"
                                       ".csv".format(radio_item[1:],
                                                     datetime.fromtimestamp(
                                                         start_timestamp),
                                                     datetime.fromtimestamp(
                                                         end_timestamp))), dash.no_update, None, dash.no_update, None, dash.no_update, None

    fig = go.Figure(data=[go.Scatter()])
    fig_battery = go.Figure(data=[go.Scatter()])
    fig_network = go.Figure(data=[go.Scatter()])
    fig.update_layout(
        title={'text': "No data has been added yet. Please choose a time slot and indicators to be able to "
                       "visualize the data.", 'font': dict(color='red')}, title_font_size=30, title_x=0.5,
        xaxis_title="Time",
        yaxis_title="Acoustic level")
    fig_battery.update_layout(
        title={
            'text': "No data has been added yet. Please choose a time slot and indicators to be able to "
                    "visualize the data.", 'font': dict(color='red')}, title_font_size=30, title_x=0.5,
        xaxis_title="Time",
        yaxis_title="Battery level")
    fig_network.update_layout(
        title={
            'text': "No data has been added yet. Please choose a time slot and indicators to be able to "
                    "visualize the data.", 'font': dict(color='red')}, title_font_size=30, title_x=0.5,
        xaxis_title="Time",
        yaxis_title="Network status")
    return {'display': 'none'}, {'display': 'none'}, {'display': 'none'}, None, None, dcc.Graph(figure=fig,
                                                                                                style={
                                                                                                    'display': 'inline'
                                                                                                               '-block',
                                                                                                    'width': '160vh',
                                                                                                    'height': '50vh',
                                                                                                    'margin-left': '30px'}), radio_item, dcc.Graph(
        figure=fig_battery,
        style={'display': 'inline'
                          '-block',
               'width': '160vh',
               'height': '50vh'}), dcc.Graph(
        figure=fig_network,
        style={'display': 'inline'
                          '-block',
               'width': '160vh',
               'height': '50vh',
               'margin-left': '30px'}), dash.no_update, dash.no_update, None, dash.no_update, None, dash.no_update, None


if __name__ == '__main__':
    app.run_server(debug=True, port=8000)
