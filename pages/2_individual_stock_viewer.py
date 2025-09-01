import streamlit as st
from streamlit_news_data_lib.duckdb_retrievers import *
from streamlit_news_data_lib.duckdb_retrievers import SymbolSortOption
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# wrap functions with caching
get_motherduck_conn = st.cache_resource(get_motherduck_conn)

get_list_of_symbols = st.cache_data(get_list_of_symbols)
get_publish_freq_per_period_for_symbol = st.cache_data(get_publish_freq_per_period_for_symbol)
get_avg_sentiment_per_period_for_symbol = st.cache_data(get_avg_sentiment_per_period_for_symbol)

md_conn = get_motherduck_conn()

st.title('Individual Stock Viewer')

sort_option = st.radio(
    'Sort by:', [option.name for option in SymbolSortOption],
    captions=['(descending)', '(alphabetic)', '(descending)'],
    index=0,
    format_func=lambda x: x.replace('_', ' ').title(),
    horizontal=True
)

list_of_symbols = get_list_of_symbols(
    md_conn,
    sort_option,
)
stock_symbol = st.selectbox('Choose a stock symbol:', list_of_symbols)

period_selection = st.selectbox('Period', [d.name.lower() for d in DuckDatePartSpecifier])
symbol_freq_per_period = get_publish_freq_per_period_for_symbol(md_conn, period_selection, stock_symbol)
symbol_freq_per_period_plot = px.bar(
    symbol_freq_per_period,
    x='date_period',
    y='count',
    title=f'{stock_symbol} publish frequency per {period_selection}',
)
st.plotly_chart(symbol_freq_per_period_plot)

symbol_avg_sentiment_per_period = get_avg_sentiment_per_period_for_symbol(
    md_conn,
    period_selection,
    stock_symbol)

fig = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.1,
    subplot_titles=(
        f'{stock_symbol} avg sentiment per {period_selection}',
        'OHLC'
    )
)

fig.add_trace(go.Scatter(
    x=symbol_avg_sentiment_per_period['timestamp'],
    y=symbol_avg_sentiment_per_period['avg_sentiment'],
    mode='markers',
    name='avg sentiment',
), row=1, col=1)

symbol_avg_sentiment_per_period_plot = px.scatter(
    symbol_avg_sentiment_per_period,
    x='timestamp',
    y='avg_sentiment',
    title=f'{stock_symbol} avg sentiment per {period_selection}',
    color='avg_method',
)
symbol_avg_sentiment_per_period_plot.add_shape(
    dict(
        type="line",
        x0=0,
        y0=0,
        x1=1,
        y1=0,
        xref='paper',
        yref='y',
        line=dict(
            color="Red",
            width=3,
            dash="dashdot"
        ),
        opacity=0.5,
    )
)
symbol_avg_sentiment_per_period_plot.update_layout(
    yaxis=dict(range=[-100, 100])
)
st.plotly_chart(symbol_avg_sentiment_per_period_plot)

min_timestamp = symbol_avg_sentiment_per_period['timestamp'].min()
max_timestamp = symbol_avg_sentiment_per_period['timestamp'].max()

symbol_ohlc = get_ohlcv_data(
    md_conn,
    stock_symbol,
    period_selection,
    min_timestamp,
    max_timestamp
)

st.write(symbol_ohlc)

fig.add_trace(go.Candlestick(
    x=symbol_ohlc['timestamp'],
    open=symbol_ohlc['open'],
    high=symbol_ohlc['high'],
    low=symbol_ohlc['low'],
    close=symbol_ohlc['close'],
    showlegend=False
), row=2, col=1)

fig.update_layout(
    xaxis2_rangeslider_visible=False,
    width=900
)

fig.add_trace(go.Bar(
    x=symbol_ohlc['timestamp'],
    y=symbol_ohlc['volume'],
    showlegend=False
), row=3, col=1)

st.plotly_chart(
    fig,
    use_container_width=False)
