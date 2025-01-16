import streamlit as st
from streamlit_news_data_lib.duckdb_retrievers import *
from streamlit_news_data_lib.duckdb_retrievers import SymbolSortOption
import plotly.express as px

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

period_selection = st.selectbox('Period', ['day', 'week', 'month'])
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
symbol_avg_sentiment_per_period_plot = px.scatter(
    symbol_avg_sentiment_per_period,
    x='date_period',
    y='avg_val',
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
