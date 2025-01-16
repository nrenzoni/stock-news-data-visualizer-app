"""
"""
from streamlit_news_data_lib.duckdb_retrievers import *
import streamlit as st
import plotly.express as px
import polars as pl

from streamlit_news_data_lib.plotly_helpers import *

# wrap functions with caching
get_motherduck_conn = st.cache_resource(get_motherduck_conn)
get_min_max_article_dates = st.cache_data(get_min_max_article_dates)
get_publish_count_per_day = st.cache_data(get_publish_count_per_day)
get_symbol_mentions_per_period = st.cache_data(get_symbol_mentions_per_period)
get_avg_sentiment_per_day = st.cache_data(get_avg_sentiment_per_day)
get_sentiment_day_return_pairs = st.cache_data(get_sentiment_day_return_pairs)
get_most_similar_with_returns = st.cache_data(get_most_similar_with_returns)

md_conn = get_motherduck_conn()

st.title('Market Overview')

min_article_date, max_article_date = get_min_max_article_dates(md_conn)

st.write(f"Data range: [{min_article_date}, {max_article_date}]")

publish_count_per_day = get_publish_count_per_day(md_conn)
st.plotly_chart(
    px.bar(
        publish_count_per_day,
        x='date',
        y='count',
        title='Article publish count per day'
    )
)

period_selection = st.selectbox('Period', ['day', 'week', 'month'])
step = 10
offset_selection = st.number_input('Offset (choose different stocks)', value=0, step=1, min_value=0)
symbol_mentions_per_period = get_symbol_mentions_per_period(
    md_conn, period_selection, offset_selection * step,
    limit=step)
symbol_mentions_per_period_plot = px.bar(
    symbol_mentions_per_period,
    x='date_period',
    y='symbol_count',
    title=f'Symbol mentions per {period_selection}',
    color='symbol',
)
st.plotly_chart(symbol_mentions_per_period_plot)

avg_sentiment_per_day = get_avg_sentiment_per_day(md_conn)
avg_sentiment_per_day_plot = px.scatter(
    avg_sentiment_per_day,
    x='date',
    y='avg_val',
    title='Avg Sentiment Per Day (all stocks)',
    hover_data={'symbols_count': True},
    color='avg_method'
)
add_horizontal_line(
    avg_sentiment_per_day_plot,
    avg_sentiment_per_day['date'].min(),
    avg_sentiment_per_day['date'].max(),
    0
)

st.plotly_chart(avg_sentiment_per_day_plot)

sentiment_day_return_pairs = get_sentiment_day_return_pairs(md_conn)
sentiment_day_return_pairs_plot = px.scatter(
    sentiment_day_return_pairs,
    x='weighted_sentiment',
    y='position_return',
    title='Sentiment vs Return'
)
add_horizontal_line(
    sentiment_day_return_pairs_plot,
    sentiment_day_return_pairs['weighted_sentiment'].min(),
    sentiment_day_return_pairs['weighted_sentiment'].max(),
    0
)
sentiment_day_return_pairs_plot.update_yaxes(range=[-5, 5])
st.plotly_chart(sentiment_day_return_pairs_plot)

most_similar_with_returns = get_most_similar_with_returns(md_conn)
similarity_range = st.slider(
    'Select similarity range to filter below plot',
    min_value=float(most_similar_with_returns['similarity'].min()),
    max_value=float(most_similar_with_returns['similarity'].max()),
    value=(float(most_similar_with_returns['similarity'].min()), float(most_similar_with_returns['similarity'].max()))
)
most_similar_with_returns_filtered = most_similar_with_returns.filter(
    pl.col('similarity').is_between(
        similarity_range[0],
        similarity_range[1]
    )
)
most_similar_with_returns_plot = px.scatter(
    most_similar_with_returns_filtered,
    x='position_return_first_article',
    y='position_return_second_article',
    title='1 day stock % return for paired most similar articles<br>'
          '<span style="font-size: small;">(same stock, using 256 dim embeddings)</span>',
    color='similarity',
    hover_data={
        'position_return_first_article': ':.2f',
        'position_return_second_article': ':.2f',
        'similarity': ':.2f'
    }
)
most_similar_with_returns_plot.update_xaxes(range=[-1, 1])
most_similar_with_returns_plot.update_yaxes(range=[-1, 1])
most_similar_with_returns_plot.update_layout(title_x=0.25)
st.plotly_chart(most_similar_with_returns_plot)
