import enum
from os import environ

import duckdb
import datetime as dt


def get_motherduck_conn():
    motherduck_token = environ['motherduck_token']
    return duckdb.connect(f'md:my_db?motherduck_token={motherduck_token}')


def get_min_max_article_dates(_md_conn: duckdb.DuckDBPyConnection):
    min_article_date: dt.datetime
    max_article_date: dt.datetime

    article_dates = _md_conn.sql(
        """
        SELECT 
          publish_time_NY
        FROM llm_feature_extract_date_ny"""
    )

    min_article_date, max_article_date = _md_conn.sql(
        """
        SELECT 
          publish_time_NY.min(),
          publish_time_NY.max()
        FROM article_dates"""
    ).fetchone()

    return min_article_date.date(), max_article_date.date()


def get_publish_count_per_day(_md_conn: duckdb.DuckDBPyConnection):
    en_articles = _md_conn.sql(
        """
        SELECT *
        FROM llm_feature_extract_date_ny
        WHERE article_language = 'en'"""
    )

    query = f"""
        SELECT 
          publish_time_NY::DATE as date,
          count(*) AS count
        FROM en_articles
        GROUP BY date
        ORDER BY date"""

    return _md_conn.sql(query).pl()


def get_symbol_mentions_per_period(_md_conn: duckdb.DuckDBPyConnection, period: str, offset: int, limit: int = 100):
    # excludes symbols with count of 0

    data_for_for_en_articles = _md_conn.sql(
        """
        SELECT *,
          (financial_event_with_symbols->>'$[*].symbol.symbol')
            .list_distinct()
            .unnest()
            .upper()
            .trim() AS symbol
        FROM llm_feature_extract_date_ny
        WHERE article_language = 'en'"""
    )

    symbols = _md_conn.sql(
        f"""
        SELECT 
            symbol
        FROM data_for_for_en_articles
        WHERE 
            symbol IN (SELECT symbol FROM clean_symbols)
        GROUP BY symbol
        ORDER BY count(symbol) DESC
        OFFSET {offset}
        LIMIT {limit}"""
    )

    query = f"""
        SELECT
            date_trunc('{period}', publish_time_NY) AS date_period,
            symbol,
            count(symbol) AS symbol_count
        FROM
            data_for_for_en_articles
        WHERE 
            symbol IN (SELECT symbol FROM symbols)
        GROUP BY
            date_period, symbol
        ORDER BY
            symbol, date_period"""

    return _md_conn.sql(query).pl()


def get_avg_sentiment_per_day(_md_conn: duckdb.DuckDBPyConnection):
    symbols_with_sentiments = _md_conn.sql(
        f"""
        SELECT 
          url,
          publish_time_NY,
          (sentiments->'$[*].sentiment_score')::FLOAT[] AS sentiment_score,
          (sentiments->>'$[*].sentiment_confidence')::FLOAT[] AS sentiment_conf,
          (financial_event_with_symbols->>'$[*].symbol.symbol')
            .list_distinct()[1]
            .upper() 
              AS symbol
          FROM llm_feature_extract_date_ny"""
    )

    sentiment_weighted = _md_conn.sql(
        """
        SELECT 
          url,
          publish_time_NY.strftime('%Y-%m-%d') as date,
          publish_time_NY,
          symbol,
          (list_zip(sentiment_score, sentiment_conf)::STRUCT(v1 FLOAT, v2 FLOAT)[])
            .list_transform(x -> x.v1 * x.v2)[1]
              AS weighted_sentiment
        FROM symbols_with_sentiments
        WHERE
          symbol NOT NULL
          AND weighted_sentiment NOT NULL"""
    )

    sentiment_weighted_by_day = _md_conn.sql(
        """
        SELECT 
          date,
          median(weighted_sentiment) as median,
          mean(weighted_sentiment) as mean,
          count(*) as symbols_count
        FROM sentiment_weighted
        GROUP BY date
        ORDER BY date"""
    )

    query = f"""
    UNPIVOT sentiment_weighted_by_day
        ON median, mean
        INTO 
            NAME avg_method
            VALUE avg_val
    """

    return _md_conn.sql(query).pl()


def get_stocks_large_sentiment_change(_md_conn: duckdb.DuckDBPyConnection):
    """
    :param _md_conn:
    :return: dataframe of stock symbols with large sentiment variation.
    the variation metric is scaled by the symbols number of articles as a proportion of the max number of articles for any symbol
    """
    sentiment_with_symbol_relation = _md_conn.sql(
        """
        SELECT 
          url,
          publish_time_NY,
          (sentiments->'$[*].sentiment_score')::FLOAT[] AS sentiment_score,
          (sentiments->>'$[*].sentiment_confidence')::FLOAT[] AS sentiment_conf,
          (financial_event_with_symbols->>'$[*].symbol.symbol')
            .list_distinct()[1]
            .upper() 
              AS symbol
          FROM llm_feature_extract_date_ny"""
    )

    sentiment_weighted = _md_conn.sql(
        f"""
            SELECT 
              url,
              date_trunc('day', publish_time_NY) AS date_period,
              symbol,
              (list_zip(sentiment_score, sentiment_conf)::STRUCT(v1 FLOAT, v2 FLOAT)[])
                .list_transform(x -> x.v1 * x.v2)[1]
                  AS weighted_sentiment
            FROM sentiment_with_symbol_relation
            WHERE
              symbol IN (SELECT symbol FROM clean_symbols)
              AND weighted_sentiment NOT NULL"""
    )

    sentiment_std_dev_relation = _md_conn.sql(
        """
        SELECT 
          symbol,
          stddev_pop(weighted_sentiment) AS sentiment_std_dev,
          COUNT(*) AS symbol_count
        FROM sentiment_weighted
        GROUP BY symbol"""
    )

    max_symbol_count = _md_conn.sql(
        """
        SELECT
          max(symbol_count) AS max_symbol_count
        FROM sentiment_std_dev_relation"""
    )

    sentiment_std_dev_scaled_by_count_relation = _md_conn.sql(
        """
        SELECT
          symbol,
          symbol_count,
          sentiment_std_dev * (symbol_count / max_symbol_count) AS sentiment_std_dev_scaled
        FROM sentiment_std_dev_relation, max_symbol_count
        ORDER BY sentiment_std_dev_scaled DESC"""
    )

    return (
        sentiment_std_dev_scaled_by_count_relation
        .select('symbol')
        .pl()
    )


def get_symbol_exchanges_unnested_relation(_md_conn: duckdb.DuckDBPyConnection):
    return _md_conn.sql(
        """
        SELECT 
          (financial_event_with_symbols->>'$[*].symbol.symbol')
              .unnest()
              .upper()
              .trim()
            AS symbol,
          (financial_event_with_symbols->>'$[*].symbol.stock_exchanges[*]')
              .unnest()
              .upper()
              .trim()
            AS exchange,
          *  
        FROM llm_feature_extract"""
    )


class SymbolSortOption(enum.Enum):
    SENTIMENT_STD_DEV = enum.auto()
    NUMBER_OF_ARTICLES = enum.auto()
    SYMBOL = enum.auto()


def get_all_symbols_sorted_alphabetically(_md_conn: duckdb.DuckDBPyConnection, sort_by: str):
    symbols_with_exchange_relation = get_symbol_exchanges_unnested_relation(_md_conn)

    query = f"""
        SELECT 
          symbol
        FROM symbols_with_exchange_relation
        WHERE 
          symbol IN (SELECT symbol FROM clean_symbols)
          AND exchange IN ('NASDAQ', 'NYSE')
        GROUP BY symbol
        ORDER BY {sort_by}
        """

    return _md_conn.sql(query).pl()[:, 0].to_list()


def get_list_of_symbols(
        _md_conn: duckdb.DuckDBPyConnection,
        sort_option_str: str
):
    sort_option = SymbolSortOption[sort_option_str.upper()]

    match sort_option:
        case SymbolSortOption.SENTIMENT_STD_DEV:
            return get_stocks_large_sentiment_change(_md_conn)
        case SymbolSortOption.SYMBOL:
            return get_all_symbols_sorted_alphabetically(_md_conn, 'symbol')
        case SymbolSortOption.NUMBER_OF_ARTICLES:
            return get_all_symbols_sorted_alphabetically(_md_conn, 'count(*) DESC')
        case _:
            raise ValueError(f"Invalid sort option: {sort_option_str}")


def get_publish_freq_per_period_for_symbol(_md_conn: duckdb.DuckDBPyConnection, period, symbol: str):
    en_articles_with_symbols_relation = _md_conn.sql(
        """
        SELECT *,
          (financial_event_with_symbols->>'$[*].symbol.symbol')
            .list_distinct()
            .unnest()
            .upper()
            .trim() AS symbol
        FROM llm_feature_extract_date_ny
        WHERE article_language = 'en'"""
    )

    query = f"""
            SELECT
                date_trunc('{period}', publish_time_NY) AS date_period,
                count(*) AS count
            FROM
                en_articles_with_symbols_relation
            WHERE 
                symbol = '{symbol}'
            GROUP BY
                date_period
            ORDER BY
                date_period"""

    return _md_conn.sql(query).pl()


def get_avg_sentiment_per_period_for_symbol(_md_conn: duckdb.DuckDBPyConnection, period, symbol: str):
    sentiment_with_symbol_relation = _md_conn.sql(
        """
        SELECT 
          url,
          publish_time_NY,
          (sentiments->'$[*].sentiment_score')::FLOAT[] AS sentiment_score,
          (sentiments->>'$[*].sentiment_confidence')::FLOAT[] AS sentiment_conf,
          (financial_event_with_symbols->>'$[*].symbol.symbol')
            .list_distinct()[1]
            .upper() 
              AS symbol
          FROM llm_feature_extract_date_ny"""
    )

    filtered_by_symbol_relation = _md_conn.sql(
        f"""
        SELECT 
          url,
          date_trunc('{period}', publish_time_NY) AS date_period,
          symbol,
          (list_zip(sentiment_score, sentiment_conf)::STRUCT(v1 FLOAT, v2 FLOAT)[])
            .list_transform(x -> x.v1 * x.v2)[1]
              AS weighted_sentiment
        FROM sentiment_with_symbol_relation
        WHERE
          symbol = '{symbol}'
          AND weighted_sentiment NOT NULL"""
    )

    avgs_by_period_relation = _md_conn.sql(
        """
        SELECT 
          date_period,
          median(weighted_sentiment) as median,
          mean(weighted_sentiment) as mean
        FROM filtered_by_symbol_relation
        GROUP BY date_period
        ORDER BY date_period"""
    )

    query = f"""
    UNPIVOT avgs_by_period_relation
    ON median, mean
    INTO 
        NAME avg_method
        VALUE avg_val
    """

    return _md_conn.sql(query).pl()


def get_sentiment_day_return_pairs(_md_conn: duckdb.DuckDBPyConnection):
    sentiment_rel = _md_conn.sql(
        """
        SELECT 
          _id,
          publish_time_NY,
          (sentiments->'$[*].sentiment_score')::FLOAT[] AS sentiment_score,
          (sentiments->>'$[*].sentiment_confidence')::FLOAT[] AS sentiment_conf,
          (financial_event_with_symbols->>'$[*].symbol.symbol')
            .list_distinct()[1]
            .upper() 
              AS symbol
        FROM llm_feature_extract_date_ny
        WHERE sentiments.json_array_length() > 0"""
    )

    weighted_sentiment_rel = _md_conn.sql(
        """
        SELECT 
          _id AS id,
          publish_time_NY,
          symbol,
          (list_zip(sentiment_score, sentiment_conf)::STRUCT(v1 FLOAT, v2 FLOAT)[])
            .list_transform(x -> x.v1 * x.v2)[1]
              AS weighted_sentiment
        FROM sentiment_rel
        WHERE
          symbol IN (SELECT symbol FROM clean_symbols)
          AND weighted_sentiment NOT NULL"""
    )

    positions_returns_relation = get_position_returns_relation(_md_conn, weighted_sentiment_rel)

    position_with_sentiment_query = _md_conn.sql(
        """
        SELECT weighted_sentiment_rel.*,
          positions_returns_relation.* EXCLUDE(symbol, publish_time_NY)
        FROM weighted_sentiment_rel
        JOIN positions_returns_relation USING (id)"""
    )

    return position_with_sentiment_query.select("weighted_sentiment", "position_return").pl()


def get_position_returns_relation(
        _md_conn: duckdb.DuckDBPyConnection,
        prior_relation: duckdb.DuckDBPyRelation
):
    """
    :param _md_conn:
    :param prior_relation: must have columns: symbol, publish_time_NY, id
    :return: relation
    """
    minute_ohlc_table = _md_conn.sql(
        f"""
        SELECT *
        FROM minute_ohlc_ny_tz
        WHERE symbol IN (SELECT symbol FROM prior_relation)"""
    )

    first_ohlc_after_publish_time = _md_conn.sql(
        f"""
        SELECT prior_relation.symbol, prior_relation.publish_time_NY, minute_ohlc_table.timestamp_ny,
          minute_ohlc_table.* EXCLUDE(symbol, timestamp_ny),
          prior_relation.id
        FROM prior_relation 
        ASOF JOIN minute_ohlc_table 
          ON (prior_relation.symbol = minute_ohlc_table.symbol AND prior_relation.publish_time_NY < minute_ohlc_table.timestamp_ny)"""
    )

    ohlc_after_1_day = _md_conn.sql(
        """
        SELECT prior_relation.symbol, prior_relation.publish_time_NY, minute_ohlc_table.timestamp_ny,
          minute_ohlc_table.* EXCLUDE(symbol, timestamp_ny),
          prior_relation.id
        FROM prior_relation 
        ASOF JOIN minute_ohlc_table 
          ON (prior_relation.symbol = minute_ohlc_table.symbol AND prior_relation.publish_time_NY + INTERVAL '1 DAY' < minute_ohlc_table.timestamp_ny)"""
    )

    combined_ohlc_data = _md_conn.sql(
        """
        SELECT 
          first_ohlc_after_publish_time.symbol, 
          first_ohlc_after_publish_time.publish_time_NY,
          first_ohlc_after_publish_time.open as first_open_price, 
          first_ohlc_after_publish_time.timestamp_ny as first_open_ts,
          ohlc_after_1_day.close as last_close_price,
          ohlc_after_1_day.timestamp_ny as last_close_ts,
          first_ohlc_after_publish_time.id
        FROM first_ohlc_after_publish_time
        JOIN ohlc_after_1_day USING (id)"""
    )

    position_returns = _md_conn.sql(
        """
        SELECT *,
          last_close_ts - first_open_ts as time_in_position,
          last_close_price / first_open_price - 1 as position_return,
        FROM combined_ohlc_data"""
    )

    return position_returns


def get_most_similar_with_returns(_md_conn: duckdb.DuckDBPyConnection):
    embeddings_base = _md_conn.sql(
        """
        SELECT _id,
          publish_time_NY,
          (financial_event_with_symbols->>'$[*].symbol.symbol').unnest().upper() AS symbol,
          (financial_event_with_symbols->>'$[*].symbol.stock_exchanges[*]').unnest().upper() AS exchange,
          summary_embeddings::FLOAT[256] embeddings,
          summary
        FROM llm_feature_extract_date_ny"""
    )

    embeddings_joined_symbol = _md_conn.sql(
        """
        select
          t1.*, t2.*,
          array_cosine_similarity(t1.embeddings, t2.embeddings) AS similarity,
        FROM embeddings_base t1
        JOIN embeddings_base t2 ON (t1.symbol = t2.symbol)
        WHERE 
          t1.exchange IN ('NASDAQ')
          AND t1.symbol IN (SELECT symbol FROM clean_symbols)
          AND t1._id <> t2._id
          AND t2.publish_time_NY < t1.publish_time_NY - INTERVAL '10 DAYS'"""
    )

    embeddings_most_similar_only = _md_conn.sql(
        """
        SELECT 
          row_number() OVER (
            PARTITION BY _id
            ORDER BY similarity DESC
          ) AS rank,
          _id,
          similarity,
          symbol, 
          summary AS summary_0, 
          summary_1,
          publish_time_NY AS publish_time_NY_0, 
          publish_time_NY_1,
        FROM embeddings_joined_symbol
        QUALIFY rank = 1"""
    )

    embeddings_most_similar_first_article = _md_conn.sql(
        """
        SELECT
          symbol, 
          _id AS id,
          publish_time_NY_0 AS publish_time_NY,
          similarity
        FROM embeddings_most_similar_only"""
    )

    embeddings_most_similar_second_article = _md_conn.sql(
        """
        SELECT
          symbol, 
          _id AS id,
          publish_time_NY_1 AS publish_time_NY,
          similarity
        FROM embeddings_most_similar_only"""
    )

    position_returns_first_article = get_position_returns_relation(_md_conn, embeddings_most_similar_first_article)
    position_returns_second_article = get_position_returns_relation(_md_conn, embeddings_most_similar_second_article)

    embeddings_with_returns_first_article = embeddings_most_similar_first_article.join(
        position_returns_first_article,
        condition='id',
        how='inner'
    )

    embeddings_with_returns_second_article = embeddings_most_similar_second_article.join(
        position_returns_second_article,
        condition='id',
        how='inner'
    )

    embeddings_with_returns_first_article_cols_cleaned = _md_conn.sql(
        """
        SELECT * EXCLUDE(symbol_1, publish_time_NY_1)
        FROM embeddings_with_returns_first_article"""
    )

    embeddings_with_returns_second_article_cols_cleaned = _md_conn.sql(
        """
        SELECT * EXCLUDE(symbol_1, publish_time_NY_1)
        FROM embeddings_with_returns_second_article"""
    )

    base_cols = ['symbol', 'id', 'similarity']

    def build_cols(relation, suffix: str):
        alias_cols = [
            duckdb.ColumnExpression(c).alias(f'{c}_{suffix}')
            for c in relation.columns
            if c not in base_cols
        ]

        return base_cols + alias_cols

    embeddings_with_returns_first_article_final = embeddings_with_returns_first_article_cols_cleaned.select(
        *build_cols(
            embeddings_with_returns_first_article_cols_cleaned,
            'first_article'
        )
    )

    embeddings_with_returns_second_article_final = embeddings_with_returns_second_article_cols_cleaned.select(
        *build_cols(
            embeddings_with_returns_second_article_cols_cleaned,
            'second_article'
        )
    )

    embeddings_with_returns_both_articles = _md_conn.sql(
        """
        SELECT * EXCLUDE(similarity),
          similarity.round(2) AS similarity  
        FROM embeddings_with_returns_first_article_final
        JOIN embeddings_with_returns_second_article_final USING (id, symbol, similarity)"""
    )

    return embeddings_with_returns_both_articles.select(
        'position_return_first_article',
        'position_return_second_article',
        'similarity'
    ).pl()


def test_md_conn(md_conn: duckdb.DuckDBPyConnection):
    return md_conn.sql("SHOW DATABASES").pl()
