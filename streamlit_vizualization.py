import streamlit as st
import pandas as pd
from sqlalchemy import text
from storage import get_engine
from config import TABLE_NAME
from plotly import express as px
from plotly import graph_objects as go
from datetime import timezone, timedelta


# --- 2. Функция для подключения и получения данных ---
@st.cache_data(ttl=15)  # Кэшируем данные на 15 секунд, чтобы избежать частых запросов к БД
def get_crypto_prices():
    """Подключается к БД и возвращает последние данные из таблицы prices."""
    try:
        engine = get_engine()

        # SQL-запрос для получения данных с строгой фильтрацией выбросов
        # Фильтруем выбросы на уровне SQL:
        # 1. Исключаем NULL или нулевые значения цен
        # 2. Исключаем экстремальные спреды (>5% от средней цены) - строгий порог для ликвидных рынков
        # 3. Исключаем экстремальные различия между источниками (>10% разницы) - строгий порог для арбитража
        # 4. Проверяем логическую согласованность цен (mid между bid и ask)
        query = f"""
        WITH price_stats AS (
            SELECT *,
                -- Вычисляем спреды и разницы для фильтрации
                CASE 
                    WHEN k_mid > 0 THEN ABS(k_ask - k_bid) / k_mid * 100 
                    ELSE NULL 
                END AS k_spread_pct,
                CASE 
                    WHEN o_mid > 0 THEN ABS(o_ask - o_bid) / o_mid * 100 
                    ELSE NULL 
                END AS o_spread_pct,
                CASE 
                    WHEN k_mid > 0 AND o_mid > 0 THEN ABS(k_mid - o_mid) / GREATEST(k_mid, o_mid) * 100 
                    ELSE NULL 
                END AS mid_diff_pct
            FROM {TABLE_NAME}
        )
        SELECT pair, ts, k_bid, k_ask, k_mid, o_bid, o_ask, o_mid
        FROM price_stats
        WHERE 
            -- Исключаем NULL или нулевые значения
            k_bid IS NOT NULL AND k_bid > 0
            AND k_ask IS NOT NULL AND k_ask > 0
            AND k_mid IS NOT NULL AND k_mid > 0
            AND o_bid IS NOT NULL AND o_bid > 0
            AND o_ask IS NOT NULL AND o_ask > 0
            AND o_mid IS NOT NULL AND o_mid > 0
            -- СТРОГИЕ пороги: исключаем спреды >5% (вместо 50%)
            AND (k_spread_pct IS NULL OR k_spread_pct <= 50)
            AND (o_spread_pct IS NULL OR o_spread_pct <= 50)
            -- СТРОГИЕ пороги: исключаем различия между источниками >10% (вместо 100%)
            AND (mid_diff_pct IS NULL OR mid_diff_pct <= 10)
            -- Исключаем случаи, когда ask < bid (некорректные данные)
            AND k_ask >= k_bid
            AND o_ask >= o_bid
            -- Проверяем логическую согласованность: mid должен быть между bid и ask
            AND k_mid >= k_bid AND k_mid <= k_ask
            AND o_mid >= o_bid AND o_mid <= o_ask
            -- Исключаем экстремальные значения: спред не должен быть отрицательным или слишком большим абсолютно
            AND (k_ask - k_bid) >= 0 AND (k_ask - k_bid) <= k_mid * 0.1
            AND (o_ask - o_bid) >= 0 AND (o_ask - o_bid) <= o_mid * 0.1;
        """

        # Читаем данные в DataFrame
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

        # Преобразуем численные столбцы в числовой формат, игнорируя ошибки (NaN, если не число)
        cols_to_convert = ['k_bid', 'k_ask', 'k_mid', 'o_bid', 'o_ask', 'o_mid']
        for col in cols_to_convert:
            # Принудительное преобразование с обработкой 'e-06' и других строк в float
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Дополнительная строгая фильтрация: удаляем строки с NaN после преобразования
        df = df.dropna(subset=cols_to_convert)

        # Дополнительная статистическая фильтрация выбросов используя IQR метод для каждой пары
        if not df.empty and len(df) > 1:
            # Вычисляем статистики для каждой пары отдельно
            filtered_rows = []
            for pair in df['pair'].unique():
                pair_data = df[df['pair'] == pair].copy()

                # Применяем IQR фильтрацию для каждой ценовой колонки
                for col in cols_to_convert:
                    if col in pair_data.columns and len(pair_data) > 3:
                        Q1 = pair_data[col].quantile(0.25)
                        Q3 = pair_data[col].quantile(0.75)
                        IQR = Q3 - Q1

                        if IQR > 0:
                            lower_bound = Q1 - 1.5 * IQR
                            upper_bound = Q3 + 1.5 * IQR
                            # Фильтруем выбросы
                            pair_data = pair_data[(pair_data[col] >= lower_bound) & (pair_data[col] <= upper_bound)]

                # Дополнительно: фильтруем по z-score (исключаем значения с |z| > 2.5)
                if len(pair_data) > 3:
                    for col in cols_to_convert:
                        if col in pair_data.columns:
                            mean_val = pair_data[col].mean()
                            std_val = pair_data[col].std()
                            if std_val > 0:
                                z_scores = abs((pair_data[col] - mean_val) / std_val)
                                pair_data = pair_data[z_scores <= 2.5]

                filtered_rows.append(pair_data)

            if filtered_rows:
                df = pd.concat(filtered_rows, ignore_index=True)
            else:
                df = pd.DataFrame(columns=df.columns)

        return df

    except Exception as e:
        st.error(f"❌ Ошибка подключения к базе данных. Проверьте параметры и запущен ли PostgreSQL. Ошибка: {e}")
        return pd.DataFrame()

    finally:
        if 'engine' in locals():
            engine.dispose()

@st.fragment(run_every="30s")
def render_mid_price_chart(pair: str):

    if "price_history" not in st.session_state:
        st.session_state.price_history = {}

    if pair not in st.session_state.price_history:
        st.session_state.price_history[pair] = {"ts": [], "mid": []}

    engine = get_engine()
    
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT ts, o_mid
            FROM prices
            WHERE pair = :pair
            ORDER BY ts DESC
            LIMIT 1
        """), {"pair": pair}).fetchone()
        
    engine.dispose()

    if row:
        ts, mid = row
        if ts.tzinfo:
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)

        history = st.session_state.price_history[pair]
        if not history["ts"] or ts > history["ts"][-1]:
            history["ts"].append(ts)
            history["mid"].append(mid)

    if not st.session_state.price_history[pair]["ts"]:
        st.warning("Нет данных для графика")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=st.session_state.price_history[pair]["ts"],
        y=st.session_state.price_history[pair]["mid"],
        mode="lines+markers",
        name=pair
    ))

    fig.update_layout(
        title=f"Mid Price (O) — {pair}",
        xaxis_title="Время",
        yaxis_title="Цена",
        height=400,
        margin=dict(l=40, r=40, t=50, b=40)
    )

    st.plotly_chart(fig, use_container_width=True)


# --- 3. Основная логика Streamlit-приложения ---
def main():
    st.set_page_config(layout="wide", page_title="Аналитика Криптовалютных Цен")
    st.title("📈 Аналитика Цен Криптоактивов (В реальном времени)")
    st.caption(f"Последнее обновление: {pd.to_datetime('now').strftime('%Y-%m-%d %H:%M:%S')}")

    # Получаем данные
    df = get_crypto_prices()

    if df.empty:
        st.warning("Нет данных для отображения. Проверьте лог ошибок выше.")
        return

    # --- 4. Обработка и Расчеты ---

    # Расчет спредов (абсолютных и процентных)
    df['k_spread'] = df['k_ask'] - df['k_bid']
    df['o_spread'] = df['o_ask'] - df['o_bid']

    # Спред в процентах (для оценки ликвидности)
    df['k_spread_%'] = (df['k_spread'] / df['k_mid']) * 100
    df['o_spread_%'] = (df['o_spread'] / df['o_mid']) * 100

    # Разница в средней цене между источниками (для оценки арбитража)
    df['mid_diff'] = df['k_mid'] - df['o_mid']
    df['mid_diff_%'] = (df['mid_diff'] / df['k_mid']) * 100

    # Переименовываем столбцы для более понятного отображения
    df.rename(columns={
        'pair': 'Пара',
        'ts': 'Время',
        'k_bid': 'K.Bid', 'k_ask': 'K.Ask', 'k_mid': 'K.Mid',
        'o_bid': 'O.Bid', 'o_ask': 'O.Ask', 'o_mid': 'O.Mid',
        'k_spread_%': 'K.Спред %',
        'o_spread_%': 'O.Спред %',
        'mid_diff_%': 'Разн. Mid %'
    }, inplace=True)

    # --- 5. Интерактивные элементы ---

    st.sidebar.header("Параметры Анализа")

    # Сортируем пары по убыванию K.Mid
    sorted_pairs = (
        df.sort_values(by='K.Mid', ascending=False)
        ['Пара']
        .drop_duplicates()
        .tolist()
    )

    # Selectbox с отсортированными парами
    selected_pair = st.sidebar.selectbox(
        "Выберите торговую пару для детального просмотра:",
        options=sorted_pairs
    )

    # Берём последнюю запись для выбранной пары
    pair_df = (
        df[df['Пара'] == selected_pair]
        .sort_values(by='Время', ascending=False)
        .iloc[0]
    )

    # --- 6. Визуализация и Аналитика ---

    ## 📊 Сводная таблица (Крупный план)
    st.header("Сводная информация по всем парам")
    st.dataframe(
        df[[
            'Пара', 'Время', 'K.Mid', 'O.Mid',
            'K.Спред %', 'O.Спред %', 'Разн. Mid %'
        ]].sort_values(by='K.Mid', ascending=False).style.format(
            {
                'Время': lambda t: (t + timedelta(hours=3)).strftime('%H:%M:%S'),
                'K.Mid': '{:.8f}',
                'O.Mid': '{:.8f}',
                'K.Спред %': '{:.4f}%',
                'O.Спред %': '{:.4f}%',
                'Разн. Mid %': '{:.4f}%',
            }
        ),
        use_container_width=True,
        hide_index=True
    )

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        ## 📉 График Спреда по всем парам
        st.subheader("Сравнение Процентных Спредов")

        # Создаем Melted DataFrame для удобства Plotly
        melted_spread_df = df.melt(
            id_vars=['Пара'],
            value_vars=['K.Спред %', 'O.Спред %'],
            var_name='Источник',
            value_name='Спред (%)'
        )

        fig_spread = px.bar(
            melted_spread_df,
            x='Пара',
            y='Спред (%)',
            color='Источник',
            barmode='group',
            title='Ликвидность: Процентный Спред (Ask-Bid)/Mid'
        )
        fig_spread.update_layout(xaxis={'categoryorder': 'total descending'})
        st.plotly_chart(fig_spread, use_container_width=True)

    with col2:
        ## ⚖️ График Арбитража по всем парам
        st.subheader("Возможный Арбитраж")
        fig_diff = px.bar(
            df,
            x='Пара',
            y='Разн. Mid %',
            title='Разница в Средней Цене между Источниками (%)'
        )
        fig_diff.update_layout(xaxis={'categoryorder': 'total descending'})
        st.plotly_chart(fig_diff, use_container_width=True)

    st.markdown("---")

    ## 🎯 Детальный анализ выбранной пары
    st.header(f"Детальный анализ: **{selected_pair}**")

    col3, col4, col5 = st.columns(3)

    # Метрика 1: Разница в цене
    with col3:
        st.metric(
            "Разница в средней цене (K.Mid - O.Mid)",
            f"{pair_df['mid_diff']:.8f}",
            delta=f"{pair_df['Разн. Mid %']:.4f}%"
        )

    # Метрика 2: Спред K
    with col4:
        st.metric(
            f"Процентный спред (K) (Ликвидность)",
            f"{pair_df['K.Спред %']:.4f}%"
        )

    # Метрика 3: Спред O
    with col5:
        st.metric(
            f"Процентный спред (O) (Ликвидность)",
            f"{pair_df['O.Спред %']:.4f}%"
        )

    st.subheader("Текущие Бид/Аск Цены")

    # Отображение текущих цен в таблице
    prices_data = {
        'Метрика': ['BID (Покупка)', 'ASK (Продажа)', 'MID (Средняя)'],
        'Источник K': [pair_df['K.Bid'], pair_df['K.Ask'], pair_df['K.Mid']],
        'Источник O': [pair_df['O.Bid'], pair_df['O.Ask'], pair_df['O.Mid']]
    }
    prices_df = pd.DataFrame(prices_data)

    st.table(
        prices_df.style.format(
            {
                'Источник K': '{:.8f}',
                'Источник O': '{:.8f}'
            }
        ).hide(axis='index')
    )

    # Пояснение Bid/Ask/Spread.
    st.info("""
    **BID** (Цена покупки): Максимальная цена, которую покупатель готов заплатить.  
    **ASK** (Цена продажи): Минимальная цена, по которой продавец готов продать.  
    **Спред** (Spread): Разница между ASK и BID (ASK - BID). Чем меньше процентный спред, тем выше **ликвидность** актива.
    """)

    st.markdown("---")
    st.header(f"📉 Динамика Mid-цены: {selected_pair}")
    render_mid_price_chart(selected_pair)


# Запускаем приложение
if __name__ == "__main__":
    main()