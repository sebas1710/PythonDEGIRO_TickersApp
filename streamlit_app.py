import math
import requests
import pandas as pd
import yfinance as yf
import streamlit as st

# ============================================================
# CONFIGURACIÓN
# ============================================================

API_KEY = "968bac07-5a9c-4358-9d85-a428d1e275d0"  # TODO: poné acá tu API key real
URL_OPENFIGI = "https://api.openfigi.com/v3/mapping"

EXCHANGES_CSV = "INPUT/codigos-exchange-FIGI.csv"  # fichero dentro del repo

st.set_page_config(
    page_title="Mapeo ISIN → Tickers",
    layout="wide"
)

st.title("🔎 Identificación de Tickers a partir del CSV de DEGIRO")


# ============================================================
# 0) Cargar mapping exchCode -> sufijos Yahoo
# ============================================================

@st.cache_data
def load_exchange_mapping(path: str):
    """
    Lee el CSV de equivalencias y devuelve:
      - EXCHANGE_MAP: dict exchCode -> lista de sufijos Yahoo
      - EXCH_CODES_DEGIRO: set de exchCode válidos (los del CSV)
    """
    df = pd.read_csv(path, sep=';')

    mapping = {}
    for _, row in df.iterrows():
        code = str(row["exchCode"]).strip().upper()
        suf = str(row["Sufijo Yahoo"]).strip()

        if suf == "" or suf.lower() == "nan":
            suffixes = [""]
        else:
            parts = [p.strip() for p in suf.split('/') if p.strip() != ""]
            suffixes = parts if parts else [""]

        mapping[code] = suffixes

    return mapping, set(mapping.keys())


EXCHANGE_MAP, EXCH_CODES_DEGIRO = load_exchange_mapping(EXCHANGES_CSV)


# ============================================================
# 1) Obtener ISIN únicos desde CSV de DEGIRO
# ============================================================

def get_unique_isin_from_degiro_df(df: pd.DataFrame) -> list:
    """
    Recibe el DataFrame original de DEGIRO y devuelve una lista de ISIN únicos:
      - Usa columna 'ISIN'
      - Elimina vacíos
      - Elimina filas cuyo ISIN contiene 'FLATEX'
      - Quita duplicados
    """
    if "ISIN" not in df.columns:
        raise Exception(
            f"El CSV de DEGIRO debe tener una columna llamada 'ISIN'. "
            f"Columnas encontradas: {list(df.columns)}"
        )

    isin_series = df["ISIN"].astype(str).str.strip()

    mask = (
        isin_series.notna()
        & (isin_series != "")
        & (~isin_series.str.contains("FLATEX", case=False, na=False))
    )

    unique_isin = isin_series[mask].drop_duplicates().tolist()
    return unique_isin


# ============================================================
# 2) FIGI: obtener todas las opciones para un ISIN
# ============================================================

@st.cache_data
def get_all_mappings_from_isin(isin: str):
    """
    Devuelve TODAS las combinaciones posibles para un ISIN desde OpenFIGI:
      ISIN, Ticker, Name, Exchange, FIGI
    """
    headers = {
        "Content-Type": "application/json",
        "X-OPENFIGI-APIKEY": API_KEY
    }

    payload = [{
        "idType": "ID_ISIN",
        "idValue": isin
    }]

    response = requests.post(URL_OPENFIGI, json=payload, headers=headers)

    if response.status_code != 200:
        st.warning(f"❌ Error con ISIN {isin}: {response.text}")
        return []

    data = response.json()

    if not data or "data" not in data[0] or data[0]["data"] is None:
        return []

    mappings = []
    for entry in data[0]["data"]:
        mappings.append({
            "ISIN": isin,
            "Ticker": entry.get("ticker"),
            "Name": entry.get("name"),
            "Exchange": entry.get("exchCode"),  # código FIGI (US, SM, LN, etc.)
            "FIGI": entry.get("figi")
        })

    return mappings


# ============================================================
# 3) FILTRAR: solo exchanges DEGIRO, afinando US y ES
# ============================================================

def filter_mappings_degiro_only(isin: str, mappings: list[dict]):
    """
    1) Solo exchanges presentes en tu CSV (EXCH_CODES_DEGIRO)
    2) ISIN 'US' -> preferimos solo mercados USA
    3) ISIN 'ES' -> preferimos solo mercados España (SM/BM)
    """
    filtered = []
    for m in mappings:
        exch = (m.get("Exchange") or "").upper()
        if exch in EXCH_CODES_DEGIRO:
            filtered.append(m)

    if not filtered:
        return []

    country = isin[:2].upper() if isinstance(isin, str) and len(isin) >= 2 else ""

    # ISIN de EEUU -> solo mercados USA
    if country == "US":
        us_like_exchanges = {
            "US",
            "NYS",
            "NSQ",
            "NAS",
            "NMS",
            "ARCA",
            "BATS",
        }

        filtered_us = [
            m for m in filtered
            if (m.get("Exchange") or "").upper() in us_like_exchanges
        ]

        if filtered_us:
            return filtered_us
        return filtered

    # ISIN de España -> solo mercados españoles
    if country == "ES":
        es_exchanges = {
            "SM",   # Spanish Market (Bolsa de Madrid)
            "BM",
        }

        filtered_es = [
            m for m in filtered
            if (m.get("Exchange") or "").upper() in es_exchanges
        ]

        if filtered_es:
            return filtered_es
        return filtered

    # Otros países: solo filtro DEGIRO
    return filtered


# ============================================================
# 4) Añadir candidatos Yahoo a cada mapping
# ============================================================

def add_yahoo_tickers_to_mappings(mappings: list[dict]):
    """
    A cada mapping le agrega una clave 'Yahoo_Tickers' con
    uno o varios tickers potenciales para Yahoo Finance.

    Lógica:
      - Obtenemos sufijos base desde EXCHANGE_MAP.
      - Para países NO ES/US, añadimos SIEMPRE '.AS' y '.DE'
        como candidatos extra (bolsas muy comunes en DEGIRO).
    """
    for m in mappings:
        exch = (m.get("Exchange") or "").upper()
        ticker = m.get("Ticker")
        isin = str(m.get("ISIN") or "").strip().upper()
        country = isin[:2] if len(isin) >= 2 else ""

        if not ticker:
            m["Yahoo_Tickers"] = None
            continue

        suffixes = EXCHANGE_MAP.get(exch)
        if not suffixes:
            suffixes = [""]

        if country not in ("ES", "US"):
            extra_suffixes = [".AS", ".DE"]
            all_suf = []
            for s in list(suffixes) + extra_suffixes:
                if s not in all_suf:
                    all_suf.append(s)
            suffixes = all_suf

        yahoo_tickers = []
        for suf in suffixes:
            suf = (suf or "").strip()
            if suf == "" or suf.lower() == "nan":
                yahoo_tickers.append(ticker)
            else:
                yahoo_tickers.append(f"{ticker}{suf}")

        m["Yahoo_Tickers"] = "|".join(yahoo_tickers)

    return mappings


# ============================================================
# 5) Obtener precio, moneda y nombre de bolsa desde Yahoo Finance
# ============================================================

@st.cache_data
def get_yahoo_quote(symbol: str):
    """
    Devuelve (precio_redondeado_2_decimales, moneda, nombre_bolsa)
    para un ticker de Yahoo. Si falla o no hay precio, devuelve (None, None, None).
    """
    try:
        t = yf.Ticker(symbol)

        price = None
        currency = None
        exch_name = None

        fast = getattr(t, "fast_info", None)
        if fast:
            try:
                get = getattr(fast, "get", None)
                if callable(get):
                    price = get("lastPrice") or get("last_price") or get("regularMarketPrice")
                    currency = get("currency")
                else:
                    price = getattr(fast, "last_price", None) or getattr(fast, "lastPrice", None) or getattr(fast, "regularMarketPrice", None)
                    currency = getattr(fast, "currency", None)
            except Exception:
                pass

        info = {}
        try:
            info = getattr(t, "info", {}) or {}
        except Exception:
            info = {}

        if price is None:
            price = info.get("regularMarketPrice") or info.get("previousClose")

        if currency is None:
            currency = info.get("currency")

        exch_name = (
            info.get("fullExchangeName")
            or info.get("exchange")
            or info.get("market")
        )

        if price is not None and not (isinstance(price, float) and math.isnan(price)):
            price = round(float(price), 2)
        else:
            price = None

        if price is None:
            return None, None, None

        return price, currency, exch_name
    except Exception as e:
        st.warning(f"⚠️ Error al obtener cotización de {symbol}: {e}")
        return None, None, None


def explode_yahoo_tickers_with_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Toma un DF que tiene una columna 'Yahoo_Tickers' con símbolos
    separados por '|' y genera un nuevo DF donde:
      - hay UNA fila por símbolo
      - añade columnas 'Yahoo_Ticker', 'LastPrice', 'Currency', 'Exchange_Name'
      - si no se encuentra precio para un símbolo, esa fila NO se añade
    """
    rows = []

    for _, row in df.iterrows():
        yt = row.get("Yahoo_Tickers", None)

        if pd.isna(yt) or not yt:
            continue

        symbols = [s.strip() for s in str(yt).split("|") if s.strip() != ""]

        for sym in symbols:
            price, curr, exch_name = get_yahoo_quote(sym)

            if price is None:
                continue

            new_row = row.copy()
            new_row["Yahoo_Ticker"] = sym
            new_row["LastPrice"] = price
            new_row["Currency"] = curr
            new_row["Exchange_Name"] = exch_name

            rows.append(new_row)

    return pd.DataFrame(rows)


# ============================================================
# 6) Pipeline: lista de ISIN -> FIGI -> Yahoo + precios
# ============================================================

def build_candidates_from_isin_list(isin_list: list[str]) -> pd.DataFrame:
    """
    Recibe una lista de ISIN y devuelve un DataFrame EXPANDIDO con:
      - FIGI mappings
      - Yahoo_Ticker uno por línea
      - precio, moneda, nombre bolsa
      - columna UnicoMultiple (unico/multiple por ISIN)
    """
    all_rows = []

    for isin in isin_list:
        if pd.isna(isin):
            continue

        isin_str = str(isin).strip()

        mappings = get_all_mappings_from_isin(isin_str)
        mappings = filter_mappings_degiro_only(isin_str, mappings)

        if not mappings:
            continue

        mappings = add_yahoo_tickers_to_mappings(mappings)
        all_rows.extend(mappings)

    if not all_rows:
        return pd.DataFrame()

    df_all = pd.DataFrame(all_rows)

    df_expanded = explode_yahoo_tickers_with_prices(df_all)

    if df_expanded.empty:
        return df_expanded

    counts = df_expanded.groupby("ISIN")["Yahoo_Ticker"].transform("count")
    df_expanded["UnicoMultiple"] = counts.apply(lambda c: "unico" if c == 1 else "multiple")

    return df_expanded


# ============================================================
# 7) UI: Subida de CSV DEGIRO
# ============================================================

uploaded_file = st.file_uploader(
    "1️⃣ Subí tu CSV de movimientos de DEGIRO",
    type=["csv"],
    help="Export directo de DEGIRO en formato CSV."
)

if uploaded_file is None:
    st.info("Subí primero el CSV de DEGIRO para empezar.")
    st.stop()

# Leemos usando coma como separador (formato estándar de DEGIRO)
try:
    df_degiro = pd.read_csv(uploaded_file, sep=',')
except Exception as e:
    st.error(f"Error leyendo el CSV de DEGIRO: {e}")
    st.stop()

st.success("✅ CSV cargado correctamente.")

if st.checkbox("Ver una muestra del CSV de DEGIRO"):
    st.dataframe(df_degiro.head())


# ============================================================
# 8) Ejecutar pipeline FIGI + Yahoo
# ============================================================

if st.button("2️⃣ Procesar ISIN y buscar tickers/cotizaciones"):
    with st.spinner("Procesando ISIN, consultando OpenFIGI y Yahoo Finance..."):
        isin_list = get_unique_isin_from_degiro_df(df_degiro)
        st.write(f"ISIN únicos encontrados (tras limpieza): **{len(isin_list)}**")

        df_candidates = build_candidates_from_isin_list(isin_list)

    if df_candidates.empty:
        st.error("No se logró obtener ninguna cotización válida. Revisá los datos.")
        st.stop()

    st.session_state["df_candidates"] = df_candidates
    st.success("✅ Proceso completado. Ahora podés validar los tickers.")


# Si ya tenemos df_candidates en sesión, mostramos las tablas de validación
if "df_candidates" not in st.session_state:
    st.stop()

df_candidates = st.session_state["df_candidates"]

st.subheader("3️⃣ Validación de tickers propuestos")

# --- Separar únicos y múltiples ---
counts = df_candidates.groupby("ISIN")["Yahoo_Ticker"].transform("count")
df_candidates["UnicoMultiple"] = counts.apply(lambda c: "unico" if c == 1 else "multiple")

df_unicos = df_candidates[df_candidates["UnicoMultiple"] == "unico"].copy()
df_multiples = df_candidates[df_candidates["UnicoMultiple"] == "multiple"].copy()

# Para únicos: una fila por ISIN
df_unicos = df_unicos.sort_values(["ISIN"])
df_unicos_simple = (
    df_unicos[["ISIN", "Name", "Yahoo_Ticker", "LastPrice", "Currency", "Exchange_Name"]]
    .drop_duplicates(subset=["ISIN"])
    .reset_index(drop=True)
)
df_unicos_simple["Ticker_Manual"] = ""

st.markdown("#### 3.a) ISIN con **una sola** cotización encontrada")
st.write("Si no completás nada en 'Ticker_Manual', se asumirá que el `Yahoo_Ticker` es correcto.")

df_unicos_edit = st.data_editor(
    df_unicos_simple,
    num_rows="fixed",
    key="unicos_editor",
    column_config={
        "Ticker_Manual": st.column_config.TextColumn("Ticker manual (opcional)")
    }
)

# Para múltiples: varias filas por ISIN + fila extra manual
st.markdown("---")
st.markdown("#### 3.b) ISIN con **múltiples** cotizaciones")

if df_multiples.empty:
    st.info("No se encontraron ISIN con múltiples cotizaciones. Podés pasar al siguiente paso.")
    df_multi_edit = pd.DataFrame()
else:
    df_multiples = df_multiples.sort_values(["ISIN", "Yahoo_Ticker"])
    df_multiples["Seleccionado"] = False
    df_multiples["Ticker_Manual"] = ""

    # Agregamos fila extra manual por ISIN
    extra_rows = []
    for isin, grp in df_multiples.groupby("ISIN"):
        extra = {
            "ISIN": isin,
            "Name": grp["Name"].iloc[0],
            "Ticker": None,
            "Exchange": None,
            "FIGI": None,
            "Yahoo_Tickers": None,
            "Yahoo_Ticker": "",
            "LastPrice": None,
            "Currency": "",
            "Exchange_Name": "",
            "UnicoMultiple": "multiple",
            "Seleccionado": False,
            "Ticker_Manual": ""
        }
        extra_rows.append(extra)

    df_extra = pd.DataFrame(extra_rows)
    df_multi_full = pd.concat([df_multiples, df_extra], ignore_index=True)

    st.write("Marcá con el check qué ticker es el bueno. Si ninguno encaja, usá la fila en blanco y completá `Ticker_Manual`.")
    df_multi_edit = st.data_editor(
        df_multi_full,
        num_rows="fixed",
        key="multi_editor",
        column_config={
            "Seleccionado": st.column_config.CheckboxColumn("Es el ticker bueno"),
            "Ticker_Manual": st.column_config.TextColumn("Ticker manual (si ninguno encaja)")
        }
    )


# ============================================================
# 9) Confirmar selección y recalcular cotizaciones finales
# ============================================================

if st.button("4️⃣ Confirmar tickers y recalcular cotizaciones finales"):
    # 1) Procesar únicos
    df_unicos_final = st.session_state.get("unicos_editor", df_unicos_edit)
    final_rows = []

    for _, row in df_unicos_final.iterrows():
        final_ticker = str(row["Ticker_Manual"]).strip() or str(row["Yahoo_Ticker"]).strip()
        final_rows.append({
            "ISIN": row["ISIN"],
            "Name": row["Name"],
            "Final_Ticker": final_ticker
        })

    # 2) Procesar múltiples
    if not df_multi_edit.empty:
        df_multi_final = st.session_state.get("multi_editor", df_multi_edit)

        for isin, grp in df_multi_final.groupby("ISIN"):
            # Filas marcadas
            sel = grp[grp["Seleccionado"] == True]

            final_ticker = None
            name = grp["Name"].dropna().astype(str).iloc[0] if not grp["Name"].dropna().empty else ""

            if not sel.empty:
                # Si hay manual en seleccionadas, prioridad
                manual_nonempty = sel[sel["Ticker_Manual"].astype(str).str.strip() != ""]
                if not manual_nonempty.empty:
                    final_ticker = manual_nonempty["Ticker_Manual"].astype(str).str.strip().iloc[0]
                else:
                    # Sino, usamos el Yahoo_Ticker de la primera seleccionada
                    yt_nonempty = sel[sel["Yahoo_Ticker"].astype(str).str.strip() != ""]
                    if not yt_nonempty.empty:
                        final_ticker = yt_nonempty["Yahoo_Ticker"].astype(str).str.strip().iloc[0]

            # Si no hay nada seleccionado, o no se pudo determinar, elegimos "la mejor" (primera con Yahoo_Ticker no vacío)
            if not final_ticker:
                candidates_nonempty = grp[grp["Yahoo_Ticker"].astype(str).str.strip() != ""]
                if not candidates_nonempty.empty:
                    final_ticker = candidates_nonempty["Yahoo_Ticker"].astype(str).str.strip().iloc[0]
                else:
                    # Como última opción, si hay Ticker_Manual rellenado en la fila extra, usamos ese
                    manual_any = grp[grp["Ticker_Manual"].astype(str).str.strip() != ""]
                    if not manual_any.empty:
                        final_ticker = manual_any["Ticker_Manual"].astype(str).str.strip().iloc[0]

            if final_ticker:
                final_rows.append({
                    "ISIN": isin,
                    "Name": name,
                    "Final_Ticker": final_ticker
                })

    if not final_rows:
        st.error("No se pudo determinar ningún ticker final. Revisá las selecciones.")
        st.stop()

    df_final_tickers = pd.DataFrame(final_rows).drop_duplicates(subset=["ISIN"]).reset_index(drop=True)

    # 3) Recalcular cotizaciones finales SOLO para tickers buenos
    st.markdown("---")
    st.subheader("5️⃣ Cotizaciones finales para tickers seleccionados")

    final_quotes = []
    with st.spinner("Buscando cotizaciones finales en Yahoo Finance..."):
        for _, row in df_final_tickers.iterrows():
            symbol = str(row["Final_Ticker"]).strip()
            price, curr, exch_name = get_yahoo_quote(symbol)
            final_quotes.append({
                "ISIN": row["ISIN"],
                "Name": row["Name"],
                "Final_Ticker": symbol,
                "LastPrice": price,
                "Currency": curr,
                "Exchange_Name": exch_name
            })

    df_final_quotes = pd.DataFrame(final_quotes)
    st.dataframe(df_final_quotes, use_container_width=True)

    csv_final = df_final_quotes.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "💾 Descargar tickers finales con cotizaciones",
        data=csv_final,
        file_name="tickers_finales_con_cotizaciones.csv",
        mime="text/csv"
    )
