import json
import re
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

# --- 1. THEME & CONFIGURATION ---
st.set_page_config(page_title="ESG Nexus | Master Repository", layout="wide")

st.markdown(
    """
    <style>
        .main { background-color: #f1f5f9; }
        h1 { color: #1e3a8a !important; }
        section[data-testid="stSidebar"] {
            background-color: #111827;
        }
        section[data-testid="stSidebar"] * {
            color: #e5e7eb;
        }
        section[data-testid="stSidebar"] .stRadio label {
            font-weight: 600;
        }
        .stExpander {
            background-color: #f0fdf4 !important;
            border: 2px solid #16a34a !important;
            border-radius: 10px;
        }
        div.stButton > button { font-weight: bold; border-radius: 8px; }
        div.stButton > button[kind="primary"] { background-color: #dc2626 !important; color: white !important; }
        div.stButton > button[kind="secondary"] { background-color: #2563eb !important; color: white !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

KPI_FILE = Path("KPIMaster_WithTopics - Functional Team(KPIMaster_WithTopics).csv")
CODES_FILE = Path("KPIMaster_WithTopics - Functional Team(Codes).csv")
DMA_QUESTIONNAIRE_FILE = Path("Predefined Industry specific questionnaires - Copy.xlsx")
IRO_FILE = Path("IRO database.xlsx")


@st.cache_data
def load_data():
    df_m = pd.read_csv(KPI_FILE, encoding="latin1", on_bad_lines="skip")
    df_c_raw = pd.read_csv(CODES_FILE, encoding="latin1", on_bad_lines="skip")

    p_map = {1.0: "Environmental", 2.0: "Social", 3.0: "Governance", 4.0: "General"}

    try:
        h_idx = df_c_raw[df_c_raw.iloc[:, 6] == "TopicCode"].index[0]
        df_topics = pd.read_csv(CODES_FILE, skiprows=h_idx + 1, encoding="latin1")
        t_id_to_name = dict(zip(df_topics["TopicCode"].astype(str), df_topics["Name"].astype(str)))
        t_name_to_id = dict(zip(df_topics["Name"].astype(str), df_topics["TopicCode"].astype(str)))
    except (IndexError, KeyError, pd.errors.ParserError):
        t_id_to_name, t_name_to_id = {}, {}

    agg_map = {
        0.0: "NONE",
        1.0: "SUM",
        2.0: "MATCH_AND_APPEND",
        3.0: "APPEND",
        4.0: "DUPLICATE",
        5.0: "CUSTOM",
        6.0: "MUL",
        7.0: "DIV",
        8.0: "SUB",
        9.0: "PERCENTAGE",
        10.0: "AVERAGE",
        11.0: "MATCH_AND_APPEND2",
    }
    type_map = {
        1.0: "TextBlock (Narrative)",
        2.0: "Table (Title)",
        3.0: "Numeric (Table)",
        4.0: "TextArea (Narrative in Table)",
        0.0: "None",
    }

    fw_map = {
        1: "ISSB",
        2: "BRSR",
        3: "GRI",
        4: "ESRS",
        5: "ASRS",
        6: "GENERAL",
        7: "CSRD",
        8: "SASB",
        9: "DJSI",
        10: "CHRB",
        11: "TNFD",
        12: "ISE",
        13: "Ecovadis",
        14: "Others",
    }
    return df_m, p_map, t_id_to_name, t_name_to_id, agg_map, type_map, fw_map


def read_latest_master() -> pd.DataFrame:
    return pd.read_csv(KPI_FILE, encoding="latin1", on_bad_lines="skip")


def save_master(df: pd.DataFrame) -> None:
    tmp_path = KPI_FILE.with_suffix(KPI_FILE.suffix + ".tmp")
    df.to_csv(tmp_path, index=False)
    tmp_path.replace(KPI_FILE)


@st.cache_data
def load_dma_questionnaire_data() -> tuple[pd.DataFrame, str]:
    df = pd.read_excel(DMA_QUESTIONNAIRE_FILE)
    with pd.ExcelFile(DMA_QUESTIONNAIRE_FILE) as workbook:
        sheet_name = workbook.sheet_names[0]
    return df, sheet_name


def read_latest_dma_questionnaire() -> tuple[pd.DataFrame, str]:
    df = pd.read_excel(DMA_QUESTIONNAIRE_FILE)
    with pd.ExcelFile(DMA_QUESTIONNAIRE_FILE) as workbook:
        sheet_name = workbook.sheet_names[0]
    return df, sheet_name


def save_dma_questionnaire(df: pd.DataFrame, sheet_name: str) -> None:
    tmp_path = DMA_QUESTIONNAIRE_FILE.with_suffix(DMA_QUESTIONNAIRE_FILE.suffix + ".tmp")
    with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    tmp_path.replace(DMA_QUESTIONNAIRE_FILE)


@st.cache_data
def load_iro_data() -> tuple[pd.DataFrame, str]:
    df = pd.read_excel(IRO_FILE)
    with pd.ExcelFile(IRO_FILE) as workbook:
        sheet_name = workbook.sheet_names[0]
    return df, sheet_name


def read_latest_iro_data() -> tuple[pd.DataFrame, str]:
    df = pd.read_excel(IRO_FILE)
    with pd.ExcelFile(IRO_FILE) as workbook:
        sheet_name = workbook.sheet_names[0]
    return df, sheet_name


def save_iro_data(df: pd.DataFrame, sheet_name: str) -> None:
    tmp_path = IRO_FILE.with_suffix(IRO_FILE.suffix + ".tmp")
    with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    tmp_path.replace(IRO_FILE)


def _make_arrow_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    safe_df = df.copy()
    for col in safe_df.columns:
        if pd.api.types.is_datetime64_any_dtype(safe_df[col]):
            safe_df[col] = safe_df[col].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
        elif safe_df[col].dtype == "object":
            safe_df[col] = safe_df[col].where(safe_df[col].notna(), "").astype(str)
    return safe_df


def _reset_after_create(module_name: str) -> None:
    st.session_state.clear()
    st.session_state["selected_module"] = module_name


(df_master, p_map, t_id_to_name, t_name_to_id, agg_map, type_map, fw_map) = load_data()

if "active_pillar" not in st.session_state:
    st.session_state["active_pillar"] = "All"
if "selected_framework_filter" not in st.session_state:
    st.session_state["selected_framework_filter"] = []
if "table_col_count" not in st.session_state:
    st.session_state["table_col_count"] = 3
if "tabular_cell_data" not in st.session_state:
    st.session_state["tabular_cell_data"] = {}
if "editing_cell" not in st.session_state:
    st.session_state["editing_cell"] = None
if "tabular_column_headers" not in st.session_state:
    st.session_state["tabular_column_headers"] = {}
if "tabular_row_headers" not in st.session_state:
    st.session_state["tabular_row_headers"] = {}
if "iro_entry_draft" not in st.session_state:
    st.session_state["iro_entry_draft"] = {"Impact": None, "Risk": None, "Opportunity": None}
if "iro_active_dialog" not in st.session_state:
    st.session_state["iro_active_dialog"] = None
if "pending_kpi_delete" not in st.session_state:
    st.session_state["pending_kpi_delete"] = None
if "flash_message" not in st.session_state:
    st.session_state["flash_message"] = None


def get_next_iris_code(df, pillar_selection):
    prefix = pillar_selection[0].upper()
    existing_codes = df["IrisKPICode"].dropna().astype(str)
    nums = []
    for code in existing_codes:
        if code.startswith(prefix):
            match = re.search(r"\d+", code)
            if match:
                nums.append(int(match.group()))
    next_num = max(nums) + 1 if nums else 1
    return f"{prefix}_{str(next_num).zfill(4)}"


def build_static_cell_code_preview(pillar_selection: str, row_count: int, col_count: int) -> dict[tuple[int, int], str]:
    latest_master = read_latest_master()
    preview_df = latest_master.copy()
    preview_codes: dict[tuple[int, int], str] = {}
    for i in range(row_count):
        for j in range(col_count):
            next_code = get_next_iris_code(preview_df, pillar_selection)
            preview_codes[(i, j)] = next_code
            preview_df = pd.concat(
                [preview_df, pd.DataFrame([{"IrisKPICode": next_code}])],
                ignore_index=True,
            )
    return preview_codes


def parse_fw(detail, target_id):
    if pd.isna(detail):
        return "—"
    try:
        data = json.loads(str(detail).replace("'", '"'))
        for item in data:
            if item.get("Standard") == target_id:
                return item.get("Description", "—")
    except (json.JSONDecodeError, TypeError):
        pass
    return "—"


def parse_fw_reference_code(detail, target_id):
    if pd.isna(detail):
        return "—"
    try:
        data = json.loads(str(detail).replace("'", '"'))
        for item in data:
            if item.get("Standard") == target_id:
                return item.get("ReferenceCode", "—")
    except (json.JSONDecodeError, TypeError):
        pass
    return "—"


def _pick_col(columns, *candidates):
    lookup = {c.lower(): c for c in columns}
    for candidate in candidates:
        col = lookup.get(candidate.lower())
        if col:
            return col
    return None


def _new_row_template(columns):
    return {c: pd.NA for c in columns}


def _set_if_present(row, columns, value, *candidates):
    col = _pick_col(columns, *candidates)
    if col is not None:
        row[col] = value


def _next_group_code(df):
    parent_col = _pick_col(df.columns, "ParentCode")
    if not parent_col:
        return "Group_1"
    max_id = 0
    for val in df[parent_col].dropna().astype(str):
        m = re.search(r"Group_(\d+)", val)
        if m:
            max_id = max(max_id, int(m.group(1)))
    return f"Group_{max_id + 1}"


def _extract_parent_iris_from_kpidetail(value):
    if pd.isna(value):
        return None
    try:
        payload = json.loads(str(value).replace("'", '"'))
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get("ParentIrisKPICode"):
                    return str(item.get("ParentIrisKPICode"))
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    return None


def _is_parent_table_row(kpidetail):
    if pd.isna(kpidetail):
        return False
    try:
        payload = json.loads(str(kpidetail).replace("'", '"'))
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get("HierarchyType") == "PARENT_TABLE":
                    return True
    except (json.JSONDecodeError, TypeError, ValueError):
        return False
    return False


def _is_non_empty_cell(value) -> bool:
    return pd.notna(value) and str(value).strip() != ""


def _infer_iro_type(row, impact_col, risk_col, opportunity_col) -> str:
    if impact_col and _is_non_empty_cell(row.get(impact_col)):
        return "Impact"
    if risk_col and _is_non_empty_cell(row.get(risk_col)):
        return "Risk"
    if opportunity_col and _is_non_empty_cell(row.get(opportunity_col)):
        return "Opportunity"
    return ""


def _has_meaningful_geo_value(value) -> bool:
    if pd.isna(value):
        return False
    normalized = str(value).strip().lower()
    return normalized not in {"", "0", "false", "no", "n", "nan", "none"}


def _get_iro_country_columns(columns, start_after: str, end_before: str | None = None) -> list[str]:
    cols = list(columns)
    if start_after not in cols:
        return []
    start_index = cols.index(start_after) + 1
    end_index = cols.index(end_before) if end_before in cols else len(cols)
    base_countries = {
        "Africa",
        "Asia",
        "Middle East and North Africa",
        "North America",
        "Europe",
        "Australia",
        "South America",
    }
    return [col for col in cols[start_index:end_index] if str(col).split(".")[0] in base_countries]


def remove_kpi_records(selected_kpi_code: str) -> int:
    latest_master = read_latest_master()
    initial_count = len(latest_master)

    target_rows = latest_master[latest_master["IrisKPICode"].astype(str) == str(selected_kpi_code)]
    if target_rows.empty:
        raise LookupError("Selected KPI code was not found in the latest master data.")

    target_row = target_rows.iloc[0]
    codes_to_remove = {str(selected_kpi_code)}

    parent_code_col = _pick_col(latest_master.columns, "ParentCode")
    if parent_code_col:
        parent_code_val = target_row.get(parent_code_col)
        if pd.notna(parent_code_val) and str(parent_code_val).strip():
            parent_table_rows = latest_master[latest_master[parent_code_col].astype(str) == str(parent_code_val)]
            has_parent_table = parent_table_rows["KPIDetail"].apply(_is_parent_table_row).any()
            if has_parent_table:
                codes_to_remove.update(parent_table_rows["IrisKPICode"].dropna().astype(str).tolist())

    if "KPIDetail" in latest_master.columns:
        parent_links = latest_master["KPIDetail"].apply(_extract_parent_iris_from_kpidetail)
        linked_rows = latest_master[parent_links.isin(codes_to_remove)]
        if not linked_rows.empty:
            codes_to_remove.update(linked_rows["IrisKPICode"].dropna().astype(str).tolist())

    keep_mask = ~latest_master["IrisKPICode"].astype(str).isin(codes_to_remove)
    updated_master = latest_master[keep_mask].copy()
    removed_count = initial_count - len(updated_master)

    save_master(updated_master)
    return removed_count


def _set_flash_message(message: str) -> None:
    st.session_state["flash_message"] = message


def _show_flash_message() -> None:
    msg = st.session_state.get("flash_message")
    if msg:
        st.success(msg)
        st.session_state["flash_message"] = None


def _get_excel_bytes(df: pd.DataFrame, sheet_name: str) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()


def render_dma_questionnaire_module():
    st.title("DMA Questionnaire Master Console")
    _show_flash_message()

    if not DMA_QUESTIONNAIRE_FILE.exists():
        st.error(f"DMA questionnaire source file not found: {DMA_QUESTIONNAIRE_FILE}")
        return

    try:
        questionnaire_df, questionnaire_sheet = load_dma_questionnaire_data()
    except ValueError:
        st.error("DMA questionnaire file is empty or unreadable.")
        return
    except FileNotFoundError:
        st.error(f"DMA questionnaire source file not found: {DMA_QUESTIONNAIRE_FILE}")
        return

    if questionnaire_df.empty and len(questionnaire_df.columns) == 0:
        st.error("DMA questionnaire file does not contain a usable sheet schema.")
        return

    question_col = _pick_col(questionnaire_df.columns, "Question")
    mandarin_col = _pick_col(questionnaire_df.columns, "Mandarin")
    industry_col = _pick_col(questionnaire_df.columns, "Industry")
    context_type_col = _pick_col(questionnaire_df.columns, "Context Type", "ContextType")
    updated_on_col = _pick_col(
        questionnaire_df.columns, "Updated On", "Updated On:", "UpdatedOn", "Updated Date", "Last Updated"
    )

    required_cols = {
        "Question": question_col,
        "Mandarin": mandarin_col,
        "Industry": industry_col,
        "Context Type": context_type_col,
        "Updated On": updated_on_col,
    }
    missing_cols = [name for name, col in required_cols.items() if col is None]
    if missing_cols:
        st.error(f"DMA questionnaire file is missing required column(s): {', '.join(missing_cols)}")
        return

    with st.expander("🟢 ADD NEW QUESTIONNAIRE"):
        c1, c2 = st.columns(2)
        with c1:
            in_question = st.text_input("Question", key="dma_question_input")
            industry_options = sorted(
                [v for v in questionnaire_df[industry_col].dropna().astype(str).unique() if v.strip()]
            )
            in_industry = st.selectbox("Industry", options=industry_options, key="dma_industry_input")
        with c2:
            in_mandarin = st.text_input("Mandarin", key="dma_mandarin_input")
            context_type_options = sorted(
                [v for v in questionnaire_df[context_type_col].dropna().astype(str).unique() if v.strip()]
            )
            in_context_type = st.selectbox(
                "Context Type", options=context_type_options, key="dma_context_type_input"
            )

        if st.button("✅ Create & Save Questionnaire", type="primary"):
            try:
                latest_df, latest_sheet = read_latest_dma_questionnaire()
                schema_cols = list(latest_df.columns)
                new_row = _new_row_template(schema_cols)
                _set_if_present(new_row, schema_cols, in_question, "Question")
                _set_if_present(new_row, schema_cols, in_mandarin, "Mandarin")
                _set_if_present(new_row, schema_cols, in_industry, "Industry")
                _set_if_present(new_row, schema_cols, in_context_type, "Context Type", "ContextType")
                _set_if_present(
                    new_row,
                    schema_cols,
                    pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Updated On",
                    "Updated On:",
                    "UpdatedOn",
                    "Updated Date",
                    "Last Updated",
                )

                save_dma_questionnaire(
                    pd.concat([latest_df, pd.DataFrame([new_row])], ignore_index=True), latest_sheet
                )
                st.cache_data.clear()
                _set_flash_message("Questionnaire is being added. Updated data has been saved.")
                _reset_after_create("DMA Questionnaire")
                st.rerun()
            except PermissionError:
                st.error("Close the Excel file!")

    if "dma_edit_mode" not in st.session_state:
        st.session_state.dma_edit_mode = False

    dma_edit_col, dma_search_col = st.columns([1.6, 4.4])
    with dma_edit_col:
        st.write("")
        if not st.session_state.dma_edit_mode:
            if st.button("✏️ Edit Existing DMA", key="dma_enable_edit_mode"):
                st.session_state.dma_edit_mode = True
                st.rerun()
        else:
            if st.button("🔒 Lock Editing", key="dma_disable_edit_mode"):
                st.session_state.dma_edit_mode = False
                st.rerun()
    with dma_search_col:
        dma_search = st.text_input(
            "🔍 Search",
            placeholder="Search questions or context...",
            key="dma_search_input",
        )

    filtered_df = questionnaire_df.copy()
    if dma_search:
        filtered_df = filtered_df[
            filtered_df.astype(str).apply(lambda x: x.str.contains(dma_search, case=False)).any(axis=1)
        ]
    filtered_df = filtered_df.reset_index().rename(columns={"index": "_row_id"})

    dma_gb = GridOptionsBuilder.from_dataframe(filtered_df)
    dma_gb.configure_default_column(
        editable=False,
        sortable=True,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
        floatingFilter=True,
        resizable=True,
    )
    dma_gb.configure_column("_row_id", hide=True)
    dma_gb.configure_column(
        industry_col,
        editable=st.session_state.dma_edit_mode,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
    )
    dma_gb.configure_column(
        context_type_col,
        editable=st.session_state.dma_edit_mode,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
    )
    dma_gb.configure_column(
        question_col,
        editable=st.session_state.dma_edit_mode,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
    )
    dma_gb.configure_column(
        mandarin_col,
        editable=st.session_state.dma_edit_mode,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
    )
    dma_gb.configure_column(
        updated_on_col,
        editable=False,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
    )
    dma_gb.configure_grid_options(
        stopEditingWhenCellsLoseFocus=True,
        suppressMenuHide=False,
        alwaysShowHorizontalScroll=True,
        alwaysShowVerticalScroll=True,
        rowSelection="single",
        suppressRowClickSelection=False,
    )
    dma_gb.configure_selection("single", use_checkbox=False)
    dma_gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)

    dma_grid_df = _make_arrow_safe_df(filtered_df)

    dma_grid_response = AgGrid(
        dma_grid_df,
        gridOptions=dma_gb.build(),
        update_on=["cellValueChanged", "filterChanged", "sortChanged", "selectionChanged"],
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=False,
        height=500,
        enable_enterprise_modules=True,
        key="dma_preview_grid",
    )

    edited_dma_df = pd.DataFrame(dma_grid_response["data"])
    selected_dma_rows = dma_grid_response.get("selected_rows", [])
    if "dma_details_popup_dismissed_for" not in st.session_state:
        st.session_state["dma_details_popup_dismissed_for"] = None
    if isinstance(selected_dma_rows, pd.DataFrame) and not selected_dma_rows.empty:
        candidate_row = selected_dma_rows.iloc[0].to_dict()
        candidate_code = str(candidate_row.get("Question", ""))
        if st.session_state.get("dma_details_popup_dismissed_for") != candidate_code:
            st.session_state["selected_dma_preview_row"] = candidate_row
    elif isinstance(selected_dma_rows, list) and len(selected_dma_rows) > 0:
        candidate_row = selected_dma_rows[0]
        candidate_code = str(candidate_row.get("Question", ""))
        if st.session_state.get("dma_details_popup_dismissed_for") != candidate_code:
            st.session_state["selected_dma_preview_row"] = candidate_row

    @st.dialog("DMA Row Details")
    def show_dma_row_details_dialog():
        row_data = st.session_state.get("selected_dma_preview_row", {})
        if row_data:
            for key, value in row_data.items():
                st.markdown(f"**{key}:** {value if pd.notna(value) else '—'}")
        if st.button("Close", key="close_dma_row_details_dialog"):
            st.session_state["dma_details_popup_dismissed_for"] = str(row_data.get(question_col, ""))
            st.session_state["selected_dma_preview_row"] = None
            st.rerun()

    if st.session_state.get("selected_dma_preview_row"):
        show_dma_row_details_dialog()

    _, dma_actions_right = st.columns([6, 2], gap="small")
    with dma_actions_right:
        dma_action_col_left, dma_action_col_right = st.columns([1, 1], gap="small")
        with dma_action_col_left:
            dma_save_clicked = st.button(
                "💾 SAVE ALL CHANGES",
                type="primary",
                key="dma_save_all_changes",
                use_container_width=True,
                disabled=not st.session_state.dma_edit_mode,
            )
        with dma_action_col_right:
            latest_q_df, latest_q_sheet = read_latest_dma_questionnaire()
            st.download_button(
                "⬇️ Download Updated Excel",
                data=_get_excel_bytes(latest_q_df, latest_q_sheet),
                file_name=DMA_QUESTIONNAIRE_FILE.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dma_download_excel",
                use_container_width=True,
            )

    if dma_save_clicked:
        try:
            latest_df, latest_sheet = read_latest_dma_questionnaire()
            for _, row in edited_dma_df.iterrows():
                row_id = row.get("_row_id")
                if pd.notna(row_id) and int(row_id) in latest_df.index:
                    latest_df.at[int(row_id), question_col] = row.get(question_col, latest_df.at[int(row_id), question_col])
                    latest_df.at[int(row_id), mandarin_col] = row.get(
                        mandarin_col, latest_df.at[int(row_id), mandarin_col]
                    )
                    latest_df.at[int(row_id), industry_col] = row.get(
                        industry_col, latest_df.at[int(row_id), industry_col]
                    )
                    latest_df.at[int(row_id), context_type_col] = row.get(
                        context_type_col, latest_df.at[int(row_id), context_type_col]
                    )

            save_dma_questionnaire(latest_df, latest_sheet)
            st.success("DMA questionnaire database updated!")
            st.cache_data.clear()
            st.rerun()
        except PermissionError:
            st.error("Close the Excel file!")


def render_dma_iro_module():
    st.title("DMA IRO Database Console")
    _show_flash_message()

    if not IRO_FILE.exists():
        st.error(f"IRO database source file not found: {IRO_FILE}")
        return

    try:
        iro_df, iro_sheet = load_iro_data()
    except ValueError:
        st.error("IRO database file is empty or unreadable.")
        return
    except FileNotFoundError:
        st.error(f"IRO database source file not found: {IRO_FILE}")
        return

    if iro_df.empty and len(iro_df.columns) == 0:
        st.error("IRO database file does not contain a usable sheet schema.")
        return

    industry_col = _pick_col(iro_df.columns, "Industry")
    material_topic_col = _pick_col(iro_df.columns, "Material Topic")
    impact_col = _pick_col(
        iro_df.columns, "Impacts (How your organization affects society, environment, and economy)"
    )
    impact_horizon_col = _pick_col(iro_df.columns, "Impacts Time Horizon")
    type_of_impact_col = _pick_col(iro_df.columns, "Type of Impact")
    nature_of_impact_col = _pick_col(iro_df.columns, "Nature of Impact")
    risk_col = _pick_col(iro_df.columns, "Risks (How sustainability issues can harm your business)")
    risk_horizon_col = _pick_col(iro_df.columns, "Risks Time Horizon")
    risk_category_col = _pick_col(iro_df.columns, "Risk Category")
    opportunity_col = _pick_col(
        iro_df.columns, "Opportunities (How sustainability trends can create advantages for your business)"
    )
    opportunity_horizon_col = _pick_col(iro_df.columns, "Opportunities Time Horizon")
    impact_country_cols = _get_iro_country_columns(iro_df.columns, "Nature of Impact", risk_col)
    risk_country_cols = _get_iro_country_columns(iro_df.columns, "Risk Category", opportunity_col)
    opportunity_country_cols = _get_iro_country_columns(iro_df.columns, "Opportunities Time Horizon")
    country_filter_options = [
        country
        for country in [
            "Africa",
            "Asia",
            "Middle East and North Africa",
            "North America",
            "Europe",
            "Australia",
            "South America",
        ]
        if any(str(col).split(".")[0] == country for col in iro_df.columns)
    ]

    required_cols = {
        "Industry": industry_col,
        "Impacts": impact_col,
        "Impacts Time Horizon": impact_horizon_col,
        "Risks": risk_col,
        "Risks Time Horizon": risk_horizon_col,
        "Opportunities": opportunity_col,
        "Opportunities Time Horizon": opportunity_horizon_col,
    }
    missing_cols = [name for name, col in required_cols.items() if col is None]
    if missing_cols:
        st.error(f"IRO database file is missing required column(s): {', '.join(missing_cols)}")
        return

    working_iro_df = iro_df.copy()
    working_iro_df["_IRO Type"] = working_iro_df.apply(
        lambda row: _infer_iro_type(row, impact_col, risk_col, opportunity_col), axis=1
    )
    working_iro_df["_Description"] = working_iro_df.apply(
        lambda row: row.get(impact_col)
        if row.get("_IRO Type") == "Impact"
        else row.get(risk_col)
        if row.get("_IRO Type") == "Risk"
        else row.get(opportunity_col)
        if row.get("_IRO Type") == "Opportunity"
        else "",
        axis=1,
    )
    working_iro_df["_Time Horizon"] = working_iro_df.apply(
        lambda row: row.get(impact_horizon_col)
        if row.get("_IRO Type") == "Impact"
        else row.get(risk_horizon_col)
        if row.get("_IRO Type") == "Risk"
        else row.get(opportunity_horizon_col)
        if row.get("_IRO Type") == "Opportunity"
        else "",
        axis=1,
    )

    with st.expander("🟢 ADD NEW IRO ENTRY"):
        c1, c2 = st.columns(2)
        with c1:
            industry_options = sorted(
                [v for v in working_iro_df[industry_col].dropna().astype(str).unique() if v.strip()]
            )
            in_industry = st.selectbox("Industry", options=industry_options, key="iro_industry_input")
        with c2:
            material_topic_options = sorted(
                [v for v in working_iro_df[material_topic_col].dropna().astype(str).unique() if v.strip()]
            ) if material_topic_col else []
            in_material_topic = st.selectbox(
                "Material Topic",
                options=material_topic_options if material_topic_options else [""],
                key="iro_material_topic_input",
            )

        action_cols = st.columns(3)
        if action_cols[0].button(
            "Add Impact" if st.session_state["iro_entry_draft"]["Impact"] is None else "Edit Impact",
            key="iro_add_impact",
        ):
            st.session_state["iro_active_dialog"] = "Impact"
        if action_cols[1].button(
            "Add Risk" if st.session_state["iro_entry_draft"]["Risk"] is None else "Edit Risk",
            key="iro_add_risk",
        ):
            st.session_state["iro_active_dialog"] = "Risk"
        if action_cols[2].button(
            "Add Opportunity"
            if st.session_state["iro_entry_draft"]["Opportunity"] is None
            else "Edit Opportunity",
            key="iro_add_opportunity",
        ):
            st.session_state["iro_active_dialog"] = "Opportunity"

        configured_sections = [
            section for section, payload in st.session_state["iro_entry_draft"].items() if payload is not None
        ]
        st.caption(
            "Configured sections: " + ", ".join(configured_sections) if configured_sections else "No IRO section added yet."
        )

        active_iro_dialog = st.session_state.get("iro_active_dialog")
        if active_iro_dialog is not None:
            if active_iro_dialog == "Impact":
                dialog_horizon_col = impact_horizon_col
                dialog_country_cols = impact_country_cols
                existing_payload = st.session_state["iro_entry_draft"]["Impact"] or {}
                dialog_horizon_options = sorted(
                    [v for v in working_iro_df[dialog_horizon_col].dropna().astype(str).unique() if v.strip()]
                )
                dialog_type_of_impact_options = sorted(
                    [v for v in working_iro_df[type_of_impact_col].dropna().astype(str).unique() if v.strip()]
                ) if type_of_impact_col else []
                dialog_nature_of_impact_options = sorted(
                    [v for v in working_iro_df[nature_of_impact_col].dropna().astype(str).unique() if v.strip()]
                ) if nature_of_impact_col else []

                @st.dialog("Add Impact")
                def render_impact_dialog():
                    impact_horizon = st.selectbox(
                        "Impacts Time Horizon",
                        options=dialog_horizon_options if dialog_horizon_options else [""],
                        index=dialog_horizon_options.index(existing_payload.get("time_horizon"))
                        if existing_payload.get("time_horizon") in dialog_horizon_options and dialog_horizon_options
                        else 0,
                        key="iro_dialog_impact_horizon",
                    )
                    impact_type = st.selectbox(
                        "Type of Impact",
                        options=dialog_type_of_impact_options if dialog_type_of_impact_options else [""],
                        index=dialog_type_of_impact_options.index(existing_payload.get("type_of_impact"))
                        if existing_payload.get("type_of_impact") in dialog_type_of_impact_options
                        and dialog_type_of_impact_options
                        else 0,
                        key="iro_dialog_type_of_impact",
                    )
                    impact_nature = st.selectbox(
                        "Nature of Impact",
                        options=dialog_nature_of_impact_options if dialog_nature_of_impact_options else [""],
                        index=dialog_nature_of_impact_options.index(existing_payload.get("nature_of_impact"))
                        if existing_payload.get("nature_of_impact") in dialog_nature_of_impact_options
                        and dialog_nature_of_impact_options
                        else 0,
                        key="iro_dialog_nature_of_impact",
                    )
                    impact_countries = st.multiselect(
                        "Geographical Area",
                        options=sorted({str(col).split(".")[0] for col in dialog_country_cols}),
                        default=existing_payload.get("countries", []),
                        key="iro_dialog_impact_countries",
                    )
                    impact_description = st.text_area(
                        "Impact Description / Statement",
                        value=existing_payload.get("description", ""),
                        key="iro_dialog_impact_description",
                    )
                    if st.button("Save Impact", key="iro_dialog_save_impact"):
                        st.session_state["iro_entry_draft"]["Impact"] = {
                            "time_horizon": impact_horizon,
                            "type_of_impact": impact_type,
                            "nature_of_impact": impact_nature,
                            "countries": impact_countries,
                            "description": impact_description,
                        }
                        st.session_state["iro_active_dialog"] = None
                        st.rerun()

                render_impact_dialog()

            elif active_iro_dialog == "Risk":
                dialog_horizon_col = risk_horizon_col
                dialog_country_cols = risk_country_cols
                existing_payload = st.session_state["iro_entry_draft"]["Risk"] or {}
                dialog_horizon_options = sorted(
                    [v for v in working_iro_df[dialog_horizon_col].dropna().astype(str).unique() if v.strip()]
                )
                dialog_risk_category_options = sorted(
                    [v for v in working_iro_df[risk_category_col].dropna().astype(str).unique() if v.strip()]
                ) if risk_category_col else []

                @st.dialog("Add Risk")
                def render_risk_dialog():
                    risk_horizon = st.selectbox(
                        "Risks Time Horizon",
                        options=dialog_horizon_options if dialog_horizon_options else [""],
                        index=dialog_horizon_options.index(existing_payload.get("time_horizon"))
                        if existing_payload.get("time_horizon") in dialog_horizon_options and dialog_horizon_options
                        else 0,
                        key="iro_dialog_risk_horizon",
                    )
                    risk_category = st.selectbox(
                        "Risk Category",
                        options=dialog_risk_category_options if dialog_risk_category_options else [""],
                        index=dialog_risk_category_options.index(existing_payload.get("risk_category"))
                        if existing_payload.get("risk_category") in dialog_risk_category_options
                        and dialog_risk_category_options
                        else 0,
                        key="iro_dialog_risk_category",
                    )
                    risk_countries = st.multiselect(
                        "Geographical Area",
                        options=sorted({str(col).split(".")[0] for col in dialog_country_cols}),
                        default=existing_payload.get("countries", []),
                        key="iro_dialog_risk_countries",
                    )
                    risk_description = st.text_area(
                        "Risk Description / Statement",
                        value=existing_payload.get("description", ""),
                        key="iro_dialog_risk_description",
                    )
                    if st.button("Save Risk", key="iro_dialog_save_risk"):
                        st.session_state["iro_entry_draft"]["Risk"] = {
                            "time_horizon": risk_horizon,
                            "risk_category": risk_category,
                            "countries": risk_countries,
                            "description": risk_description,
                        }
                        st.session_state["iro_active_dialog"] = None
                        st.rerun()

                render_risk_dialog()

            elif active_iro_dialog == "Opportunity":
                dialog_horizon_col = opportunity_horizon_col
                dialog_country_cols = opportunity_country_cols
                existing_payload = st.session_state["iro_entry_draft"]["Opportunity"] or {}
                dialog_horizon_options = sorted(
                    [v for v in working_iro_df[dialog_horizon_col].dropna().astype(str).unique() if v.strip()]
                )

                @st.dialog("Add Opportunity")
                def render_opportunity_dialog():
                    opportunity_horizon = st.selectbox(
                        "Opportunities Time Horizon",
                        options=dialog_horizon_options if dialog_horizon_options else [""],
                        index=dialog_horizon_options.index(existing_payload.get("time_horizon"))
                        if existing_payload.get("time_horizon") in dialog_horizon_options and dialog_horizon_options
                        else 0,
                        key="iro_dialog_opportunity_horizon",
                    )
                    opportunity_countries = st.multiselect(
                        "Geographical Area",
                        options=sorted({str(col).split(".")[0] for col in dialog_country_cols}),
                        default=existing_payload.get("countries", []),
                        key="iro_dialog_opportunity_countries",
                    )
                    opportunity_description = st.text_area(
                        "Opportunity Description / Statement",
                        value=existing_payload.get("description", ""),
                        key="iro_dialog_opportunity_description",
                    )
                    if st.button("Save Opportunity", key="iro_dialog_save_opportunity"):
                        st.session_state["iro_entry_draft"]["Opportunity"] = {
                            "time_horizon": opportunity_horizon,
                            "countries": opportunity_countries,
                            "description": opportunity_description,
                        }
                        st.session_state["iro_active_dialog"] = None
                        st.rerun()

                render_opportunity_dialog()

        if st.button("✅ Create & Save IRO Entry", type="primary", key="iro_add_button"):
            try:
                latest_df, latest_sheet = read_latest_iro_data()
                schema_cols = list(latest_df.columns)
                new_row = _new_row_template(schema_cols)
                _set_if_present(new_row, schema_cols, in_industry, "Industry")
                _set_if_present(new_row, schema_cols, in_material_topic, "Material Topic")

                impact_payload = st.session_state["iro_entry_draft"].get("Impact")
                risk_payload = st.session_state["iro_entry_draft"].get("Risk")
                opportunity_payload = st.session_state["iro_entry_draft"].get("Opportunity")

                if impact_payload is not None:
                    _set_if_present(
                        new_row,
                        schema_cols,
                        impact_payload.get("description", ""),
                        "Impacts (How your organization affects society, environment, and economy)",
                    )
                    _set_if_present(new_row, schema_cols, impact_payload.get("time_horizon", ""), "Impacts Time Horizon")
                    _set_if_present(new_row, schema_cols, impact_payload.get("type_of_impact", ""), "Type of Impact")
                    _set_if_present(
                        new_row, schema_cols, impact_payload.get("nature_of_impact", ""), "Nature of Impact"
                    )
                    for country_col in impact_country_cols:
                        if country_col in schema_cols:
                            new_row[country_col] = (
                                "Yes"
                                if str(country_col).split(".")[0] in impact_payload.get("countries", [])
                                else "No"
                            )

                if risk_payload is not None:
                    _set_if_present(
                        new_row,
                        schema_cols,
                        risk_payload.get("description", ""),
                        "Risks (How sustainability issues can harm your business)",
                    )
                    _set_if_present(new_row, schema_cols, risk_payload.get("time_horizon", ""), "Risks Time Horizon")
                    _set_if_present(new_row, schema_cols, risk_payload.get("risk_category", ""), "Risk Category")
                    for country_col in risk_country_cols:
                        if country_col in schema_cols:
                            new_row[country_col] = (
                                "Yes"
                                if str(country_col).split(".")[0] in risk_payload.get("countries", [])
                                else "No"
                            )

                if opportunity_payload is not None:
                    _set_if_present(
                        new_row,
                        schema_cols,
                        opportunity_payload.get("description", ""),
                        "Opportunities (How sustainability trends can create advantages for your business)",
                    )
                    _set_if_present(
                        new_row,
                        schema_cols,
                        opportunity_payload.get("time_horizon", ""),
                        "Opportunities Time Horizon",
                    )
                    for country_col in opportunity_country_cols:
                        if country_col in schema_cols:
                            new_row[country_col] = (
                                "Yes"
                                if str(country_col).split(".")[0] in opportunity_payload.get("countries", [])
                                else "No"
                            )

                save_iro_data(pd.concat([latest_df, pd.DataFrame([new_row])], ignore_index=True), latest_sheet)
                st.cache_data.clear()
                _set_flash_message("IRO entry is being added. Updated data has been saved.")
                _reset_after_create("DMA IRO Database")
                st.rerun()
            except PermissionError:
                st.error("Close the Excel file!")

    if "iro_edit_mode" not in st.session_state:
        st.session_state.iro_edit_mode = False

    iro_edit_col, iro_search_col = st.columns([1.6, 4.4])
    with iro_edit_col:
        st.write("")
        if not st.session_state.iro_edit_mode:
            if st.button("✏️ Edit Existing IRO", key="iro_enable_edit_mode"):
                st.session_state.iro_edit_mode = True
                st.rerun()
        else:
            if st.button("🔒 Lock Editing", key="iro_disable_edit_mode"):
                st.session_state.iro_edit_mode = False
                st.rerun()
    with iro_search_col:
        iro_search = st.text_input(
            "🔍 Search",
            placeholder="Search across all IRO fields...",
            key="iro_search_input",
        )

    filtered_iro_df = working_iro_df.copy()
    if iro_search:
        filtered_iro_df = filtered_iro_df[
            filtered_iro_df.astype(str).apply(lambda x: x.str.contains(iro_search, case=False)).any(axis=1)
        ]
    grid_iro_df = filtered_iro_df.reset_index().rename(columns={"index": "_row_id"})
    grid_iro_df = grid_iro_df[
        ["_row_id", industry_col, "_IRO Type", "_Description", "_Time Horizon"]
        + ([material_topic_col] if material_topic_col else [])
    ].copy()
    grid_iro_df = grid_iro_df.rename(
        columns={"_IRO Type": "IRO Type", "_Description": "Description", "_Time Horizon": "Time Horizon"}
    )

    iro_gb = GridOptionsBuilder.from_dataframe(grid_iro_df)
    iro_gb.configure_default_column(
        editable=False,
        sortable=True,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
        floatingFilter=True,
        resizable=True,
    )
    iro_gb.configure_column("_row_id", hide=True)
    iro_gb.configure_column(
        industry_col,
        editable=st.session_state.iro_edit_mode,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
    )
    iro_gb.configure_column(
        "IRO Type",
        editable=st.session_state.iro_edit_mode,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
    )
    iro_gb.configure_column(
        "Time Horizon",
        editable=st.session_state.iro_edit_mode,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
    )
    iro_gb.configure_column(
        "Description",
        editable=st.session_state.iro_edit_mode,
        filter="agSetColumnFilter",
        filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
        menuTabs=["filterMenuTab"],
    )
    iro_gb.configure_grid_options(
        stopEditingWhenCellsLoseFocus=True,
        suppressMenuHide=False,
        alwaysShowHorizontalScroll=True,
        alwaysShowVerticalScroll=True,
        rowSelection="single",
        suppressRowClickSelection=False,
    )
    iro_gb.configure_selection("single", use_checkbox=False)
    iro_gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)

    iro_grid_safe_df = _make_arrow_safe_df(grid_iro_df)

    iro_grid_response = AgGrid(
        iro_grid_safe_df,
        gridOptions=iro_gb.build(),
        update_on=["cellValueChanged", "filterChanged", "sortChanged", "selectionChanged"],
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=False,
        height=500,
        enable_enterprise_modules=True,
        key="iro_preview_grid",
    )

    edited_iro_df = pd.DataFrame(iro_grid_response["data"])
    selected_iro_rows = iro_grid_response.get("selected_rows", [])
    if "iro_details_popup_dismissed_for" not in st.session_state:
        st.session_state["iro_details_popup_dismissed_for"] = None
    if isinstance(selected_iro_rows, pd.DataFrame) and not selected_iro_rows.empty:
        candidate_row = selected_iro_rows.iloc[0].to_dict()
        candidate_code = str(candidate_row.get("_row_id", ""))
        if st.session_state.get("iro_details_popup_dismissed_for") != candidate_code:
            st.session_state["selected_iro_preview_row"] = candidate_row
    elif isinstance(selected_iro_rows, list) and len(selected_iro_rows) > 0:
        candidate_row = selected_iro_rows[0]
        candidate_code = str(candidate_row.get("_row_id", ""))
        if st.session_state.get("iro_details_popup_dismissed_for") != candidate_code:
            st.session_state["selected_iro_preview_row"] = candidate_row

    @st.dialog("IRO Row Details")
    def show_iro_row_details_dialog():
        row_data = st.session_state.get("selected_iro_preview_row", {})
        if row_data:
            for key, value in row_data.items():
                st.markdown(f"**{key}:** {value if pd.notna(value) else '—'}")
        if st.button("Close", key="close_iro_row_details_dialog"):
            st.session_state["iro_details_popup_dismissed_for"] = str(row_data.get("_row_id", ""))
            st.session_state["selected_iro_preview_row"] = None
            st.rerun()

    if st.session_state.get("selected_iro_preview_row"):
        show_iro_row_details_dialog()

    _, iro_actions_right = st.columns([6, 2], gap="small")
    with iro_actions_right:
        iro_action_col_left, iro_action_col_right = st.columns([1, 1], gap="small")
        with iro_action_col_left:
            iro_save_clicked = st.button(
                "💾 SAVE ALL CHANGES",
                type="primary",
                key="iro_save_all_changes",
                use_container_width=True,
                disabled=not st.session_state.iro_edit_mode,
            )
        with iro_action_col_right:
            latest_iro_df, latest_iro_sheet = read_latest_iro_data()
            st.download_button(
                "⬇️ Download Updated Excel",
                data=_get_excel_bytes(latest_iro_df, latest_iro_sheet),
                file_name=IRO_FILE.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="iro_download_excel",
                use_container_width=True,
            )

    if iro_save_clicked:
        try:
            latest_df, latest_sheet = read_latest_iro_data()
            for _, row in edited_iro_df.iterrows():
                row_id = row.get("_row_id")
                if pd.notna(row_id) and int(row_id) in latest_df.index:
                    row_index = int(row_id)
                    new_industry = row.get(industry_col, latest_df.at[row_index, industry_col])
                    new_type = row.get("IRO Type", "")
                    new_description = row.get("Description", "")
                    new_horizon = row.get("Time Horizon", "")

                    latest_df.at[row_index, industry_col] = new_industry

                    if impact_col:
                        latest_df.at[row_index, impact_col] = pd.NA
                    if impact_horizon_col:
                        latest_df.at[row_index, impact_horizon_col] = pd.NA
                    if risk_col:
                        latest_df.at[row_index, risk_col] = pd.NA
                    if risk_horizon_col:
                        latest_df.at[row_index, risk_horizon_col] = pd.NA
                    if opportunity_col:
                        latest_df.at[row_index, opportunity_col] = pd.NA
                    if opportunity_horizon_col:
                        latest_df.at[row_index, opportunity_horizon_col] = pd.NA

                    if new_type == "Impact":
                        latest_df.at[row_index, impact_col] = new_description
                        latest_df.at[row_index, impact_horizon_col] = new_horizon
                    elif new_type == "Risk":
                        latest_df.at[row_index, risk_col] = new_description
                        latest_df.at[row_index, risk_horizon_col] = new_horizon
                    elif new_type == "Opportunity":
                        latest_df.at[row_index, opportunity_col] = new_description
                        latest_df.at[row_index, opportunity_horizon_col] = new_horizon

            save_iro_data(latest_df, latest_sheet)
            st.success("IRO database updated!")
            st.cache_data.clear()
            st.rerun()
        except PermissionError:
            st.error("Close the Excel file!")


st.sidebar.title("Modules")
selected_module = st.sidebar.radio(
    "Go to",
    options=[
        "KPI Repository",
        "DMA IRO Database",
        "DMA Questionnaire",
        "Value Chain Questionaire",
    ],
    label_visibility="collapsed",
    key="selected_module",
)

if selected_module == "DMA IRO Database":
    render_dma_iro_module()
    st.stop()

if selected_module == "DMA Questionnaire":
    render_dma_questionnaire_module()
    st.stop()

if selected_module != "KPI Repository":
    st.title(selected_module)
    st.info(f"{selected_module} module placeholder. Define the workflow and I will implement it next.")
    st.stop()


st.title("ESG Nexus Master Console")
_show_flash_message()

if "kpi_flow_target" not in st.session_state:
    st.session_state.kpi_flow_target = None

st.markdown(
    """
    <style>
    div[data-testid="stPopover"] > div > button {
        background-color: #e8f1ff !important;
        color: #123a75 !important;
        border: 1px solid #123a75 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.popover("⚙️ ADD / REMOVE KPI", use_container_width=False):
    c1, c2 = st.columns(2)
    if c1.button("➕ Add KPI", use_container_width=True, key="kpi_manage_add"):
        st.session_state["selected_kpi_preview_row"] = None
        st.session_state.kpi_flow_target = "add"
        st.rerun()
    if c2.button("🗑️ Remove KPI", use_container_width=True, key="kpi_manage_remove"):
        st.session_state["selected_kpi_preview_row"] = None
        st.session_state.kpi_flow_target = "remove"
        st.rerun()


@st.dialog("Add KPI", width="large")
def add_kpi_dialog():
    if st.button("Close", key="close_add_kpi_dialog"):
        st.session_state.kpi_flow_target = None
        st.rerun()

    entry_mode = st.selectbox(
        "KPI Type",
        options=["Narrative KPI", "Tabular — Static", "Tabular — Dynamic"],
        index=0,
        key="add_kpi_entry_mode",
    )

    st.subheader("Business Language Entry")

    if entry_mode == "Narrative KPI":
        c1, c2 = st.columns(2)
        with c1:
            in_title = st.text_input("Master KPI Name")
            in_pillar = st.selectbox("Pillar", options=list(p_map.values()))
            in_topic = st.selectbox("Topic", options=list(t_name_to_id.keys()))
        with c2:
            in_type = st.selectbox("Type", options=["TextBlock (Narrative)"])
            in_agg = st.selectbox("Aggregation", options=list(agg_map.values()))
            selected_frameworks = st.multiselect(
                "Map to Framework(s)",
                options=list(fw_map.values()),
                default=[list(fw_map.values())[0]],
                key="add_kpi_frameworks_standard",
            )

        framework_detail_inputs = {}
        for fw_name in selected_frameworks:
            st.markdown(f"**{fw_name} mapping**")
            fw_desc = st.text_area(
                f"{fw_name} Description",
                key=f"fw_desc_standard_{fw_name}",
            )
            fw_ref_code = st.text_input(
                f"{fw_name} Reference Code (optional)",
                key=f"fw_ref_standard_{fw_name}",
            )
            framework_detail_inputs[fw_name] = {"Description": fw_desc, "ReferenceCode": fw_ref_code}
        in_desc = (
            framework_detail_inputs[selected_frameworks[0]]["Description"]
            if selected_frameworks
            else ""
        )

        is_table_title = False
        is_dynamic_table = False
        table_name = ""
        row_count = 0
        col_count = 0

    else:
        is_table_title = True
        is_dynamic_table = entry_mode == "Tabular — Dynamic"

        c1, c2, c3 = st.columns(3)
        with c1:
            in_pillar = st.selectbox("Pillar", options=list(p_map.values()))
        with c2:
            in_topic = st.selectbox("Topic", options=list(t_name_to_id.keys()))
        with c3:
            selected_frameworks = st.multiselect(
                "Map to Framework(s)",
                options=list(fw_map.values()),
                default=[list(fw_map.values())[0]],
                key="add_kpi_frameworks_tabular",
            )

        in_title = st.text_input("Table Title")
        table_name = in_title
        framework_detail_inputs = {}
        for fw_name in selected_frameworks:
            st.markdown(f"**{fw_name} mapping**")
            fw_desc = st.text_area(
                f"{fw_name} Description",
                key=f"fw_desc_tabular_{fw_name}",
            )
            fw_ref_code = st.text_input(
                f"{fw_name} Reference Code (optional)",
                key=f"fw_ref_tabular_{fw_name}",
            )
            framework_detail_inputs[fw_name] = {"Description": fw_desc, "ReferenceCode": fw_ref_code}
        in_desc = (
            framework_detail_inputs[selected_frameworks[0]]["Description"]
            if selected_frameworks
            else ""
        )

        in_type = "Table (Title)"
        in_agg = "NONE"

        st.markdown(f"### 🧩 Table Configuration — {'Dynamic' if is_dynamic_table else 'Static'}")
        if is_dynamic_table:
            row_count = 1
            col_count = int(
                st.number_input(
                    "Number of Columns *",
                    min_value=1,
                    max_value=25,
                    value=int(st.session_state.table_col_count),
                    step=1,
                    key="table_col_count_input",
                )
            )
            st.session_state.table_col_count = col_count
        else:
            r1, r2 = st.columns(2)
            with r1:
                row_count = int(st.number_input("Number of Rows *", min_value=1, max_value=25, value=2, step=1))
            with r2:
                col_count = int(
                    st.number_input(
                        "Number of Columns *",
                        min_value=1,
                        max_value=25,
                        value=int(st.session_state.table_col_count),
                        step=1,
                        key="table_col_count_input",
                    )
                )
                st.session_state.table_col_count = col_count

        # keep cell config only for current size
        st.session_state.tabular_cell_data = {
            key: val
            for key, val in st.session_state.tabular_cell_data.items()
            if key[0] < row_count and key[1] < col_count
        }
        if not is_dynamic_table:
            st.session_state.tabular_column_headers = {
                key: val for key, val in st.session_state.tabular_column_headers.items() if key < col_count
            }
            st.session_state.tabular_row_headers = {
                key: val for key, val in st.session_state.tabular_row_headers.items() if key < row_count
            }
            for j in range(col_count):
                st.session_state.tabular_column_headers.setdefault(j, f"C{j}")
            for i in range(row_count):
                st.session_state.tabular_row_headers.setdefault(i, f"Row {i + 1}")
            preview_cell_codes = build_static_cell_code_preview(in_pillar, row_count, col_count)
        else:
            preview_cell_codes = {}

        st.markdown("#### 📋 Table Preview (Click a cell to configure)")
        header_cols = st.columns(col_count + 1)
        header_cols[0].markdown("**Row Header**" if not is_dynamic_table else "**#**")
        for j in range(col_count):
            if is_dynamic_table:
                header_cols[j + 1].markdown(f"**C{j}**")
            else:
                header_cols[j + 1].text_input(
                    f"Column Header {j}",
                    key=f"col_header_{j}",
                    value=st.session_state.tabular_column_headers.get(j, f"C{j}"),
                )
                st.session_state.tabular_column_headers[j] = st.session_state.get(f"col_header_{j}", f"C{j}")

        for i in range(row_count):
            cols = st.columns(col_count + 1)
            if is_dynamic_table:
                cols[0].markdown(f"**{i}**")
            else:
                cols[0].text_input(
                    f"Row Header {i}",
                    key=f"row_header_{i}",
                    value=st.session_state.tabular_row_headers.get(i, f"Row {i + 1}"),
                )
                st.session_state.tabular_row_headers[i] = st.session_state.get(f"row_header_{i}", f"Row {i + 1}")
            for j in range(col_count):
                cur = st.session_state.tabular_cell_data.get((i, j), {})
                btn_text = cur.get("title", f"R{i},C{j}")
                if is_dynamic_table:
                    if cols[j + 1].button(btn_text, key=f"cell_btn_{i}_{j}"):
                        st.session_state.editing_cell = (i, j)
                else:
                    code_col, button_col = cols[j + 1].columns([1.3, 3])
                    code_col.caption(preview_cell_codes.get((i, j), "—"))
                    if button_col.button(btn_text, key=f"cell_btn_{i}_{j}"):
                        st.session_state.editing_cell = (i, j)

        if st.session_state.editing_cell is not None:
            i, j = st.session_state.editing_cell
            existing = st.session_state.tabular_cell_data.get((i, j), {})

            st.markdown(f"### Edit Cell Details — Row {i}, Column {j}")
            ctype = st.selectbox(
                "KPI Type",
                options=["Numeric (Table)", "TextArea (Narrative in Table)"],
                index=0 if existing.get("kpi_type", "Numeric (Table)") == "Numeric (Table)" else 1,
                key=f"dlg_type_{i}_{j}",
            )
            ctitle = st.text_input(
                "Title",
                value=existing.get("title", f"Cell R{i}C{j}"),
                key=f"dlg_title_{i}_{j}",
            )
            cdesc = st.text_area(
                "Description",
                value=existing.get("desc", in_desc),
                key=f"dlg_desc_{i}_{j}",
            )
            cagg = st.selectbox(
                "Aggregation",
                options=list(agg_map.values()),
                index=list(agg_map.values()).index(existing.get("aggregation", "NONE"))
                if existing.get("aggregation", "NONE") in list(agg_map.values())
                else 0,
                key=f"dlg_agg_{i}_{j}",
            )
            cformula = st.text_input(
                "Cell Formula",
                value=existing.get("formula", ""),
                key=f"dlg_formula_{i}_{j}",
            )
            cagg_formula = st.text_input(
                "Aggregation Formula",
                value=existing.get("aggregation_formula", ""),
                key=f"dlg_agg_formula_{i}_{j}",
            )
            if not is_dynamic_table:
                st.caption(f"Iris KPI Code: {preview_cell_codes.get((i, j), '—')}")

            save_col, cancel_col = st.columns(2)
            if save_col.button("Save Cell", key=f"dlg_save_{i}_{j}"):
                st.session_state.tabular_cell_data[(i, j)] = {
                    "kpi_type": ctype,
                    "title": ctitle,
                    "desc": cdesc,
                    "aggregation": cagg,
                    "formula": cformula,
                    "aggregation_formula": cagg_formula,
                }
                st.session_state.editing_cell = None
                st.rerun()
            if cancel_col.button("Cancel Cell Edit", key=f"dlg_cancel_{i}_{j}"):
                st.session_state.editing_cell = None
                st.rerun()

    if st.button("✅ Create & Save KPI", type="primary"):
        inv_p = {v: k for k, v in p_map.items()}
        inv_a = {v: k for k, v in agg_map.items()}
        inv_t = {v: k for k, v in type_map.items()}
        inv_fw = {v: k for k, v in fw_map.items()}

        try:
            latest_master = read_latest_master()
            if not selected_frameworks:
                st.error("Please select at least one framework mapping.")
                return
            schema_cols = list(latest_master.columns)
            group_code = _next_group_code(latest_master) if is_table_title else None
            parent_id = get_next_iris_code(latest_master, in_pillar)
            rows_to_add = []

            parent_payload = [
                {
                    "Standard": inv_fw[fw_name],
                    "Description": framework_detail_inputs.get(fw_name, {}).get("Description", ""),
                    "ReferenceCode": framework_detail_inputs.get(fw_name, {}).get("ReferenceCode", ""),
                }
                for fw_name in selected_frameworks
            ]
            parent_row = _new_row_template(schema_cols)
            _set_if_present(parent_row, schema_cols, group_code if is_table_title else parent_id, "IrisKPICode")
            _set_if_present(parent_row, schema_cols, inv_p[in_pillar], "Category")
            _set_if_present(parent_row, schema_cols, t_name_to_id[in_topic], "TopicId")
            _set_if_present(parent_row, schema_cols, inv_t[in_type], "Type")
            _set_if_present(parent_row, schema_cols, in_title, "Title")
            _set_if_present(parent_row, schema_cols, table_name, "TableName")
            _set_if_present(parent_row, schema_cols, inv_a["NONE"] if is_table_title else inv_a[in_agg], "AggregationType", "Aggregation")
            _set_if_present(parent_row, schema_cols, json.dumps(parent_payload), "KPIDetail")
            _set_if_present(parent_row, schema_cols, 0, "RowIndex")
            _set_if_present(parent_row, schema_cols, 0, "ColIndex", "ColumnIndex")
            _set_if_present(parent_row, schema_cols, is_dynamic_table if is_table_title else False, "IsDynamic")
            rows_to_add.append(parent_row)

            if is_table_title:
                temp_df = pd.concat([latest_master, pd.DataFrame(rows_to_add)], ignore_index=True)
                for i in range(row_count):
                    for j in range(col_count):
                        cfg = st.session_state.tabular_cell_data.get((i, j), {})
                        child_id = get_next_iris_code(temp_df, in_pillar)
                        cell_payload = [
                            {
                                "Standard": inv_fw[fw_name],
                                "Description": cfg.get("desc", in_desc),
                                "ReferenceCode": framework_detail_inputs.get(fw_name, {}).get("ReferenceCode", ""),
                            }
                            for fw_name in selected_frameworks
                        ]
                        cell_row = _new_row_template(schema_cols)
                        _set_if_present(cell_row, schema_cols, child_id, "IrisKPICode")
                        _set_if_present(cell_row, schema_cols, inv_p[in_pillar], "Category")
                        _set_if_present(cell_row, schema_cols, t_name_to_id[in_topic], "TopicId")
                        _set_if_present(cell_row, schema_cols, inv_t[cfg.get("kpi_type", "Numeric (Table)")], "Type")
                        _set_if_present(cell_row, schema_cols, cfg.get("title", f"Cell R{i}C{j}"), "Title")
                        _set_if_present(
                            cell_row,
                            schema_cols,
                            inv_a[cfg.get("aggregation", "NONE")],
                            "AggregationType",
                            "Aggregation",
                        )
                        if not is_dynamic_table:
                            _set_if_present(cell_row, schema_cols, json.dumps(cell_payload), "KPIDetail")
                        cell_formula_value = cfg.get("formula", "").strip()
                        aggregation_formula_value = cfg.get("aggregation_formula", "").strip()
                        _set_if_present(
                            cell_row,
                            schema_cols,
                            cell_formula_value if cell_formula_value else pd.NA,
                            "CellFormula",
                        )
                        _set_if_present(
                            cell_row,
                            schema_cols,
                            aggregation_formula_value if aggregation_formula_value else pd.NA,
                            "AggregationFormula",
                        )
                        _set_if_present(cell_row, schema_cols, i, "RowIndex")
                        _set_if_present(cell_row, schema_cols, j, "ColIndex", "ColumnIndex")
                        _set_if_present(cell_row, schema_cols, is_dynamic_table, "IsDynamic")
                        _set_if_present(cell_row, schema_cols, group_code, "ParentCode")
                        if not is_dynamic_table:
                            _set_if_present(
                                cell_row,
                                schema_cols,
                                st.session_state.tabular_row_headers.get(i, f"Row {i + 1}"),
                                "RowHeader",
                            )
                            _set_if_present(
                                cell_row,
                                schema_cols,
                                st.session_state.tabular_column_headers.get(j, f"C{j}"),
                                "ColumnHeader",
                            )
                        else:
                            _set_if_present(
                                cell_row,
                                schema_cols,
                                cfg.get("title", f"Cell R{i}C{j}"),
                                "ColumnHeader",
                            )
                        rows_to_add.append(cell_row)
                        temp_df = pd.concat([temp_df, pd.DataFrame([cell_row])], ignore_index=True)

            save_master(pd.concat([latest_master, pd.DataFrame(rows_to_add)], ignore_index=True))
            st.cache_data.clear()
            _set_flash_message("KPI is being added. Updated data has been saved.")
            _reset_after_create("KPI Repository")
            st.session_state.kpi_flow_target = None
            st.rerun()
        except PermissionError:
            st.error("Close the Excel file!")

# existing remove/edit/view logic unchanged
@st.dialog("Remove KPI")
def remove_kpi_dialog():
    if st.button("Close", key="close_remove_kpi_dialog"):
        st.session_state.kpi_flow_target = None
        st.rerun()

    latest_master_for_delete = read_latest_master()
    kpi_options = sorted(latest_master_for_delete["IrisKPICode"].dropna().astype(str).unique().tolist())

    remove_mode = st.radio(
        "Remove by",
        options=["Iris KPI Code", "KPI Title"],
        horizontal=True,
        key="remove_kpi_mode",
    )

    selected_kpi_code = None
    if remove_mode == "Iris KPI Code":
        selected_kpi_code = st.selectbox("Select Iris KPI Code", options=[""] + kpi_options, key="remove_kpi_code")
    else:
        title_df = latest_master_for_delete[["IrisKPICode", "Title"]].dropna(subset=["IrisKPICode"]).copy()
        title_df["Title"] = title_df["Title"].fillna("").astype(str)
        title_df["Display"] = title_df["IrisKPICode"].astype(str) + " | " + title_df["Title"]
        display_options = [""] + sorted(title_df["Display"].tolist())
        selected_display = st.selectbox("Select KPI", options=display_options, key="remove_kpi_title")
        if selected_display:
            selected_kpi_code = selected_display.split(" | ", 1)[0]

    st.warning("This action permanently removes KPI row(s) from the master CSV file.")

    if st.button("🗑️ Remove KPI", type="secondary"):
        if not selected_kpi_code:
            st.error("Please select a KPI to remove.")
        else:
            latest_master = read_latest_master()
            target_rows = latest_master[latest_master["IrisKPICode"].astype(str) == str(selected_kpi_code)]
            if target_rows.empty:
                st.error("Selected KPI code was not found in the latest master data.")
            else:
                target_title = str(target_rows.iloc[0].get("Title", "") or "")
                st.session_state["pending_kpi_delete"] = {
                    "code": str(selected_kpi_code),
                    "title": target_title,
                }

    pending_delete = st.session_state.get("pending_kpi_delete")
    if pending_delete:
        st.warning("This action will permanently delete the selected KPI.")
        st.markdown(f"**Iris KPI Code:** {pending_delete['code']}")
        st.markdown(f"**KPI Title:** {pending_delete['title']}")
        c1, c2 = st.columns(2)
        if c1.button("Confirm Delete", type="primary", key="confirm_kpi_delete_button"):
            try:
                removed_count = remove_kpi_records(pending_delete["code"])
                st.session_state["pending_kpi_delete"] = None
                st.session_state.kpi_flow_target = None
                st.success(f"Removed {removed_count} row(s) for KPI selection: {pending_delete['code']}.")
                st.cache_data.clear()
                st.rerun()
            except PermissionError:
                st.error("❌ Permission Denied: Close the Excel file!")
            except LookupError as exc:
                st.session_state["pending_kpi_delete"] = None
                st.error(str(exc))
        if c2.button("Cancel", key="cancel_kpi_delete_button"):
            st.session_state["pending_kpi_delete"] = None
            st.session_state.kpi_flow_target = None
            st.rerun()


if st.session_state.kpi_flow_target == "add":
    add_kpi_dialog()
elif st.session_state.kpi_flow_target == "remove":
    remove_kpi_dialog()

if "kpi_edit_mode" not in st.session_state:
    st.session_state.kpi_edit_mode = False

framework_options = list(fw_map.values())
kpi_edit_col, kpi_right_controls_col = st.columns([1.2, 2.8])
with kpi_edit_col:
    st.write("")
    if not st.session_state.kpi_edit_mode:
        if st.button("✏️ Edit Existing KPI", key="kpi_enable_edit_mode"):
            st.session_state.kpi_edit_mode = True
            st.rerun()
    else:
        if st.button("🔒 Lock Editing", key="kpi_disable_edit_mode"):
            st.session_state.kpi_edit_mode = False
            st.rerun()
with kpi_right_controls_col:
    kpi_framework_col, kpi_search_col = st.columns([2, 2])
    with kpi_framework_col:
        selected_framework_values = st.multiselect(
            "Framework",
            options=framework_options,
            default=st.session_state.get("selected_framework_filter", []),
            max_selections=1,
            placeholder="All Frameworks",
            key="selected_framework_filter",
        )
    with kpi_search_col:
        search = st.text_input("🔍 Search", placeholder="Search names or IDs...")

biz_df = df_master.copy()
biz_df["Pillar"] = biz_df["Category"].map(p_map)
biz_df["Topic"] = biz_df["TopicId"].map(t_id_to_name)
biz_df["Type"] = biz_df["Type"].map(type_map)
biz_df["Aggregation"] = biz_df["AggregationType"].map(agg_map)

for sid, sname in fw_map.items():
    biz_df[f"{sname} Language"] = biz_df["KPIDetail"].apply(lambda x: parse_fw(x, sid))
    biz_df[f"{sname} ReferenceCode"] = biz_df["KPIDetail"].apply(lambda x: parse_fw_reference_code(x, sid))

if search:
    biz_df = biz_df[biz_df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

biz_df["Title (Master KPI)"] = biz_df["Title"]

m_cols = ["IrisKPICode", "Pillar", "Topic", "Type", "Title (Master KPI)", "Aggregation"]
if selected_framework_values:
    selected_framework = selected_framework_values[0]
    selected_fw_col = f"{selected_framework} Language"
    selected_fw_ref_col = f"{selected_framework} ReferenceCode"
    biz_df = biz_df[
        biz_df[selected_fw_col].astype(str).str.strip().isin(["", "—", "nan", "None"]) == False
    ]
    final_cols = m_cols + [selected_fw_col, selected_fw_ref_col]
else:
    final_cols = m_cols

grid_df = biz_df[final_cols].copy()

gb = GridOptionsBuilder.from_dataframe(grid_df)
is_kpi_edit_mode = st.session_state.kpi_edit_mode
gb.configure_default_column(
    editable=False,
    sortable=True,
    filter="agSetColumnFilter",
    filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
    menuTabs=["filterMenuTab"],
    floatingFilter=True,
    resizable=True,
)
gb.configure_column(
    "Pillar",
    editable=is_kpi_edit_mode,
    filter="agSetColumnFilter",
    filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
    menuTabs=["filterMenuTab"],
)
gb.configure_column(
    "Type",
    editable=False,
    filter="agSetColumnFilter",
    filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
    menuTabs=["filterMenuTab"],
)
gb.configure_column(
    "Topic",
    editable=is_kpi_edit_mode,
    filter="agSetColumnFilter",
    filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
    menuTabs=["filterMenuTab"],
)
gb.configure_column(
    "Title (Master KPI)",
    editable=is_kpi_edit_mode,
    filter="agSetColumnFilter",
    filterParams={"buttons": ["reset", "apply"], "excelMode": "windows"},
    menuTabs=["filterMenuTab"],
)
gb.configure_grid_options(
    stopEditingWhenCellsLoseFocus=True,
    suppressMenuHide=False,
    alwaysShowHorizontalScroll=True,
    alwaysShowVerticalScroll=True,
    rowSelection="single",
    suppressRowClickSelection=False,
)
gb.configure_selection("single", use_checkbox=False)
gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
grid_options = gb.build()

grid_safe_df = _make_arrow_safe_df(grid_df)

grid_response = AgGrid(
    grid_safe_df,
    gridOptions=grid_options,
    update_on=["cellValueChanged", "filterChanged", "sortChanged", "selectionChanged"],
    fit_columns_on_grid_load=False,
    allow_unsafe_jscode=False,
    height=500,
    enable_enterprise_modules=True,
    key="kpi_preview_grid",
)

edited_df = pd.DataFrame(grid_response["data"])
selected_kpi_rows = grid_response.get("selected_rows", [])
if "kpi_details_popup_dismissed_for" not in st.session_state:
    st.session_state["kpi_details_popup_dismissed_for"] = None
if isinstance(selected_kpi_rows, pd.DataFrame):
    if not selected_kpi_rows.empty:
        candidate_row = selected_kpi_rows.iloc[0].to_dict()
        candidate_code = str(candidate_row.get("IrisKPICode", ""))
        if st.session_state.get("kpi_details_popup_dismissed_for") != candidate_code:
            st.session_state["selected_kpi_preview_row"] = candidate_row
elif isinstance(selected_kpi_rows, list):
    if len(selected_kpi_rows) > 0:
        candidate_row = selected_kpi_rows[0]
        candidate_code = str(candidate_row.get("IrisKPICode", ""))
        if st.session_state.get("kpi_details_popup_dismissed_for") != candidate_code:
            st.session_state["selected_kpi_preview_row"] = candidate_row


@st.dialog("KPI Row Details")
def show_kpi_row_details_dialog():
    row_data = st.session_state.get("selected_kpi_preview_row", {})
    if not row_data:
        st.info("No KPI row selected.")
    else:
        for key, value in row_data.items():
            st.markdown(f"**{key}:** {value if pd.notna(value) else '—'}")
    if st.button("Close", key="close_kpi_row_details_dialog"):
        st.session_state["kpi_details_popup_dismissed_for"] = str(row_data.get("IrisKPICode", ""))
        st.session_state["selected_kpi_preview_row"] = None
        st.rerun()


if st.session_state.get("selected_kpi_preview_row") and st.session_state.get("kpi_flow_target") is None:
    show_kpi_row_details_dialog()

_, kpi_actions_right = st.columns([6, 2], gap="small")
with kpi_actions_right:
    kpi_action_col_left, kpi_action_col_right = st.columns([1, 1], gap="small")
    with kpi_action_col_left:
        kpi_save_clicked = st.button(
            "💾 SAVE ALL CHANGES",
            type="primary",
            use_container_width=True,
            disabled=not st.session_state.kpi_edit_mode,
        )
    with kpi_action_col_right:
        st.download_button(
            "⬇️ Download Updated CSV",
            data=read_latest_master().to_csv(index=False).encode("utf-8"),
            file_name=KPI_FILE.name,
            mime="text/csv",
            key="kpi_download_csv",
            use_container_width=True,
        )

if kpi_save_clicked:
    inv_p = {v: k for k, v in p_map.items()}
    inv_topic = {v: k for k, v in t_id_to_name.items()}

    try:
        latest_master = read_latest_master()
        changed_rows = 0

        for _, row in edited_df.iterrows():
            idx = latest_master[latest_master["IrisKPICode"] == row["IrisKPICode"]].index
            if not idx.empty:
                row_index = idx[0]
                new_title = row["Title (Master KPI)"]
                new_category = inv_p.get(row["Pillar"], latest_master.loc[row_index, "Category"])
                new_topic = inv_topic.get(row["Topic"], latest_master.loc[row_index, "TopicId"])

                has_change = (
                    str(latest_master.loc[row_index, "Title"]) != str(new_title)
                    or latest_master.loc[row_index, "Category"] != new_category
                    or str(latest_master.loc[row_index, "TopicId"]) != str(new_topic)
                )
                if has_change:
                    changed_rows += 1
                    latest_master.loc[row_index, "Title"] = new_title
                    latest_master.loc[row_index, "Category"] = new_category
                    latest_master.loc[row_index, "TopicId"] = new_topic

        if changed_rows == 0:
            st.warning("No KPI edits detected to save.")
        else:
            save_master(latest_master)
            st.success(f"Master database updated! Saved changes in {changed_rows} row(s).")
            st.cache_data.clear()
            st.rerun()
    except PermissionError:
        st.error("Close the CSV file in Excel!")
