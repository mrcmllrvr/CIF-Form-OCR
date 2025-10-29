# app.py - CIF Form Data Intelligence Engine
# Extracts customer information from DBP CIF Individual forms

import streamlit as st
import os
import io
import json
import pandas as pd
import traceback
import time
import concurrent.futures
import tempfile
import hashlib
from datetime import datetime

st.set_page_config(page_title="CIF Form Data Intelligence Engine", layout="wide", initial_sidebar_state="expanded")
st.title("📋 CIF Form Data Intelligence Engine")

# ---------- Sidebar aesthetic tweaks + Bold field labels ----------
st.markdown("""
<style>
/* ===== Missing CSS Classes ===== */
.sb-label {
  color: #374151;
  font-weight: 600;
  font-size: 0.9rem;
  margin: 0.1rem 0;
}

.sb-help {
  color: #9CA3AF;
  font-size: 0.75rem;
  margin: 0.2rem 0 0.4rem;
  text-align: center;
  padding: 0 0.5rem;
  line-height: 1.3;
}

.sb-group {
  margin: 0.25rem 0;
}

/* ===== BOLD FIELD LABELS ===== */
div[data-testid="stTextInput"] label,
div[data-testid="stTextArea"] label {
    font-weight: 800 !important;
}

/* ===== TEXT INPUT COLOR ===== */
div[data-testid="stTextInput"] input {
    color: #2563EB !important;
}

div[data-testid="stTextArea"] textarea {
    color: #2563EB !important;
}

/* ===== SIDEBAR BUTTONS ===== */
[data-testid="stSidebar"] .stButton,
[data-testid="stSidebar"] .stDownloadButton {
  width: 100% !important;
  margin: .25rem 0 !important;
}

[data-testid="stSidebar"] .stButton > button,
[data-testid="stSidebar"] .stDownloadButton > button,
[data-testid="stSidebar"] .stButton button[kind="primary"],
[data-testid="stSidebar"] .stButton button[kind="secondary"],
[data-testid="stSidebar"] .stDownloadButton button[kind="primary"],
[data-testid="stSidebar"] .stDownloadButton button[kind="secondary"],
[data-testid="stSidebar"] .stButton button,
[data-testid="stSidebar"] .stDownloadButton button {
  display: block !important;
  width: 100% !important;
  min-width: 100% !important;
  max-width: 100% !important;
  box-sizing: border-box !important;
  height: 2.8rem !important;
  padding: 0 1rem !important;
  border-radius: 12px !important;
  margin: 0 !important;
  background: #FFFFFF !important;
  border: 1px solid #E5E7EB !important;
  color: #1F2937 !important;
  box-shadow: 0 1px 3px rgba(0,0,0,0.1) !important;
  transition: all 0.2s ease !important;
  font-weight: 500 !important;
  font-size: 0.95rem !important;
  text-align: center !important;
  white-space: nowrap !important;
  overflow: hidden !important;
  text-overflow: ellipsis !important;
}

[data-testid="stSidebar"] .stButton > button:hover,
[data-testid="stSidebar"] .stDownloadButton > button:hover,
[data-testid="stSidebar"] .stButton button:hover,
[data-testid="stSidebar"] .stDownloadButton button:hover {
  background: #BF4342 !important;
  border-color: #BF4342 !important;
  color: #FFFFFF !important;
  transform: translateY(-1px) !important;
  box-shadow: 0 4px 8px rgba(191, 67, 66, 0.3) !important;
}

[data-testid="stSidebar"] .stButton > button:active,
[data-testid="stSidebar"] .stDownloadButton > button:active,
[data-testid="stSidebar"] .stButton button:active,
[data-testid="stSidebar"] .stDownloadButton button:active {
  background: #BF4342 !important;
  border-color: #BF4342 !important;
  color: #FFFFFF !important;
  transform: translateY(0px) !important;
  box-shadow: 0 2px 4px rgba(191, 67, 66, 0.4) !important;
}

/* Primary Download Button in Sidebar */
[data-testid="stSidebar"] .stDownloadButton:first-of-type > button {
  background: #f54f4f !important;
  border-color: #f54f4f !important;
  color: #FFFFFF !important;
}

[data-testid="stSidebar"] .stDownloadButton:first-of-type > button:hover {
  background: #BF4342 !important;
  border-color: #BF4342 !important;
  color: #FFFFFF !important;
}

/* ===== MAIN CONTENT BUTTONS ===== */
.stButton > button[kind="primary"],
.stDownloadButton > button,
button[data-testid*="update_record"],
button[data-testid*="download_excel"],
button[data-testid*="dl_cleaned"],
button[data-testid*="dl_raw"] {
  display: inline-block !important;
  height: 2.5rem !important;
  padding: 0 1.5rem !important;
  border-radius: 8px !important;
  background: #f54f4f !important;
  border: 1px solid #f54f4f !important;
  color: #FFFFFF !important;
  box-shadow: 0 2px 4px rgba(37, 99, 235, 0.2) !important;
  transition: all 0.2s ease !important;
  font-weight: 500 !important;
  font-size: 0.9rem !important;
  text-align: center !important;
  cursor: pointer !important;
}

.stButton > button[kind="primary"]:hover,
.stDownloadButton > button:hover,
button[data-testid*="update_record"]:hover,
button[data-testid*="download_excel"]:hover,
button[data-testid*="dl_cleaned"]:hover,
button[data-testid*="dl_raw"]:hover {
  background: #BF4342 !important;
  border-color: #BF4342 !important;
  transform: translateY(-1px) !important;
  box-shadow: 0 4px 8px rgba(29, 78, 216, 0.3) !important;
}

/* Secondary buttons */
.stButton > button[kind="secondary"],
button[data-testid*="reprocess"],
button[data-testid*="clear"] {
  display: inline-block !important;
  height: 2.5rem !important;
  padding: 0 1.5rem !important;
  border-radius: 8px !important;
  background: #FFFFFF !important;
  border: 1px solid #E5E7EB !important;
  color: #1F2937 !important;
  box-shadow: 0 1px 3px rgba(0,0,0,0.1) !important;
  transition: all 0.2s ease !important;
  font-weight: 500 !important;
  font-size: 0.9rem !important;
  text-align: center !important;
  cursor: pointer !important;
}

.stButton > button[kind="secondary"]:hover,
button[data-testid*="reprocess"]:hover,
button[data-testid*="clear"]:hover {
  background: #f54f4f !important;
  border-color: #f54f4f !important;
  color: #FFFFFF !important;
  transform: translateY(-1px) !important;
  box-shadow: 0 4px 8px rgba(37, 99, 235, 0.3) !important;
}

/* Container fixes */
[data-testid="stSidebar"] .element-container,
[data-testid="stSidebar"] .stButton .element-container,
[data-testid="stSidebar"] .stDownloadButton .element-container {
  width: 100% !important;
}

[data-testid="stSidebar"] .stButton,
[data-testid="stSidebar"] .stDownloadButton {
  flex: none !important;
}

[data-testid="stSidebar"] button[data-testid] {
  width: 100% !important;
  min-width: 100% !important;
}
</style>
""", unsafe_allow_html=True)

# ---------- Import processing helpers ----------
MAIN_AVAILABLE = True
_import_error = None
try:
    from main import process_pdf, process_image, flatten_json
except Exception:
    MAIN_AVAILABLE = False
    _import_error = traceback.format_exc()

# ---------- Runtime folders (for derived artifacts only) ----------
OUTPUT_PDF_IMAGES = os.path.join("output", "pdf_images")
OUTPUT_PROCESSED_IMAGES = os.path.join("output", "processed_images")
CLEANED_TEXT_FOLDER = "cleaned_text"

os.makedirs("output", exist_ok=True)
os.makedirs(OUTPUT_PDF_IMAGES, exist_ok=True)
os.makedirs(OUTPUT_PROCESSED_IMAGES, exist_ok=True)
os.makedirs(CLEANED_TEXT_FOLDER, exist_ok=True)

# ---------- Session-scoped state ----------
if "temp_dir" not in st.session_state:
    st.session_state["temp_dir"] = tempfile.mkdtemp(prefix="cif_uploads_")
if "uploads" not in st.session_state:
    st.session_state["uploads"] = []  # list of {"name": str, "path": str, "digest": str}
if "upload_index" not in st.session_state:
    # maps file_digest -> {"name": str, "path": str}
    st.session_state["upload_index"] = {}
if "cache" not in st.session_state:
    st.session_state["cache"] = {}
if "selected_file_path" not in st.session_state:
    st.session_state["selected_file_path"] = None
if "has_new_uploads" not in st.session_state:
    st.session_state["has_new_uploads"] = False

# ---------- Helpers ----------
def _file_sig(path):
    try:
        if not os.path.exists(path):
            return None
        stat = os.stat(path)
        if time.time() - stat.st_mtime < 0.5:
            time.sleep(0.1)
            stat = os.stat(path)
        return (stat.st_size, int(stat.st_mtime * 1000))
    except (FileNotFoundError, OSError):
        return None

# NEW: original uploaded filename for a given temp path
def _original_name_for_path(p: str) -> str:
    for it in st.session_state.get("uploads", []):
        if it.get("path") == p:
            return it.get("name") or os.path.basename(p)
    return os.path.basename(p)

def excel_bytes_for_single_doc(data: dict) -> bytes:
    cols = [
        "Document_Type",
        "Page_Count",
        "Name_of_file",
        "Last_Name",
        "First_Name",
        "Suffix",
        "Middle_Name",
        "Date_of_Birth",
        "Place_of_Birth_Town",
        "Place_of_Birth_Province",
        "raw_text",
        "cleaned_text",
    ]
    row = {c: "" for c in cols}
    for k in row.keys():
        row[k] = data.get(k, "")
    df = pd.DataFrame([row], columns=cols)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="extracted")
    buf.seek(0)
    return buf.read()

def excel_bytes_for_all_docs(cache: dict) -> bytes:
    cols = [
        "Document_Type",
        "Page_Count",
        "Name_of_file",
        "Last_Name",
        "First_Name",
        "Suffix",
        "Middle_Name",
        "Date_of_Birth",
        "Place_of_Birth_Town",
        "Place_of_Birth_Province",
        "raw_text",
        "cleaned_text",
    ]
    rows = []
    # CHANGED: iterate items to get path, and override Name_of_file with original upload name
    for path, entry in cache.items():
        data = (entry or {}).get("result")
        if not data:
            continue
        row = {c: "" for c in cols}
        for k in row.keys():
            row[k] = data.get(k, "")
        row["Name_of_file"] = _original_name_for_path(path)
        rows.append(row)

    df = pd.DataFrame(rows, columns=cols)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="extracted")
    buf.seek(0)
    return buf.read()

def process_permit(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        pdf_folder = os.path.dirname(file_path)
        image_output_folder = OUTPUT_PDF_IMAGES
        os.makedirs(image_output_folder, exist_ok=True)
        return process_pdf(os.path.basename(file_path), pdf_folder, image_output_folder)
    elif ext in [".png", ".jpg", ".jpeg"]:
        image_folder = os.path.dirname(file_path)
        image_output_folder = OUTPUT_PROCESSED_IMAGES
        os.makedirs(image_output_folder, exist_ok=True)
        return process_image(os.path.basename(file_path), image_folder, image_output_folder)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

# ---------- Upload (temp files, non-persistent) ----------
uploaded_files = st.file_uploader(
    "Upload one or more files",
    type=["pdf", "png", "jpg", "jpeg"],
    accept_multiple_files=True
)

newly_uploaded = []
if uploaded_files:
    for up in uploaded_files:
        # compute MD5 fingerprint to dedupe across reruns
        data = up.getvalue()  # bytes
        digest = hashlib.md5(data).hexdigest()
        if digest in st.session_state["upload_index"]:
            # already saved this exact file in this session; skip
            continue
        suffix = os.path.splitext(up.name)[1]
        with tempfile.NamedTemporaryFile(delete=False, dir=st.session_state["temp_dir"], suffix=suffix) as tmp:
            tmp.write(data)
            temp_path = tmp.name
        entry = {"name": up.name, "path": temp_path, "digest": digest}
        st.session_state["uploads"].append(entry)
        st.session_state["upload_index"][digest] = {"name": up.name, "path": temp_path}
        newly_uploaded.append(entry)

    if newly_uploaded:
        st.session_state["has_new_uploads"] = True
        st.success(f"Saved {len(newly_uploaded)} new file(s) to a temporary session folder")

# ---------- Processing: ONLY when there are truly new uploads ----------
def _needs_processing(file_path):
    current_sig = _file_sig(file_path)
    if current_sig is None:
        return False
    cache_entry = st.session_state["cache"].get(file_path)
    if cache_entry is None:
        return True
    if cache_entry.get("sig") != current_sig:
        return True
    if cache_entry.get("result") is None:
        return True
    return False

pending_paths = []
if st.session_state["has_new_uploads"]:
    for item in newly_uploaded:
        p = item["path"]
        if _needs_processing(p):
            pending_paths.append(p)

def _batch_process(paths):
    if not paths:
        return
    progress_holder = st.empty()
    with st.spinner(f"Processing {len(paths)} document(s)…"):
        progress = progress_holder.progress(0, text="Starting…")
        completed, total = 0, len(paths)

        def _one(p):
            try:
                return process_permit(p)
            except Exception as e:
                st.warning(f"Failed to process {os.path.basename(p)}: {e}")
                st.code(traceback.format_exc())
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, total)) as ex:
            futures = {ex.submit(_one, p): p for p in paths}
            for fut in concurrent.futures.as_completed(futures):
                p = futures[fut]
                res = fut.result()
                current_sig = _file_sig(p)
                st.session_state["cache"][p] = {"sig": current_sig, "result": res, "processed_at": time.time()}
                completed += 1
                progress.progress(int(completed / total * 100), text=f"Processed {completed}/{total}")
                time.sleep(0.02)
    progress_holder.empty()

if pending_paths:
    _batch_process(pending_paths)
    # we've consumed the new uploads; avoid reprocessing on button clicks
    st.session_state["has_new_uploads"] = False

# ---------- Sidebar: Session-only Document List ----------
with st.sidebar:
    st.title("📁 Document Library")
    st.divider()

    st.markdown('<div class="sb-label"><b>Find document:</b></div>', unsafe_allow_html=True)
    q = st.text_input("", placeholder="Search by filename..", key="sb_search")

    session_files = st.session_state["uploads"]
    if q:
        display_files = [it for it in session_files if q.lower() in it["name"].lower()]
        if not display_files:
            st.info(f"No matches for '{q}'")
    else:
        display_files = session_files

    st.markdown('<div class="sb-group">', unsafe_allow_html=True)
    st.markdown('<div class="sb-label"><b>Select document:</b></div>', unsafe_allow_html=True)

    if display_files:
        selected_idx = st.radio(
            "",
            options=list(range(len(display_files))),
            format_func=lambda i: display_files[i]["name"],
            key="sb_file_select_idx",
        )
        selected_path = display_files[selected_idx]["path"]
        if st.session_state["selected_file_path"] != selected_path:
            st.session_state["selected_file_path"] = selected_path

        # status line
        entry = st.session_state["cache"].get(selected_path)
        status_icon = "✓ Processed" if (entry and entry.get("result")) else (
            "Not yet processed" if (entry and "result" in entry and entry.get("result") is None) else "Processing…"
        )
        if os.path.exists(selected_path):
            stat = os.stat(selected_path)
            file_kind = "PDF" if selected_path.lower().endswith(".pdf") else "Image"
            size_kb = stat.st_size // 1024
            st.markdown(
                f'<div class="sb-help">Status: {status_icon} • Type: {file_kind} • Size: {size_kb} KB</div>',
                unsafe_allow_html=True
            )

    else:
        st.info("No files uploaded this session. Use the uploader above.")

    st.divider()

    # Export all from this session
    all_excel = excel_bytes_for_all_docs(st.session_state["cache"])
    st.download_button(
        "📥 Export All Data",
        data=all_excel,
        file_name="cif_forms_extracted.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="sb_download_all",
    )

selected_path = st.session_state.get("selected_file_path")
result = st.session_state["cache"].get(selected_path, {}).get("result") if selected_path else None

st.divider()
col1, col2, col3 = st.columns([30, 1, 40])

with col1:
    st.subheader("Document Preview")
    if selected_path and os.path.exists(selected_path):
        tab_original, tab_processed = st.tabs(["Original Image", "Processed Image"])
        ext = os.path.splitext(selected_path)[1].lower()

        with tab_original:
            if ext == ".pdf":
                st.info("Original is a PDF. You can download and view the original.")
                st.download_button(
                    "⬇️ Download Original PDF",
                    data=open(selected_path, "rb"),
                    file_name=os.path.basename(selected_path),
                )
            else:
                try:
                    st.image(selected_path, use_container_width=True)
                except Exception:
                    st.write("Preview not available for this image type.")
                    st.download_button(
                        "⬇️ Download Original Image",
                        data=open(selected_path, "rb"),
                        file_name=os.path.basename(selected_path),
                    )

        with tab_processed:
            if ext == ".pdf":
                base = os.path.splitext(os.path.basename(selected_path))[0]
                page1_path = os.path.join(OUTPUT_PDF_IMAGES, f"{base}_page_1.png")
                if os.path.exists(page1_path):
                    st.image(page1_path, use_container_width=True)
                else:
                    st.info("Processed preview will appear here after processing.")
            else:
                base = os.path.splitext(os.path.basename(selected_path))[0]
                processed_path = os.path.join(OUTPUT_PROCESSED_IMAGES, f"{base}_processed.png")
                if os.path.exists(processed_path):
                    st.image(processed_path, use_container_width=True)
                else:
                    st.info("Processed preview will appear here after processing.")
    else:
        st.info("No file selected or file not found.")

with col2:
    st.markdown("", unsafe_allow_html=True)

with col3:
    st.subheader("Extracted Data")
    if not result:
        st.info("No extracted data yet. If you just uploaded, processing should complete shortly.")
    elif not selected_path:
        st.info("Please select a document from the sidebar.")
    else:
        tabs = st.tabs(["Customer Information", "Cleaned Text", "Raw Extracted Text"])
        file_key = os.path.basename(selected_path)

        with tabs[0]:
            st.markdown("##### Personal Details")
            last_name = st.text_input(
                "**Last Name**", result.get("Last_Name", ""), key=f"{file_key}_last_name"
            )
            first_name = st.text_input(
                "**First Name**", result.get("First_Name", ""), key=f"{file_key}_first_name"
            )
            suffix = st.text_input(
                "**Suffix**", result.get("Suffix", ""), key=f"{file_key}_suffix",
                help="e.g., Jr., Sr., III"
            )
            middle_name = st.text_input(
                "**Middle Name**", result.get("Middle_Name", ""), key=f"{file_key}_middle_name"
            )
            date_of_birth = st.text_input(
                "**Date of Birth**",
                result.get("Date_of_Birth", ""),
                key=f"{file_key}_date_of_birth",
                help="Format: mm-dd-yyyy"
            )

            st.markdown("##### Place of Birth")
            place_of_birth_town = st.text_input(
                "**Town/Municipality/City**",
                result.get("Place_of_Birth_Town", ""),
                key=f"{file_key}_birth_town"
            )
            place_of_birth_province = st.text_input(
                "**Province/Country**",
                result.get("Place_of_Birth_Province", ""),
                key=f"{file_key}_birth_province"
            )

            bcol1, bcol2 = st.columns(2)
            with bcol1:
                if st.button("Update Record", key=f"{file_key}_update_record"):
                    updated = result.copy()
                    updated.update({
                        "Last_Name": last_name,
                        "First_Name": first_name,
                        "Suffix": suffix,
                        "Middle_Name": middle_name,
                        "Date_of_Birth": date_of_birth,
                        "Place_of_Birth_Town": place_of_birth_town,
                        "Place_of_Birth_Province": place_of_birth_province,
                        # CHANGED: ensure Name_of_file is the ORIGINAL uploaded name
                        "Name_of_file": _original_name_for_path(selected_path),
                    })
                    st.session_state["cache"][selected_path] = {"sig": _file_sig(selected_path), "result": updated}
                    result = updated
                    st.success("✓ Changes saved.")

            with bcol2:
                current = st.session_state["cache"].get(selected_path, {"result": result})["result"]
                # CHANGED: override Name_of_file for export to the original uploaded filename
                if current:
                    current = {**current, "Name_of_file": _original_name_for_path(selected_path)}
                excel_bytes = excel_bytes_for_single_doc(current or {})
                st.download_button(
                    "Export to Excel",
                    data=excel_bytes,
                    # CHANGED: filename now uses the ORIGINAL uploaded filename
                    file_name=f"{_original_name_for_path(selected_path)}_extracted.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"{file_key}_download_excel",
                )

        with tabs[1]:
            if result.get("cleaned_text"):
                st.text_area("Cleaned Text", result.get("cleaned_text", ""), height=300, key=f"{file_key}_cleaned_text")
                base = os.path.splitext(os.path.basename(selected_path))[0]
                cleaned_path = os.path.join(CLEANED_TEXT_FOLDER, f"{base}.txt")
                if os.path.exists(cleaned_path):
                    with open(cleaned_path, "rb") as f:
                        st.download_button("Export Cleaned Text", data=f, file_name=f"{base}.txt", mime="text/plain", key=f"{file_key}_dl_cleaned")
                else:
                    st.download_button("Download", data=(result.get("cleaned_text") or "").encode("utf-8"),
                                       file_name=f"{base}.txt", mime="text/plain", key=f"{file_key}_dl_cleaned_mem")
            else:
                st.info("No cleaned text available.")

        with tabs[2]:
            if result.get("raw_text"):
                st.text_area("Raw Extracted Text", result.get("raw_text", ""), height=300, key=f"{file_key}_raw_text")
                base = os.path.splitext(os.path.basename(selected_path))[0]
                st.download_button("Export Raw Extracted Text",
                                   data=(result.get("raw_text") or "").encode("utf-8"),
                                   file_name=f"{base}_raw.txt", mime="text/plain",
                                   key=f"{file_key}_dl_raw")
            else:
                st.info("No raw OCR text available.")
