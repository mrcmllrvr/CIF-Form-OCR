# main.py - CIF Form Data Extraction
# Extracts personal details from Customer Information File (CIF) Individual forms

import concurrent.futures
import base64
import os
import requests
import csv
import json
from mimetypes import guess_type
from dotenv import load_dotenv
from pdf2image import convert_from_path
from datetime import datetime
import re
import cv2
import numpy as np
from PIL import Image
from io import BytesIO
import pandas as pd
import openpyxl
import time
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import io
import hashlib
import pathlib

load_dotenv()

# --------------------- Config: Azure Endpoints & Keys ---------------------
endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
api_key = os.getenv("AZURE_OPENAI_API_KEY")
if not endpoint or not api_key:
    raise ValueError("Azure OpenAI endpoint and API key must be set in .env file")

# Azure Document Intelligence API endpoint and key
adi_endpoint = os.getenv("ADI_ENDPOINT")
adi_api_key = os.getenv("ADI_API_KEY")
if not adi_endpoint or not adi_api_key:
    raise ValueError("Azure ADI endpoint and API key must be set in .env file")

headers = {
    "Content-Type": "application/json",
    "api-key": api_key
}

# --------------------- Image Helpers ---------------------
def convert_pdf_to_images(pdf_path, output_folder, dpi=300):
    """
    Convert PDF pages to images. Higher DPI preserves small strokes (e.g., 'w').
    """
    os.makedirs(output_folder, exist_ok=True)
    images = convert_from_path(pdf_path, dpi=dpi)
    image_paths = []
    for i, image in enumerate(images):
        image_path = os.path.join(
            output_folder,
            f"{os.path.splitext(os.path.basename(pdf_path))[0]}_page_{i + 1}.png"
        )
        image.save(image_path, "PNG")
        image_paths.append(image_path)
    return image_paths, len(images)

def preprocess_image(image: Image.Image) -> Image.Image:
    """
    Gentle, optional preprocessing for display/noisy scans ONLY.
    We do NOT feed this to OCR by default to avoid glyph breakup (e.g., 'w'→'n'+'j').
    """
    open_cv_image = np.array(image)
    open_cv_image = cv2.cvtColor(open_cv_image, cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(open_cv_image, cv2.COLOR_BGR2GRAY)

    # (Optional) Crop to largest contour (page); safe in most cases, but not required
    contours, _ = cv2.findContours(gray, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if contours:
        cnts_sorted = sorted(contours, key=lambda x: cv2.contourArea(x), reverse=True)
        x, y, w, h = cv2.boundingRect(cnts_sorted[0])
        gray = gray[y:y+h, x:x+w]

    # Gentler binarization: Otsu (avoids eroding diagonal strokes of 'w')
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return Image.fromarray(thresh)

def convert_image_to_base64(image_path):
    mime_type, _ = guess_type(image_path)
    if mime_type is None:
        mime_type = "application/octet-stream"
    with open(image_path, "rb") as image_file:
        base64_encoded_data = base64.b64encode(image_file.read()).decode("utf-8")
    return f"data:{mime_type};base64,{base64_encoded_data}"

# --------------------- Azure Document Intelligence (OCR) ---------------------
def get_raw_text(image_data_url_or_path, model_name="prebuilt-read"):
    """
    Use Azure Document Intelligence to read plain text (prebuilt-read).
    Accepts either a data URL (data:...;base64,...) or a file path.
    """
    try:
        client = DocumentAnalysisClient(endpoint=adi_endpoint, credential=AzureKeyCredential(adi_api_key))

        if isinstance(image_data_url_or_path, str) and image_data_url_or_path.startswith('data:'):
            # data URL: decode to bytes
            _, base64_data = image_data_url_or_path.split(',', 1)
            image_bytes = base64.b64decode(base64_data)
            image_stream = io.BytesIO(image_bytes)
            poller = client.begin_analyze_document(model_name, image_stream)
            result = poller.result()
        else:
            # assume file path
            with open(image_data_url_or_path, "rb") as image_file:
                poller = client.begin_analyze_document(model_name, image_file)
                result = poller.result()

        if hasattr(result, "content") and result.content:
            return result.content

        # Fallback: join lines
        extracted_text = " ".join([line.content for page in result.pages for line in page.lines])
        return extracted_text

    except Exception as e:
        print(f"Error analyzing document: {e}")
        import traceback
        traceback.print_exc()
        return None

# --------------------- Text Cleaning (LLM) ---------------------
def clean_ocr_text(raw_text):
    """
    Clean raw OCR text using Azure OpenAI Chat endpoint.
    """
    extracted_text = raw_text
    system_prompt = """
    You are an expert OCR text cleaner specializing in banking forms. Your task is to clean and format the raw OCR text to make it more readable and easier to parse for data extraction.

    Fix spacing and line breaks, correct obvious OCR errors, preserve structure, and do not add information. Output plain text only.
    """.strip()

    try:
        data = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": extracted_text}
            ],
            "max_tokens": 1200,
            "temperature": 0
        }
        response = requests.post(endpoint, headers=headers, json=data)
        response.raise_for_status()
        cleaned_text = response.json()["choices"][0]["message"]["content"]
        return cleaned_text
    except Exception as e:
        print(f"Error in OCR text cleaning: {str(e)}")
        return raw_text

# --------------------- Structured Data (LLM) ---------------------
def parse_structured_response(response_content):
    if isinstance(response_content, dict):
        return response_content
    if isinstance(response_content, str):
        json_match = re.search(r'<initial_attempt>\s*```json(.*?)```\s*</initial_attempt>', response_content, re.DOTALL)
        if json_match:
            json_str = json_match.group(1).strip()
            try:
                structured_data = json.loads(json_str)
                return structured_data
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                print("Extracted JSON was:", json_str)
                return None
        else:
            print("No JSON found in <initial_attempt> tags.")
            return None
    print("Unexpected response content type:", type(response_content))
    return None

def get_structured_data_from_text(raw_text):
    system_prompt = """
        You are an AI assistant specialized in extracting personal details from banking Customer Information File (CIF) forms.

        <user_task>
        ═══════════════════════════════════════════════════════════════
        1. PURPOSE AND OUTPUT REQUIREMENTS
        ═══════════════════════════════════════════════════════════════
        1.1 Goal: Extract personal details from Customer Information File Individual forms with absolute accuracy.

        Key Objectives:
        • Extract ONLY the specified personal detail fields
        • Focus on name components and birth information
        • Maintain exact spelling and formatting from the form
        • Handle handwritten entries carefully

        1.2 Critical Requirements:
        • Extract ONLY the specified fields
        • Strict JSON format – no deviations
        • Missing fields must be explicitly labeled as "None"
        • Multi-page documents must be combined into a single structured JSON object
        • No assumptions or inferences: Only extract what is explicitly visible
        • Preserve exact spelling of names
        • ALL dates must be in mm-dd-yyyy format (e.g., 03-15-2024, 01-01-1990)
        • NEVER infer or calculate dates from partial information

        ═══════════════════════════════════════════════════════════════
        2. FIELD EXTRACTION RULES
        ═══════════════════════════════════════════════════════════════
        
        2.1 Personal Name Components:
        • Last Name: Extract from "Last Name" field
        • First Name: Extract from "First Name" field
        • Suffix: Extract from "Suffix" field (e.g., Jr., Sr., III, IV)
        • Middle Name: Extract from "Middle Name" field
        • Preserve exact spelling from the form, but normalize capitalization so each word begins with a capital letter (e.g., "Dela Cruz", "Juan", "Santos")
        • Include all components even if handwritten

        2.2 Date of Birth:
        • Format: mm-dd-yyyy (e.g., 03-15-1985, 12-01-1990)
        • Extract from "Date of Birth" field
        • CRITICAL: Only extract if COMPLETE date (month, day, year) is visible
        • If incomplete, use "[unclear]"
        • Common format on form: MM-DD-YYYY with boxes

        2.3 Place of Birth:
        • Town/Municipality/City: Extract from the first field under "Place of Birth"
        • Province/Country: Extract from the second field under "Place of Birth"
        • May be handwritten
        • Preserve exact spelling and correct capitalization for all place names worldwide (e.g., "Manila", "Los Angeles", "Tokyo")

        ═══════════════════════════════════════════════════════════════
        3. OUTPUT FORMAT
        ═══════════════════════════════════════════════════════════════
        Produce a single JSON object containing exactly the following fields:

        {
            "Document_Type": "DBP Customer Information File - Individual",
            "Page_Count": "integer",
            "Last_Name": "string",
            "First_Name": "string",
            "Suffix": "string",
            "Middle_Name": "string",
            "Date_of_Birth": "string (mm-dd-yyyy format, or [unclear] if incomplete)",
            "Place_of_Birth_Town": "string (Town/Municipality/City)",
            "Place_of_Birth_Province": "string (Province/Country)"
        }

        Notes:  
        • Mark any field explicitly absent as "None"
        • If data is visible but unclear, use "[unclear]"
        • Ensure no extraneous keys are added
        • Use underscore format for field names to ensure Excel compatibility
        • Format ALL dates as mm-dd-yyyy (e.g., 03-15-1985, 12-01-1990)

        ═══════════════════════════════════════════════════════════════
        4. DATE EXTRACTION RULES - STRICT COMPLIANCE REQUIRED
        ═══════════════════════════════════════════════════════════════

        For Date_of_Birth field:

        ONLY extract dates that are COMPLETELY and EXPLICITLY visible with ALL three components:
        • Full month number (01-12)
        • Full day number (01-31)
        • Full year (4 digits)

        If ANY component is missing, unclear, or requires inference:
        • Return "[unclear]" 
        • DO NOT infer missing values
        • DO NOT assume dates from partial information

        Valid Examples:
        ✓ "03-15-1985" → "03-15-1985"
        ✓ "12/01/1990" → "12-01-1990"
        ✓ "01-25-2000" → "01-25-2000"
        
        Invalid Examples (use "[unclear]"):
        ✗ "1985" → "[unclear]" (only year visible)
        ✗ "03-1985" → "[unclear]" (day missing)
        ✗ "__-15-1985" → "[unclear]" (month unclear)

        REMEMBER: When in doubt, use "[unclear]". Never guess or calculate dates.

        ═══════════════════════════════════════════════════════════════
        5. OUTPUT EXAMPLE
        ═══════════════════════════════════════════════════════════════
        {
            "Document_Type": "DBP Customer Information File - Individual",
            "Page_Count": "1",
            "Last_Name": "Dela Cruz",
            "First_Name": "Juan",
            "Suffix": "Jr.",
            "Middle_Name": "Santos",
            "Date_of_Birth": "03-15-1985",
            "Place_of_Birth_Town": "Manila",
            "Place_of_Birth_Province": "Metro Manila"
        }

        ═══════════════════════════════════════════════════════════════
        6. CRITICAL NOTES
        ═══════════════════════════════════════════════════════════════
        • Cultural Sensitivity: Preserve Filipino naming conventions including compound surnames
        • Handwritten Text: Be especially careful with handwritten entries which are common on these forms
        • Date Extraction: ONLY extract complete dates. Use "[unclear]" for any incomplete date information
        • Place Names: Extract complete place names with correct spelling
        • The PRIMARY SUCCESS METRIC is accuracy of the extracted personal details

        </user_task>

        Please follow these steps:

        1. Initial Attempt:
        Make an initial attempt at completing the task focusing on accurate field extraction. Present this attempt in <initial_attempt> tags with JSON format.

        2. Final Answer:
        Present your final JSON answer in <answer> tags after analysis.

    """.strip()

    try:
        data = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": [
                    {"type": "text", "text": "Extract and structure the information from the following CIF form text. Provide your response in JSON format wrapped within ```json and ``` inside <initial_attempt> tags."},
                    {"type": "text", "text": raw_text}
                ]}
            ],
            "max_tokens": 3000,
            "temperature": 0.0
        }
        response = requests.post(endpoint, headers=headers, json=data)
        response.raise_for_status()
        response_content = response.json()["choices"][0]["message"]["content"]
        structured_data = parse_structured_response(response_content)
        return structured_data
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None

# --------------------- Small helpers ---------------------
def flatten_json(nested_json):
    flat = {}
    for key, value in nested_json.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                flat[subkey] = subvalue
        else:
            flat[key] = value
    return flat

def save_to_excel(structured_data_list, excel_output_path):
    csv_headers = [
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

    flat_data_list = []
    for item in structured_data_list:
        flat = flatten_json(item)
        flat_data_list.append(flat)

    df = pd.DataFrame(flat_data_list)
    for col in csv_headers:
        if col not in df.columns:
            df[col] = None
    df = df[csv_headers]
    df.to_excel(excel_output_path, index=False)
    print(f"Excel file saved to: {excel_output_path}")

# --------------------- PDF/Image processing ---------------------
def _hash_file(path):
    return hashlib.md5(pathlib.Path(path).read_bytes()).hexdigest()

def process_pdf(pdf_file, pdf_folder, image_folder):
    """
    Accuracy-first PDF pipeline:
    - 300 DPI render
    - OCR the ORIGINAL images (not binarized)
    - Optional preprocessing is for display only (not used for OCR)
    - Parallel OCR
    """
    pdf_path = os.path.join(pdf_folder, pdf_file)
    print(f"Processing PDF: {pdf_file}...")
    image_paths, page_count = convert_pdf_to_images(pdf_path, image_folder, dpi=180)

    # ---- OCR pages concurrently on ORIGINAL images
    def _ocr_one(img_path):
        b64 = convert_image_to_base64(img_path)
        return get_raw_text(b64) or ""

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(6, len(image_paths))) as ex:
        ocr_texts = list(ex.map(_ocr_one, image_paths))

    raw_text = "\n".join(ocr_texts)

    # ---- (Optional) create processed previews for UI without overwriting originals
    for img_path in image_paths:
        try:
            img = Image.open(img_path)
            proc = preprocess_image(img)
            # save a sidecar preview if desired; here we skip writing to avoid confusion
            # preview_path = img_path.replace(".png", "_proc.png")
            # proc.save(preview_path)
        except Exception:
            pass

    # ---- Clean text (LLM)
    cleaned_text = clean_ocr_text(raw_text)

    # ---- Persist cleaned text for UI download (same behavior as before)
    os.makedirs('cleaned_text', exist_ok=True)
    with open(f'cleaned_text/{pdf_file.replace(".pdf", "")}.txt', 'w', encoding='utf-8') as file:
        file.write(cleaned_text)

    # ---- Structure data (LLM)
    structured_api_response = get_structured_data_from_text(cleaned_text)
    structured_data = structured_api_response or {}

    if structured_data:
        structured_data["Name_of_file"] = pdf_file
        structured_data["Page_Count"] = page_count
        structured_data["raw_text"] = raw_text
        structured_data["cleaned_text"] = cleaned_text

    return structured_data

def process_image(image_file, image_input_folder, image_output_folder):
    """
    Single-image path:
    - OCR the ORIGINAL image
    - Optional preprocessing for preview only
    """
    image_path = os.path.join(image_input_folder, image_file)
    print(f"Processing Image: {image_file}...")

    # OCR original
    b64 = convert_image_to_base64(image_path)
    raw_text = get_raw_text(b64) or ""

    # Preview (optional)
    try:
        base_name = os.path.splitext(os.path.basename(image_path))[0]
        os.makedirs(image_output_folder, exist_ok=True)
        img = Image.open(image_path)
        proc = preprocess_image(img)
        proc.save(os.path.join(image_output_folder, f"{base_name}_processed.png"))
    except Exception:
        pass

    # Clean
    cleaned_text = clean_ocr_text(raw_text)

    # Persist cleaned text (if app expects it)
    os.makedirs('cleaned_text', exist_ok=True)
    with open(f'cleaned_text/{base_name}.txt', 'w', encoding='utf-8') as file:
        file.write(cleaned_text)

    # Structure
    structured_api_response = get_structured_data_from_text(cleaned_text)
    structured_data = structured_api_response or {}

    if structured_data:
        structured_data["Name_of_file"] = image_file
        structured_data["Page_Count"] = 1
        structured_data["raw_text"] = raw_text
        structured_data["cleaned_text"] = cleaned_text

    return structured_data

# --------- CLI entry (optional local run) ---------
def main():
    pdf_folder = r"C:\path\to\input\pdfs"
    image_input_folder = r"C:\path\to\input\images"
    pdf_image_output_folder = r"C:\path\to\output\pdf_images"
    image_output_folder = r"C:\path\to\output\processed_images"
    excel_output = r"C:\path\to\output\cif_forms_extracted.xlsx"

    os.makedirs(pdf_image_output_folder, exist_ok=True)
    os.makedirs(image_output_folder, exist_ok=True)
    os.makedirs(os.path.dirname(excel_output), exist_ok=True)
    os.makedirs('cleaned_text', exist_ok=True)

    structured_data_list = []
    pdf_files = [f for f in os.listdir(pdf_folder)] if os.path.exists(pdf_folder) else []
    image_files = [f for f in os.listdir(image_input_folder)] if os.path.exists(image_input_folder) else []

    if pdf_files:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(process_pdf, pdf_file, pdf_folder, pdf_image_output_folder): pdf_file
                for pdf_file in pdf_files
            }
            for future in concurrent.futures.as_completed(futures):
                pdf_file = futures[future]
                try:
                    structured_data = future.result()
                    if structured_data:
                        structured_data_list.append(structured_data)
                except Exception as exc:
                    print(f"{pdf_file} generated an exception: {exc}")

    if image_files:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(process_image, image_file, image_input_folder, image_output_folder): image_file
                for image_file in image_files
            }
            for future in concurrent.futures.as_completed(futures):
                image_file = futures[future]
                try:
                    structured_data = future.result()
                    if structured_data:
                        structured_data_list.append(structured_data)
                except Exception as exc:
                    print(f"{image_file} generated an exception: {exc}")

    if structured_data_list:
        save_to_excel(structured_data_list, excel_output)
    else:
        print("No structured data extracted.")

def process_permit(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        pdf_folder = os.path.dirname(file_path)
        image_output_folder = os.path.join("output", "pdf_images")
        os.makedirs(image_output_folder, exist_ok=True)
        return process_pdf(os.path.basename(file_path), pdf_folder, image_output_folder)
    elif ext in [".jpg", ".jpeg", ".png"]:
        image_folder = os.path.dirname(file_path)
        image_output_folder = os.path.join("output", "processed_images")
        os.makedirs(image_output_folder, exist_ok=True)
        return process_image(os.path.basename(file_path), image_folder, image_output_folder)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Script generated an exception: {exc}")
