
import re
import pandas as pd
import pdfplumber
import os
def extract_data(text):
    
    extracted_data = {}
    
    # Extract title from above the "1 ITEM" line
    # title_pattern = r"(.*?)\n.*?1\s+ITEM"
    # title_match = re.search(title_pattern, text, re.DOTALL)
    # if title_match:
    #     # Get the last line of the text above "1 ITEM"
    #     title_lines = title_match.group(1).strip().split('\n')
    #     if title_lines:
    #         extracted_data["Title"] = title_lines[-1].strip()
    
    # # Extract Item value
    # item_pattern = r"1\s+ITEM\s+([\w\d\-]+\s*[\w\d/]*)"
    # item_match = re.search(item_pattern, text)
    item_pattern = r"1\s+ITEM\s+(.*?)(?:\n|$)"
    item_match = re.search(item_pattern, text)
    if item_match:
        extracted_data["item"] = item_match.group(1).strip()
    
    # Extract Service value
    service_pattern = r"2\s+SERVICE\s+(.*?)\s+DEVICE\s+TYPE"
    service_match = re.search(service_pattern, text)
    if service_match:
        extracted_data["service"] = service_match.group(1).strip()
    
    # Extract Device Type value
    #device_type_pattern = r"DEVICE\s+TYPE\s+(.*?)(?:\r?\n|$)"
    # device_type_pattern = r"DEVICE\s+TYPE\s+(.*?)(?:\n)"
    # #device_type_pattern = r"DEVICE\s+TYPE\s+(.*?)(?:\n|$)"
    # device_type_match = re.search(device_type_pattern, text)
    # if device_type_match:
    #     extracted_data["Device Type"] = device_type_match.group(1).strip()
    
    # Find the line containing "DEVICE TYPE"
    # Extract Burst Pressure        
    pressure_pattern = r"SET\s+/\s+BURST\s+PRESSURE\s+(.*?)\s+OVERPRESSURE\s+%"
    pressure_match = re.search(pressure_pattern, text)
    if pressure_match:
        extracted_data["set_or_burst_pressure"] = pressure_match.group(1).strip()
        
    # Extract Fluid Description   
    fluid_pattern = r"11\s+FLUID\s+DESCRIPTION\s+(.*?)\s+OPERATING\s+PRESSURE"
    fluid_match = re.search(fluid_pattern, text)
    if fluid_match:
        extracted_data["fluid_description"] = fluid_match.group(1).strip()
        
    lines = text.split('\n')
    device_type_line_index = None

    for i, line in enumerate(lines):
        if re.search(r"DEVICE\s+TYPE", line):
            device_type_line_index = i
            break

    if device_type_line_index is not None:
        # Get the text after "DEVICE TYPE" on the same line
        line = lines[device_type_line_index]
        after_device_type = re.search(r"DEVICE\s+TYPE\s+(.*?)$", line)
        
        if after_device_type and after_device_type.group(1).strip():
            extracted_data["device_type"] = after_device_type.group(1).strip()
        else:
            extracted_data["device_type"] = ""
    
    return extracted_data

def extract_pdf(pdf_path):
    all_extracted_data = []
    
    with pdfplumber.open(pdf_path) as pdf:
              
        for page in pdf.pages:
            text = page.extract_text()
            #print(text)
            extracted_data = extract_data(text)
            extracted_data["Filename"] = os.path.basename(pdf_path)
            #print(extracted_data)
            
            if any(key in extracted_data for key in ["item", "service", "device_type", "set_or_burst_pressure", "fluid_description"]):
                all_extracted_data.append(extracted_data)
            
    return all_extracted_data

def main():
    pdf_path= r"C:\Users\isarivelan.mani\repo\rag-chatbot\backend\adnoc\0751\pdf7\SR03-0751-00-DAT-303171_3.pdf"
    
    all_extracted_data = extract_pdf(pdf_path)   
    print(all_extracted_data)
    
    df_list = []
    for data in all_extracted_data:
        df = pd.DataFrame([data])  # Creates a single-row DataFrame
        df_list.append(df)
        
    # Concatenate all DataFrames into one
    df_concat = pd.concat(df_list, ignore_index=True)
    df_concat['item'].str.strip()
    df_concat['item'].drop_duplicates()
    
    # Export to CSV
    df_concat.to_excel("safety_valve_dataset.xlsx", index=False)
    print("\nData exported to CSV file.")
    
    


if __name__ == "__main__":
    
    main()
    
    