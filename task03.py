# import pdfplumber
# import re
# import pandas as pd
# import os
# from typing import List, Dict

# def extract_text_from_pdf(pdf_path: str) -> str:
#     """
#     Extract text content from a PDF file.
    
#     Args:
#         pdf_path: Path to the PDF file
        
#     Returns:
#         Extracted text content from the PDF
#     """
#     full_text = ""
#     try:
#         with pdfplumber.open(pdf_path) as pdf:
#             for page in pdf.pages:
#                 page_text = page.extract_text()
#                 if page_text:
#                     full_text += page_text + "\n"
#         return full_text
#     except Exception as e:
#         print(f"Error extracting text from PDF: {str(e)}")
#         return ""
    


# def extract_all_tables(pdf_path):
#     table_counter = 0
    
#     with pdfplumber.open(pdf_path) as pdf:
#         for page_num, page in enumerate(pdf.pages):
#             print(f"Processing page {page_num + 1}...")
#             tables = page.extract_tables()
            
#             for table in tables:
#                 if table:  # Check if table is not empty
#                     try:
#                         df = pd.DataFrame(table)
#                         # Remove completely empty rows and columns
#                         df = df.dropna(how='all').dropna(axis=1, how='all')
                        
#                         if not df.empty:  # Only save if dataframe is not empty
#                             filename = f"output_page{page_num + 1}_table{table_counter}.xlsx"
#                             df.to_excel(filename, index=False)
#                             print(f"Saved: {filename}")
#                             table_counter += 1
#                         else:
#                             print(f"Skipped empty table on page {page_num + 1}")
#                     except Exception as e:
#                         print(f"Error processing table on page {page_num + 1}: {e}")
#                         continue
    
#     print(f"Total tables extracted: {table_counter}")


    
    
# def main():
#     # Define the path to the PDF file
#     pdf_path = r"C:\Users\isarivelan.mani\repo\rag-chatbot\backend\adnoc\0751\task03\SR03-0751-15-LST-348180_Z3.pdf"
    
#     # Extract text from the PDF
#     #text_content = extract_text_from_pdf(pdf_path)
#     extract_all_tables(pdf_path)
#     # Print the extracted text content
#     #print("Extracted Text Content:")
#     #print(text_content)
#     # Print the extracted data
    
    
#     # # Extract table data from the text content
#     # table_data = extract_table_data(text_content)
    
#     # # Convert the extracted data to a DataFrame
#     # df = pd.DataFrame(table_data)
    
#     # # Save the DataFrame to an Excel file
#     # excel_path = "output.xlsx"
#     # df.to_excel(excel_path, index=False)
    
#     # print(f"Data extracted and saved to {excel_path}")
    
# if __name__ == "__main__":
#     main()
    

import pdfplumber
import pandas as pd
from collections import defaultdict
import hashlib

def get_table_structure(table):
    """
    Extract the structure/format of a table for comparison
    Returns a tuple representing the table format
    """
    if not table or len(table) == 0:
        return None
    
    # Get number of columns
    num_cols = len(table[0]) if table[0] else 0
    
    # Get header row (first non-empty row)
    header = None
    for row in table:
        if row and any(cell and str(cell).strip() for cell in row):
            header = tuple(str(cell).strip().lower() if cell else '' for cell in row)
            break
    
    # Create a structure signature
    structure = {
        'num_columns': num_cols,
        'header': header,
        'num_rows': len(table)
    }
    
    return structure

def tables_have_same_format(table1, table2):
    """
    Check if two tables have the same format
    """
    struct1 = get_table_structure(table1)
    struct2 = get_table_structure(table2)
    
    if not struct1 or not struct2:
        return False
    
    # Compare number of columns and headers
    return (struct1['num_columns'] == struct2['num_columns'] and 
            struct1['header'] == struct2['header'])

def group_tables_by_format(all_tables_info):
    """
    Group tables by their format/structure
    Returns a dictionary where keys are format signatures and values are lists of table info
    """
    format_groups = defaultdict(list)
    
    for table_info in all_tables_info:
        table = table_info['table']
        structure = get_table_structure(table)
        
        if structure:
            # Create a hashable key for the structure
            key = (structure['num_columns'], structure['header'])
            format_groups[key].append(table_info)
    
    return format_groups

def merge_similar_tables(tables_info_list):
    """
    Merge tables with the same format into a single DataFrame
    """
    if not tables_info_list:
        return None
    
    merged_dfs = []
    
    for table_info in tables_info_list:
        table = table_info['table']
        if table:
            df = pd.DataFrame(table)
            # Add metadata columns
            df['source_page'] = table_info['page']
            df['source_table'] = table_info['table_index']
            merged_dfs.append(df)
    
    if merged_dfs:
        # Concatenate all dataframes
        merged_df = pd.concat(merged_dfs, ignore_index=True)
        # Remove completely empty rows and columns
        merged_df = merged_df.dropna(how='all').dropna(axis=1, how='all')
        return merged_df
    
    return None

def extract_and_merge_tables(pdf_path, min_group_size=2):
    """
    Extract all tables from PDF, group by format, and merge similar ones
    
    Args:
        pdf_path: Path to PDF file
        min_group_size: Minimum number of tables needed to form a group for merging
    """
    all_tables_info = []
    
    # Extract all tables with metadata
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            print(f"Processing page {page_num + 1}...")
            tables = page.extract_tables()
            
            for table_idx, table in enumerate(tables):
                if table:
                    all_tables_info.append({
                        'table': table,
                        'page': page_num + 1,
                        'table_index': table_idx,
                        'id': f"page{page_num + 1}_table{table_idx}"
                    })
    
    print(f"Found {len(all_tables_info)} tables total")
    
    # Group tables by format
    format_groups = group_tables_by_format(all_tables_info)
    
    merged_count = 0
    individual_count = 0
    
    for group_key, tables_group in format_groups.items():
        num_cols, header = group_key
        print(f"\nFormat: {num_cols} columns, Header: {header}")
        print(f"Number of tables with this format: {len(tables_group)}")
        
        if len(tables_group) >= min_group_size:
            # Merge tables with same format
            merged_df = merge_similar_tables(tables_group)
            if merged_df is not None and not merged_df.empty:
                filename = f"merged_format_{merged_count}.xlsx"
                merged_df.to_excel(filename, index=False)
                print(f"✓ Merged {len(tables_group)} tables into: {filename}")
                
                # Also save a summary
                summary_info = {
                    'merged_filename': filename,
                    'num_tables_merged': len(tables_group),
                    'source_tables': [info['id'] for info in tables_group],
                    'format': {'columns': num_cols, 'header': header}
                }
                
                with open(f"merge_info_{merged_count}.txt", 'w') as f:
                    f.write(f"Merged file: {filename}\n")
                    f.write(f"Number of tables merged: {len(tables_group)}\n")
                    f.write(f"Source tables: {', '.join(summary_info['source_tables'])}\n")
                    f.write(f"Format: {num_cols} columns\n")
                    f.write(f"Header: {header}\n")
                
                merged_count += 1
        else:
            # Save individual tables
            for table_info in tables_group:
                df = pd.DataFrame(table_info['table'])
                filename = f"individual_{table_info['id']}.xlsx"
                df.to_excel(filename, index=False)
                individual_count += 1
    
    print(f"\nSummary:")
    print(f"- Created {merged_count} merged files")
    print(f"- Created {individual_count} individual files")
    print(f"- Found {len(format_groups)} different table formats")

def analyze_table_formats(pdf_path):
    """
    Just analyze and report table formats without merging
    """
    all_tables_info = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            for table_idx, table in enumerate(tables):
                if table:
                    all_tables_info.append({
                        'table': table,
                        'page': page_num + 1,
                        'table_index': table_idx,
                        'id': f"page{page_num + 1}_table{table_idx}"
                    })
    
    format_groups = group_tables_by_format(all_tables_info)
    
    print("Table Format Analysis:")
    print("=" * 50)
    
    for i, (group_key, tables_group) in enumerate(format_groups.items()):
        num_cols, header = group_key
        print(f"\nFormat Group {i + 1}:")
        print(f"  Columns: {num_cols}")
        print(f"  Header: {header}")
        print(f"  Count: {len(tables_group)} tables")
        print(f"  Tables: {[info['id'] for info in tables_group]}")

# Usage examples:
if __name__ == "__main__":
    pdf_path = r"your_pdf_path"
    
    # Option 1: Just analyze formats
    print("=== ANALYSIS ONLY ===")
    analyze_table_formats(pdf_path)
    
    print("\n" + "="*60 + "\n")
    
    # Option 2: Extract and merge
    print("=== EXTRACT AND MERGE ===")
    extract_and_merge_tables(pdf_path, min_group_size=2)
