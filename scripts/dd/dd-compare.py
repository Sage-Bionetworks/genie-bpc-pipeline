"""
Purpose: Takes in two PRISSMM data dictionaries and print out a simple comparison report
Author: Sage Bionetworks
Date: 29AUG2024

Environments:
pandas==3.9
synapseclient==4.1.1
"""

import pandas as pd
import synapseclient
import argparse
from io import StringIO

# Log in 
syn = synapseclient.Synapse()
syn.login()

# Set the default PRISSMM data dictionaries

default1 = 'syn52903784' #v4.0.1
default2 = 'syn61600728' #v4.1.4

# Initialize arg parser
parser = argparse.ArgumentParser(description="Compare two data dictionaries.")
parser.add_argument('--synid1', type=str, default=default1, help='synapse ID of the first PRISSMM data dictionary CSV')
parser.add_argument('--synid2', type=str, default=default2, help='synapse ID of the second PRISSMM data dictionary CSV')

# Parse the arguments
args = parser.parse_args()

entity1 = syn.get(args.synid1)
entity2 = syn.get(args.synid2)

"""
Function to clean up the multiline content while preserving specific rows

Example multiline input:
image_casite6,prissmm_imaging,,dropdown,"Where is the cancer located?
Cancer Site 6", ...

Example cleaned output:
image_casite6,prissmm_imaging,,dropdown,"Where is the cancer located? Cancer Site 6", ...
"""
def clean_multiline_content(file_content):
    cleaned_lines = []
    temp_line = ''
    inside_quotes = False

    for line in file_content:
        if 'Change Type = Removed' in line:
            cleaned_lines.append(line.strip())
            continue
        
        if inside_quotes:
            temp_line += ' ' + line.strip()
            if line.count('"') % 2 != 0:  # Odd number of quotes indicates end of quoted section
                inside_quotes = False
                cleaned_lines.append(temp_line)
                temp_line = ''
        else:
            if line.count('"') % 2 != 0:  # Odd number of quotes indicates start of quoted section
                inside_quotes = True
                temp_line = line.strip()
            else:
                cleaned_lines.append(line.strip())
                
    return cleaned_lines

# Function to read and clean a CSV file, then load into a pandas DataFrame
def read_and_clean_csv(file_path):
    with open(file_path, 'r') as file:
        content = file.readlines()

    # Clean the content
    cleaned_content = clean_multiline_content(content)

    # Create a temporary CSV file in memory
    temp_csv = StringIO("\n".join(cleaned_content))

    # Read the cleaned content into a pandas DataFrame
    df = pd.read_csv(temp_csv)
    
    return df

# Clean and read both CSV files into DataFrames
df1 = read_and_clean_csv(entity1.path)
df2 = read_and_clean_csv(entity2.path)

folder_name1 = syn.get(entity1.parentId).name
folder_name2 = syn.get(entity2.parentId).name


print(f"Comparing {folder_name1} and {folder_name2}")

# Ensure that the necessary columns are in both DataFrame
required_columns = [
    "Variable / Field Name", "Form Name", "Field Type", "Field Label",
    "Choices, Calculations, OR Slider Labels", "Field Note", 
    "Text Validation Type OR Show Slider Number", "Text Validation Min", 
    "Text Validation Max", "Identifier?", "Required Field?"
]

missing_columns_df1 = [col for col in required_columns if col not in df1.columns]
missing_columns_df2 = [col for col in required_columns if col not in df2.columns]

if missing_columns_df1 or missing_columns_df2:
    print("One or more required columns are missing in the dataframes.")
    if missing_columns_df1:
        print(f"Missing in {folder_name1}: {missing_columns_df1}")
    if missing_columns_df2:
        print(f"Missing in {folder_name2}: {missing_columns_df2}")
else:
    # Initialize a list to store the comparison results
    comparison_results = []

    # Extract the unique variable names from each DataFrame
    variables_df1 = set(df1["Variable / Field Name"])
    variables_df2 = set(df2["Variable / Field Name"])

    # Find added and removed variables
    added_variables = variables_df2 - variables_df1
    removed_variables = variables_df1 - variables_df2

    # Handle Added Variables
    for variable in added_variables:
        comparison_results.append([variable, f"Added (in {folder_name2})", "Variable / Field Name", "N/A", "N/A"])

    # Handle Removed Variables
    for variable in removed_variables:
        comparison_results.append([variable, f"Missing (from {folder_name2})", "Variable / Field Name", "N/A", "N/A"])

    # Handle Updated Variables
    common_variables = variables_df1 & variables_df2
    for variable in common_variables:
        row_df1 = df1[df1["Variable / Field Name"] == variable].iloc[0]
        row_df2 = df2[df2["Variable / Field Name"] == variable].iloc[0]

        for column in required_columns[1:]:  # Skip the first column ("Variable / Field Name")
            value_df1 = row_df1[column]
            value_df2 = row_df2[column]

            # Check if values are different
            if value_df1 != value_df2:
                # Check if both values are blank or NaN
                if not ((pd.isna(value_df1) or value_df1 == "") and (pd.isna(value_df2) or value_df2 == "")):
                    comparison_results.append([
                        variable,
                        "Modified",
                        column,
                        value_df1 if pd.notna(value_df1) else "",
                        value_df2 if pd.notna(value_df2) else ""
                    ])

    # Convert the comparison results into a DataFrame
    comparison_df = pd.DataFrame(
        comparison_results,
        columns=[
            "Variable / Field Name",
            "Update Type",
            "REDCap Column",
            f"Value ({folder_name1})",
            f"Value ({folder_name2})"
        ]
    )

    # Save the comparison results to a CSV file
    comparison_df.to_csv("comparison.csv", index=False)

    print("Comparison complete. Results saved to 'comparison.csv'.")
