# import pandas as pd

# def selective_merge_excel(merged_file_path, addition_file_path, key_column):
#     # Load the addition file (small one)
#     addition_df = pd.read_excel(addition_file_path)
#     addition_df.set_index(key_column, inplace=True)

#     # Determine columns to update (excluding the key column)
#     update_columns = [col for col in addition_df.columns if col != key_column]

#     # Load the merged file
#     merged_df = pd.read_excel(merged_file_path)
#     merged_df.set_index(key_column, inplace=True)

#     # Identify common indices
#     common_index = merged_df.index.intersection(addition_df.index)

#     # Update relevant columns
#     for col in update_columns:
#         if col in merged_df.columns:
#             merged_df.loc[common_index, col] = addition_df.loc[common_index, col]

#     # Reset index and save to a new file
#     merged_df.reset_index(inplace=True)
#     output_file = "updated_" + merged_file_path.split('/')[-1]
#     output_path = f"{output_file}"
#     merged_df.to_excel(output_path, index=False)
    
#     return output_path

# selective_merge_excel("mergedFile.xlsx", "additionFile.xlsx", "respondent_serial")

import pandas as pd
from tqdm import tqdm
import logging

# Setup logging
logging.basicConfig(filename="merge_log.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def selective_merge_excel(merged_file_path, addition_file_path, key_column):
    print("Opening addition file...")
    try:
        addition_df = pd.read_excel(addition_file_path)
        addition_df.set_index(key_column, inplace=True)
    except Exception as e:
        logging.error(f"Failed to open or process addition file: {e}")
        print("Error opening addition file.")
        return

    update_columns = [col for col in addition_df.columns if col != key_column]

    print("Opening merged file...")
    try:
        merged_df = pd.read_excel(merged_file_path)
        merged_df.set_index(key_column, inplace=True)
    except Exception as e:
        logging.error(f"Failed to open or process merged file: {e}")
        print("Error opening merged file.")
        return

    common_index = merged_df.index.intersection(addition_df.index)
    print(f"Found {len(common_index)} matching records.")

    for col in tqdm(update_columns, desc="Updating columns"):
        if col in merged_df.columns:
            merged_df.loc[common_index, col] = addition_df.loc[common_index, col]
        else:
            logging.warning(f"Column '{col}' not found in merged file.")
            print(f"Column '{col}' not found in merged file.")

    merged_df.reset_index(inplace=True)

    output_file = "updated_" + merged_file_path.split('/')[-1]
    output_path = f"{output_file}"

    print("Saving updated file...")
    try:
        merged_df.to_excel(output_path, index=False)
        print(f"Merge complete. Output saved as: {output_path}")
        logging.info("Merge completed successfully.")
    except Exception as e:
        logging.error(f"Failed to save output file: {e}")
        print("Error saving output file.")
    
    return output_path


selective_merge_excel("mergedFile.xlsx", "additionFile.xlsx", "respondent_serial")
