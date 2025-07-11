
import camelot
import os
import pandas as pd

print("Current working directory:", os.getcwd())
print("Files in directory before processing:", os.listdir())

try:
    # Step 1: Read PDF and extract only page 260
    tables = camelot.read_pdf("crimes.pdf", pages="259", flavor="stream")
    print(f"Total tables detected: {len(tables)}")

    crime_table_found = False

    # Step 2: Loop through tables and find one containing crime stats by state
    for i, table in enumerate(tables):
        df = table.df
        
        # Print some info about current table for debugging
        print(f"\nProcessing Table #{i+1}, Page Range: {table.page}")
        print("Preview of first few rows:")
        print(df.head(3).to_string(index=False))

        # Drop empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')

        # Step: Attempt to detect correct header row
        header_found = False
        for header_idx in range(min(5, len(df))):
            non_numeric_ratio = (
                df.iloc[header_idx].str.contains(r'[A-Za-z]', na=False).sum()
                / len(df.columns)
            )
            if non_numeric_ratio > 0.6:
                df.columns = df.iloc[header_idx].str.strip()
                df = df.drop(index=list(range(header_idx + 1))).reset_index(drop=True)
                header_found = True
                print(f"Header detected and set from row {header_idx}")
                break

        if not header_found:
            print(" Could not detect a valid header row. Skipping this table.")
            continue

        # Clean column names
        df.columns = [col.replace('\n', ' ').strip() for col in df.columns]

        # Check for crime keywords
        if any(keyword in df.values.astype(str) for keyword in ['Dowry Deaths', 'Murder', 'Rape', 'Assault']):
            print(f"Found potential crime-by-state table on Page: {table.page}, Table #{i+1}")

            # Check if 'State/UT' column exists
            if 'State/UT' not in df.columns:
                print(" 'State/UT' column not found. Available columns:")
                print(df.columns.tolist())
                continue

            # Drop empty or invalid rows
            df = df[df['State/UT'].notna() & (df['State/UT'] != '')]

            # Remove non-ASCII characters
            df = df.apply(lambda series: series.astype(str).str.replace(r'[^\x00-\x7F]+', '', regex=True))

            # Reshape the DataFrame to long format
            df_long = df.melt(id_vars=['State/UT'], var_name='CrimeStat', value_name='Value')

            # Split "CrimeStat" into Crime and Stat (I/V/R)
            if df_long['CrimeStat'].str.contains('\n').any():
                df_long[['Crime', 'Stat']] = df_long['CrimeStat'].str.split('\n', expand=True)
            else:
                df_long[['Crime', 'Stat']] = df_long['CrimeStat'].str.rsplit(' ', n=1, expand=True)

            df_long['Crime'] = df_long['Crime'].str.replace(r'\d+[\.\d+]*', '', regex=True).str.strip()

            # Pivot to wide format
            df_wide = df_long.pivot(index='State/UT', columns=['Crime', 'Stat'], values='Value')
            df_wide.columns = [f"{crime}_{stat}" for crime, stat in df_wide.columns]
            df_wide = df_wide.reset_index()

            # Save reshaped data
            clean_file = 'clean_crime_data_by_state.xlsx'
            df_wide.to_excel(clean_file, index=False)
            print(f"\n Cleaned and reshaped data saved to '{clean_file}'")
            print("Columns:", df_wide.columns.tolist())

            crime_table_found = True
            break

    if not crime_table_found:
        print("\n Could not find a valid crime-by-state table.")

    # Export all raw tables for manual inspection
    raw_file = 'output_tables2.xlsx'
    with pd.ExcelWriter(raw_file) as writer:
        for idx, table in enumerate(tables):
            table.df.to_excel(writer, sheet_name=f'Table_{idx+1}', index=False)
    print(f"\n Raw tables exported to '{raw_file}'")

    print("\nFiles in directory after processing:", os.listdir())

except FileNotFoundError:
    print(" Error: The file 'crimes.pdf' was not found.")
except Exception as e:
    print(" An unexpected error occurred:", str(e))