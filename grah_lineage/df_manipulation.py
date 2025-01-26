import pandas as pd

# Sample input DataFrame
data = {
    'src': ['A', 'B', 'C', 'D', 'E'],
    'srcAit': ['A1', 'B1', 'C1', 'D1', 'E1'],
    'srcCol1': [10, 20, 30, 40, 50],
    'tgt': ['F', 'G', 'H', 'A', 'B'],
    'tgtAit': ['F1', 'G1', 'H1', 'A2', 'B2'],
    'tgtCol1': [60, 70, 80, 90, 100],
}
sttm = pd.DataFrame(data)

# Step 1: Extract unique values from src and tgt columns
src_unique = sttm[['src', 'srcAit', 'srcCol1']].rename(columns={'src': 'elem', 'srcAit': 'elemAit', 'srcCol1': 'srcCol'})
tgt_unique = sttm[['tgt', 'tgtAit', 'tgtCol1']].rename(columns={'tgt': 'elem', 'tgtAit': 'elemAit', 'tgtCol1': 'srcCol'})

# Step 2: Combine unique values into one DataFrame
meta = pd.concat([src_unique, tgt_unique]).drop_duplicates().reset_index(drop=True)

# Step 3: Add the `matched_or_not` column based on the given list
matching_list = ['A', 'B', 'C']
meta['matched_or_not'] = meta['elem'].apply(lambda x: 'Yes' if x in matching_list else 'No')

# Display the resulting DataFrame
print("\nMeta DataFrame:")
print(meta)




#######################3

import pandas as pd

# List of lists
data = [
    ['A', 'X', 10],
    ['B', 'Y', 20],
    ['C', 'Z', 30],
    ['D', 'W', 40]
]

# Column names
columns = ['Column1', 'Column2', 'Column3']

# Create DataFrame
df = pd.DataFrame(data, columns=columns)

# Display the DataFrame
print("\nDataFrame created from list of lists:")
print(df)


################################


import pandas as pd

# Separate lists
names = ['Alice', 'Bob', 'Charlie', 'Diana']
ages = [25, 30, 22, 28]
dates_of_birth = ['1998-01-15', '1993-03-22', '2001-07-19', '1995-09-05']

# Create DataFrame
df = pd.DataFrame({
    'Name': names,
    'Age': ages,
    'Date of Birth': dates_of_birth
})

# Display the DataFrame
print("\nDataFrame created from separate lists:")
print(df)
