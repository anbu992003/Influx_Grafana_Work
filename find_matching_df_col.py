import pandas as pd

# Create a sample DataFrame
data = {
    'src1': ['A', 'B', 'C', 'D'],
    'src2': ['E', 'F', 'G', 'H'],
    'tgt': ['I', 'J', 'K', 'L'],
    'trans1': [1, 2, 3, 4],
    'trans2': [5, 6, 7, 8],
    'trans3': [9, 10, 11, 12]
}
df = pd.DataFrame(data)

# Input list of values
input_list = ['B', 'G', 'X', 'H']

# Check for matches and create a new column 'match_src'
df['match_src'] = df.apply(
    lambda row: 'src1' if row['src1'] in input_list else ('src2' if row['src2'] in input_list else None),
    axis=1
)

# Find input matches in src1 or src2
matched_inputs = set(input_list).intersection(set(df['src1']).union(df['src2']))
non_matching_inputs = set(input_list) - matched_inputs

# Calculate matches in src1 and src2
matches_src1 = sum(df['match_src'] == 'src1')
matches_src2 = sum(df['match_src'] == 'src2')

# Print the counts
print(f"Number of matches in src1: {matches_src1}")
print(f"Number of matches in src2: {matches_src2}")
print(f"Number of input items that have a match in the dataframe: {len(matched_inputs)}")
print(f"Number of input items that have no match in the dataframe: {len(non_matching_inputs)}")

# Display the updated DataFrame
print("\nUpdated DataFrame:")
print(df)
