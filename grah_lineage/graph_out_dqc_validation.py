import pandas as pd

# Simulate the output DataFrame from the previous code for demonstration
data = {
    'path': ['1 -> 2 -> 3 -> 4', '4 -> 5 -> 6', '1 -> 2 -> 5 -> 6', '4 -> 5 -> 8'],
    'levels': [3, 2, 3, 2],
    'start': [1, 4, 1, 4],
    'end': ['3', '6', '6', '8'],
    'level1': ['1', '4', '1', '4'],
    'level2': ['2', '5', '2', '5'],
    'level3': ['3', '6', '5', '8'],
    'level4': ['4', None, None, None],
    'bus_Elem1': ['BE1', 'BE4', 'BE1', 'BE4'],
    'bus_Elem2': ['BE2', 'BE5', 'BE2', 'BE5'],
    'bus_Elem3': ['BE3', 'BE6', 'BE5', 'BE8'],
    'bus_Elem4': ['BE4', None, None, None],
    'pde_Elem1': ['PE1', 'PE4', 'PE1', 'PE4'],
    'pde_Elem2': ['PE2', 'PE5', 'PE2', 'PE5'],
    'pde_Elem3': ['PE3', 'PE6', 'PE5', 'PE8'],
    'pde_Elem4': ['PE4', None, None, None],
    'trans1': ['T1 -> U1', 'T4 -> U4', None, 'T7 -> U7'],
    'trans2': ['T2 -> U2', None, None, 'T8 -> U8'],
}
df = pd.DataFrame(data)

# Domain values for the `end` column validation
domain_values = ['A', 'B', 'C']

# Rule 1: Validate if bus_Elem1 to bus_Elem n is not null
bus_elem_columns = [col for col in df.columns if col.startswith('bus_Elem')]
null_counts_bus_elem = df[bus_elem_columns].isnull().sum().sum()
not_null_counts_bus_elem = df[bus_elem_columns].notnull().sum().sum()
print(f"Rule 1: Null count in bus_Elem columns: {null_counts_bus_elem}, Not null count: {not_null_counts_bus_elem}")

# Rule 2: Validate if trans1 to trans2 is null/blank and set to "Not Applicable"
trans_columns = [col for col in df.columns if col.startswith('trans')]
df[trans_columns] = df[trans_columns].fillna("Not Applicable")
print(f"Rule 2: Updated 'trans' columns where null/blank values are set to 'Not Applicable'.")

# Rule 3: Validate if values in the end column are in the domain list
end_in_domain = df['end'].isin(domain_values).sum()
end_not_in_domain = (~df['end'].isin(domain_values)).sum()
print(f"Rule 3: Count of 'end' in domain: {end_in_domain}, not in domain: {end_not_in_domain}")

# Rule 4: Validate if bus_Elem1 to bus_Elem n is not the same as pde_Elem1 to pde_Elem n
bus_pde_columns = zip(bus_elem_columns, [col.replace('bus_Elem', 'pde_Elem') for col in bus_elem_columns])
match_count = sum((df[bus] == df[pde]).sum() for bus, pde in bus_pde_columns)
non_match_count = sum((df[bus] != df[pde]).sum() for bus, pde in bus_pde_columns)
print(f"Rule 4: Match count between bus_Elem and pde_Elem columns: {match_count}, Non-match count: {non_match_count}")

# Rule 5: If level1 is A and either level2 or level3 is B, then level4 should not be null
condition = (df['level1'] == 'A') & ((df['level2'] == 'B') | (df['level3'] == 'B'))
null_count_level4 = df.loc[condition, 'level4'].isnull().sum()
not_null_count_level4 = df.loc[condition, 'level4'].notnull().sum()
print(f"Rule 5: Null count in level4 under condition: {null_count_level4}, Not null count: {not_null_count_level4}")

# Output the DataFrame to ensure updates are applied
output_file = "validated_output.csv"
df.to_csv(output_file, index=False)
print(f"\nValidated DataFrame written to {output_file}")
