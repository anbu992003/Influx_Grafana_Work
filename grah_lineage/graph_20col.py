import pandas as pd
import re

# Sample Data
data = {
    "full_lineage": [
        "asasfaa~afafa|asssss~reqrrq|aadfaf~aagaga|asdasfa~ggwg",
        "fagdggeg~h4h44h|ju6k66k~7ii7g4trh|rtrhh~r44h44h4h|oliree~2r3grgbrb",
        "fagdggeg~h4h44h|ju6k66k~7ii7g4trh|asfasfa m2432432~wfwfw m3324234|rtrhh~r44h44h4h|wwfwrfw m131413~wfwfwef m31423|oliree~2r3grgbrb",
        "fagdggeg~h4h44h|fafafd #m32432424~fasfsa #m33433|ju6k66k~7ii7g4trh|rtrhh~r44h44h4h|oliree~2r3grgbrb"
    ],
    "top_root": [
        "asasfaa~afafa",
        "fagdggeg~h4h44h",
        "fagdggeg~h4h44h",
        "fagdggeg~h4h44h"
    ]
}

df = pd.DataFrame(data)

# Function to process lineage
def process_lineage(full_lineage, top_root):
    lineage_list = full_lineage.split("|")  # Split lineage by pipe symbol
    model_list = [entry for entry in lineage_list if re.search(r'm\d{4,}', entry)]  # Find models
    
    results = []

    # Store indices
    #indices = list(range(1, len(lineage_list) + 1))
    #results.append(f"Indices: {' | '.join(map(str, indices))}")

    if not model_list:  # No models found, store original lineage
        results.append(f"{full_lineage}| Indices: 1-{len(lineage_list)+1}")
    else:
        # First level to first model
        first_model_idx = lineage_list.index(model_list[0]) + 1
        results.append(f"{top_root}|{'|'.join(lineage_list[:first_model_idx])} | Indices: 1-{first_model_idx}")
        
        # Model lineage transformations
        for i in range(len(model_list) - 1):
            start_idx = lineage_list.index(model_list[i]) + 1
            end_idx = lineage_list.index(model_list[i + 1]) + 1
            results.append(f"{model_list[i]}|{model_list[i + 1]} | Indices: {start_idx}-{end_idx}")
        
        # Last model to last level
        last_model_idx = lineage_list.index(model_list[-1]) + 1
        results.append(f"{model_list[-1]}|{'|'.join(lineage_list[last_model_idx:])} | Indices: {last_model_idx}-{len(lineage_list)}")

    return results

# Apply processing to each row
df["processed_lineage"] = df.apply(lambda row: process_lineage(row["full_lineage"], row["top_root"]), axis=1)

# Flatten results for CSV output
output_data = []
for index, row in df.iterrows():
    for lineage in row["processed_lineage"]:
        output_data.append({"processed_lineage": lineage})

# Save to CSV
output_df = pd.DataFrame(output_data)
output_df.to_csv("processed_lineage.csv", index=False)

print("Processed lineage successfully written to 'processed_lineage.csv'.")