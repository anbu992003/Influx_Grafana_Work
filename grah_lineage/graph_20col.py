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
    lineage_list = full_lineage.split("|")  # Split by pipe symbol
    model_list = [entry for entry in lineage_list if re.search(r'm\d{4,}', entry)]  # Find models

    results = []

    if not model_list:  # No models found, store original lineage
        results.append(full_lineage)
    else:
        #results.append(f"{top_root}|{'|'.join(lineage_list[:lineage_list.index(model_list[0]) + 1])}")  # First level to first model
        results.append(f"{'|'.join(lineage_list[:lineage_list.index(model_list[0]) + 1])}")  # First level to first model
        for i in range(len(model_list) - 1):  # Between models
            results.append(f"{model_list[i]}|{model_list[i+1]}")
        #results.append(f"{model_list[-1]}|{'|'.join(lineage_list[lineage_list.index(model_list[-1]):])}")  # Last model to last level
        results.append(f"{'|'.join(lineage_list[lineage_list.index(model_list[-1]):])}")  # Last model to last level

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