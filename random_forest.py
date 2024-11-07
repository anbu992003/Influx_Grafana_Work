import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from sklearn.tree import plot_tree
import numpy as np

# Load the CSV data
df = pd.read_csv('your_data.csv')

# Check for any missing values and handle them as needed
df.dropna(inplace=True)  # For simplicity, we drop missing values

# Define features and target variable
X = df.drop(columns=['target'])  # Replace 'target' with your actual target column name
y = df['target']

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Initialize the Random Forest classifier
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Calculate accuracy
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)

# Generate a classification report
print("Classification Report:\n", classification_report(y_test, y_pred))

# Confusion matrix visualization
conf_matrix = confusion_matrix(y_test, y_pred)
plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues', xticklabels=np.unique(y), yticklabels=np.unique(y))
plt.xlabel("Predicted")
plt.ylabel("Actual")
plt.title("Confusion Matrix")
plt.show()

# Visualize accuracy score
plt.figure(figsize=(6, 4))
sns.barplot(x=['Accuracy'], y=[accuracy], palette='viridis')
plt.ylim(0, 1)
plt.title("Model Accuracy")
plt.ylabel("Accuracy Score")
plt.show()

# Visualize one of the decision trees in the random forest
plt.figure(figsize=(20, 10))
tree = model.estimators_[0]  # Get one tree from the forest
plot_tree(tree, feature_names=X.columns, class_names=str(model.classes_), filled=True, rounded=True)
plt.title("Decision Tree Structure from Random Forest")
plt.show()
