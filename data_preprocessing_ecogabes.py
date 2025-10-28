import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

input_file = "EcoGabesAI_dataset.csv"
output_clean_file = "EcoGabesAI_clean.csv"
figures_dir = "figures"
os.makedirs(figures_dir, exist_ok=True)

df = pd.read_csv(input_file)
colonnes_inutiles = [col for col in df.columns if 'Unnamed' in col or 'id' in col.lower()]
df.drop(columns=colonnes_inutiles, inplace=True, errors='ignore')
df.drop_duplicates(inplace=True)
df.fillna(df.mean(numeric_only=True), inplace=True)
colonnes_num = df.select_dtypes(include=[np.number]).columns
df[colonnes_num] = (df[colonnes_num] - df[colonnes_num].min()) / (df[colonnes_num].max() - df[colonnes_num].min())

for col in colonnes_num:
    plt.figure(figsize=(6, 4))
    sns.histplot(df[col], kde=True, bins=30)
    plt.title(f"Distribution de {col}")
    plt.tight_layout()
    plt.savefig(f"{figures_dir}/hist_{col}.png")
    plt.close()

for col in colonnes_num:
    plt.figure(figsize=(6, 4))
    sns.boxplot(x=df[col])
    plt.title(f"Détection d'anomalies pour {col}")
    plt.tight_layout()
    plt.savefig(f"{figures_dir}/box_{col}.png")
    plt.close()

plt.figure(figsize=(10, 8))
sns.heatmap(df[colonnes_num].corr(), cmap="coolwarm", annot=False)
plt.title("Matrice de corrélation entre les variables")
plt.tight_layout()
plt.savefig(f"{figures_dir}/correlation_matrix.png")
plt.close()

z_scores = np.abs(stats.zscore(df[colonnes_num]))
threshold = 3
anomalies = (z_scores > threshold)
df["anomaly_score"] = anomalies.sum(axis=1)
df_anomalies = df[df["anomaly_score"] > 0]
df_anomalies.to_csv("EcoGabesAI_anomalies.csv", index=False)

df.to_csv(output_clean_file, index=False)
