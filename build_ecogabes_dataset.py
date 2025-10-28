import argparse
import pandas as pd
import numpy as np
from datetime import datetime
import os

SOIL_SCALE = {'Pb': (0.1, 0.4), 'Hg': (0.01, 0.05), 'Cd': (0.005, 0.02)} 
WATER_SCALE = {'Cu': (0.01, 0.05), 'Zn': (0.05, 0.2)} 
HEALTH_SENSITIVITY = {'PM2.5': 0.02, 'NO2': 0.015, 'SO2': 0.01}  
ECO_RISK_WEIGHTS = {'CO2': 0.2, 'SO2': 0.3, 'PM2.5': 0.5}
RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True, help='Chemin vers le CSV air+health (Kaggle)')
    p.add_argument('--output', default='EcoGabesAI_dataset.csv', help='Chemin fichier de sortie')
    return p.parse_args()

def standardize_columns(df):
    df.columns = [c.strip() for c in df.columns]
    return df

def ensure_timestamp(df, ts_col_candidates=['timestamp','date','datetime','time']):
    for c in ts_col_candidates:
        if c in df.columns:
            df['timestamp'] = pd.to_datetime(df[c])
            return df
    df['timestamp'] = pd.date_range(end=pd.Timestamp.now(), periods=len(df), freq='H')
    return df

def normalize_units(df):
    for col in df.columns:
        if col.lower() in ['co2','no2','so2','pm2.5','pm25','pm10','benzene','o3','co']:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                pass
    return df

def generate_soil_pollution(df):
    so2 = df.get('SO2') if 'SO2' in df else df.get('so2')
    pm25 = df.get('PM2.5') if 'PM2.5' in df else df.get('PM25') if 'PM25' in df else None
    base = pd.Series(0, index=df.index, dtype=float)
    if so2 is not None:
        base = base + so2.fillna(0) * 0.1
    if pm25 is not None:
        base = base + pm25.fillna(0) * 0.05
    df['soil_Pb_mgkg'] = base * np.random.uniform(SOIL_SCALE['Pb'][0], SOIL_SCALE['Pb'][1], size=len(df))
    df['soil_Hg_mgkg'] = base * np.random.uniform(SOIL_SCALE['Hg'][0], SOIL_SCALE['Hg'][1], size=len(df))
    df['soil_Cd_mgkg'] = base * np.random.uniform(SOIL_SCALE['Cd'][0], SOIL_SCALE['Cd'][1], size=len(df))
    return df

def generate_water_pollution(df):
    no2 = df.get('NO2') if 'NO2' in df else df.get('no2')
    so2 = df.get('SO2') if 'SO2' in df else df.get('so2')
    base = pd.Series(0, index=df.index, dtype=float)
    if no2 is not None:
        base = base + no2.fillna(0) * 0.02
    if so2 is not None:
        base = base + so2.fillna(0) * 0.01
    df['water_Cu_mgL'] = base * np.random.uniform(WATER_SCALE['Cu'][0], WATER_SCALE['Cu'][1], size=len(df))
    df['water_Zn_mgL'] = base * np.random.uniform(WATER_SCALE['Zn'][0], WATER_SCALE['Zn'][1], size=len(df))
    return df

def compute_eco_risk_index(df):
    score = pd.Series(0, index=df.index, dtype=float)
    for k,w in ECO_RISK_WEIGHTS.items():
        if k in df.columns:
            score += df[k].fillna(0) * w
    if score.max() > 0:
        score = 100 * (score - score.min()) / (score.max() - score.min())
    df['eco_risk_index'] = score
    return df

def compute_health_risk_and_diseases(df):
    score = pd.Series(0, index=df.index, dtype=float)
    for pol, sens in HEALTH_SENSITIVITY.items():
        if pol in df.columns:
            score += df[pol].fillna(0) * sens
    if score.max() > 0:
        score = 100 * (score - score.min()) / (score.max() - score.min())
    df['health_risk_score'] = score
    df['p_asthma'] = (df['health_risk_score'] / 100) * 0.12  
    df['p_cvd'] = (df['health_risk_score'] / 100) * 0.08     
    df['p_lung_cancer'] = (df['health_risk_score'] / 100) * 0.02
    if 'population' in df.columns:
        df['est_asthma_cases'] = (df['p_asthma'] * df['population']).round().astype('Int64')
    return df

def detect_peaks(df, pollutant='PM2.5', window=24, z_thresh=2.5):
    s = df.get(pollutant)
    if s is None:
        return df
    rm = s.rolling(window=window, min_periods=1).mean()
    resid = s - rm
    mu = resid.mean()
    sigma = resid.std(ddof=0)
    df[f'{pollutant}_is_peak'] = ((resid - mu) / (sigma if sigma>0 else 1)).abs() > z_thresh
    return df

def main():
    args = parse_args()
    if not os.path.exists(args.input):
        print("Fichier d'entrée non trouvé:", args.input)
        return
    df = pd.read_csv(args.input)
    df = standardize_columns(df)
    df = ensure_timestamp(df)
    df = normalize_units(df)
    df = generate_soil_pollution(df)
    df = generate_water_pollution(df)
    df = compute_eco_risk_index(df)
    df = compute_health_risk_and_diseases(df)

    if 'PM2.5' in df.columns or 'PM25' in df.columns:
        key = 'PM2.5' if 'PM2.5' in df.columns else 'PM25'
        df = detect_peaks(df, pollutant=key)
    if 'NO2' in df.columns:
        df = detect_peaks(df, pollutant='NO2')
    df.to_csv(args.output, index=False)
    print("Fichier enrichi généré:", args.output)

if __name__ == "__main__":
    main()
