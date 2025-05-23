import pandas as pd
from sklearn.preprocessing import StandardScaler

# Inicializar el escalador (puedes exportarlo si necesitas usarlo en otro script)
scaler = StandardScaler()

def load_data(file_path):
    """
    Carga el dataset desde la ruta especificada.
    """
    return pd.read_csv(file_path)

def clean_data(df):
    """
    Elimina columnas innecesarias del dataframe.
    """
    columns_to_drop = [
        'Happiness_Rank', 'Standard_Error', 'Lower_Confidence_Interval',
        'Upper_Confidence_Interval', 'Whisker_High', 'Whisker_Low',
        'Year', 'Dystopia_Residual', 'Happiness Category',
        'Happiness_Level', 'Cluster', 'Region'
    ]
    return df.drop(columns=columns_to_drop, errors='ignore')

def preprocess_features(df):
    """
    Realiza codificación de variables categóricas (por ejemplo, 'Country') 
    y retorna el DataFrame con variables dummy.
    """
    # Verifica que 'Country' está en el DataFrame
    if 'Country' in df.columns:
        # Crea variables dummy para 'Country', eliminando la primera para evitar multicolinealidad
        df_encoded = pd.get_dummies(df, columns=['Country'], drop_first=True)
    else:
        df_encoded = df.copy()  # Si no existe la columna, devuelve el DataFrame original
    
    return df_encoded



