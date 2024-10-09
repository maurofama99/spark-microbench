import os
import pandas as pd
from datetime import timedelta
import random

# Configurazione: intervallo di secondi random da aggiungere
MIN_SECONDS = 0
MAX_SECONDS = 5

# Funzione per aggiungere secondi randomici a un timestamp
def add_random_seconds(timestamp, min_sec=MIN_SECONDS, max_sec=MAX_SECONDS):
    seconds_to_add = random.randint(min_sec, max_sec)
    return timestamp + timedelta(seconds=seconds_to_add)

# Funzione per processare i CSV nella cartella
def process_csv_folder(folder_path, output_folder):
    # Leggi tutti i file CSV nella cartella
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

    for csv_file in csv_files:
        # Leggi il CSV
        file_path = os.path.join(folder_path, csv_file)
        df = pd.read_csv(file_path)

        # Converti la colonna 'ts' in un datetime
        df['ts'] = pd.to_datetime(df['ts'])

        # Aggiungi secondi randomici a ciascun timestamp
        df['ts'] = df['ts'].apply(add_random_seconds)

        # Salva il CSV modificato nella cartella di output
        output_file_path = os.path.join(output_folder, csv_file)
        df.to_csv(output_file_path, index=False)
        print(f'File processato: {csv_file}')

# Imposta il percorso della cartella contenente i CSV e la cartella di output
input_folder = '/home/maurofama/Documents/PolyFlow/phd/OVERT/plots/ooo_evaluation_systems/spark/dataset_io'  # Sostituisci con il percorso della cartella dei CSV
output_folder = '/home/maurofama/spark-microbench/examples/src/main/scala/org/apache/spark/examples/sql/streaming/files/csv_session'  # Sostituisci con il percorso della cartella di output

# Creare la cartella di output se non esiste
os.makedirs(output_folder, exist_ok=True)

# Esegui la funzione per processare tutti i CSV
process_csv_folder(input_folder, output_folder)
