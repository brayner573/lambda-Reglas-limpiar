import boto3
import os
import csv
import json
import tempfile
from datetime import datetime

s3 = boto3.client('s3')

def apply_rules(row):
    try:
        # Regla 1: ID numérico positivo
        row['id'] = int(row['id'])
        if row['id'] <= 0:
            return None
    except:
        return None

    # Regla 2: Campos críticos no nulos
    if not row.get('fecha_not') or not row.get('clasificacion'):
        return None

    # Regla 3: Uppercase en campos clave
    for key in ['diresa', 'red', 'microred', 'establecimiento', 'institucion', 'clasificacion']:
        if row.get(key):
            row[key] = row[key].strip().upper()

    # Regla 4: Valor por defecto para asintomático
    row['asintomatico'] = row.get('asintomatico', 'NO ESPECIFICADO').strip().upper()

    # Regla 5: Validar y formatear fecha
    try:
        fecha = datetime.strptime(row['fecha_not'], "%m/%d/%Y")
        if fecha > datetime.now():
            return None
        row['fecha_not'] = fecha.strftime("%Y-%m-%d")
    except:
        return None

    # Regla 6: Año y semana como enteros
    try:
        row['ano'] = int(row['ano'])
        row['semana'] = int(row['semana'])
    except:
        return None

    # Regla 7: Semana válida (1–53)
    if row['semana'] < 1 or row['semana'] > 53:
        return None

    # Regla 8: Clasificación permitida
    if row['clasificacion'] not in ['CONFIRMADO', 'DESCARTADO', 'SOSPECHOSO']:
        return None

    # Regla 9: Limpiar espacios en 'institucion'
    row['institucion'] = row['institucion'].replace("  ", " ").strip()

    # Regla 10: Excluir establecimiento inválido
    if "SIN DATO" in row['establecimiento']:
        return None

    # Regla 11: Crear campo anio_semana
    row['anio_semana'] = f"{row['ano']}-S{str(row['semana']).zfill(2)}"

    # Reglas 12–20: Limpieza adicional
    for key in ['microred', 'establecimiento']:
        row[key] = row.get(key, '').replace('"', '').replace("'", "").strip()

    return row

def lambda_handler(event, context):
    input_bucket = os.environ['INPUT_BUCKET']
    output_bucket = os.environ['OUTPUT_BUCKET']

    print(f"Event received: {event}")
    print(f"Input bucket: {input_bucket}, Output bucket: {output_bucket}")

    for record in event['Records']:
        key = record['s3']['object']['key']
        print(f"Processing file: {key}")

        if not key.endswith('.csv'):
            print("Skipped non-CSV file")
            continue

        with tempfile.TemporaryDirectory() as tmp:
            local_csv = os.path.join(tmp, 'input.csv')
            local_json = os.path.join(tmp, 'output.json')

            # Descargar archivo CSV desde S3
            s3.download_file(input_bucket, key, local_csv)
            print(f"Downloaded file to {local_csv}")

            # Procesar CSV y aplicar reglas
            cleaned_data = []
            with open(local_csv, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    clean = apply_rules(row)
                    if clean:
                        cleaned_data.append(clean)

            print(f"Rows after cleaning: {len(cleaned_data)}")

            # Guardar JSON procesado
            with open(local_json, 'w', encoding='utf-8') as f:
                json.dump(cleaned_data, f, ensure_ascii=False)

            # Subir archivo JSON al bucket de salida
            output_key = key.replace('uploads/', 'processed/').replace('.csv', '.json')
            print(f"Uploading cleaned file to: {output_key}")
            s3.upload_file(local_json, output_bucket, output_key)

    print("Lambda completed successfully")
    return {'status': 'success'}
