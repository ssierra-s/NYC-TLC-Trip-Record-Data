"""Descarga por lotes de los Parquet publicados por la NYC TLC (Capa Bronce)."""

from pathlib import Path

import requests

# CDN oficial al que apunta el portal de la TLC. Se descarga de aqui y no se
# hace scraping del HTML: la URL es predecible y estable
# (<tipo>_tripdata_<anio>-<mes>.parquet), lo que permite iterar meses de forma
# programatica sin depender de cambios en la maqueta de la pagina.
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DOWNLOAD_TIMEOUT_SECONDS = 120
STREAM_CHUNK_BYTES = 1024 * 1024  # 1 MB


def build_file_name(taxi_type: str, year: int, month: int) -> str:
    return f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"


def download_month(taxi_type: str, year: int, month: int,
                   output_directory: str = "data/bronze") -> Path:
    """Descarga (o reutiliza) el Parquet de un mes.

    - Streaming a disco por bloques de 1 MB: nunca se carga el archivo completo
      (~50-500 MB) en memoria.
    - Idempotente: si el archivo ya existe, no se vuelve a bajar.
    - Escritura atomica: se baja a un `.part` y solo al terminar se renombra. Sin
      esto, una descarga cortada dejaria un Parquet truncado que la siguiente
      ejecucion daria por bueno y saltaria.
    """
    file_name = build_file_name(taxi_type, year, month)
    url = f"{BASE_URL}/{file_name}"
    output_path = Path(output_directory) / file_name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        print(f"Archivo {file_name} ya existe. Saltando descarga.")
        return output_path

    temp_path = output_path.with_suffix(output_path.suffix + ".part")
    print(f"Descargando {url} de {output_path}...")
    try:
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
            response.raise_for_status()
            with open(temp_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=STREAM_CHUNK_BYTES):
                    if chunk:
                        file.write(chunk)
        temp_path.replace(output_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise

    print(f"Descarga exitosa de {file_name}")
    return output_path
