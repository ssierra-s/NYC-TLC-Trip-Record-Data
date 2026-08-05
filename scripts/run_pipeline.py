"""Runner CLI del pipeline ETL.

Ejemplos:
    python -m scripts.run_pipeline                                  # usa el .env
    python -m scripts.run_pipeline --taxi-type yellow --year 2024 --months 1 2 3
    python -m scripts.run_pipeline --taxi-type green --start 2024-11 --months-count 3
"""

import argparse

from etl.config import TAXI_TYPE, START_YEAR, START_MONTH, MONTHS_TO_PROCESS
from etl.pipeline import build_periods, run_pipeline


def parse_args():
    parser = argparse.ArgumentParser(description="Run NYC Taxi Batch ETL Pipeline")
    parser.add_argument("--taxi-type", type=str, default=TAXI_TYPE,
                        choices=["yellow", "green"], help="Tipo de taxi a procesar")
    parser.add_argument("--year", type=int, default=None,
                        help="Anio a procesar (se combina con --months)")
    parser.add_argument("--months", nargs="+", type=int, default=None,
                        help="Meses de --year a procesar (ej: 1 2 3)")
    parser.add_argument("--start", type=str, default=None,
                        help="Periodo inicial YYYY-MM; con --months-count genera N meses consecutivos")
    parser.add_argument("--months-count", type=int, default=None,
                        help="Cantidad de meses consecutivos a partir de --start")
    return parser.parse_args()


def resolve_periods(args) -> list[tuple[int, int]]:
    """Resuelve los periodos (anio, mes) a procesar.

    Precedencia: --year/--months explicitos > --start/--months-count >
    valores del .env. La generacion de meses consecutivos siempre pasa por
    `build_periods`, que maneja el salto de anio (11, 12 -> 1 del anio siguiente).
    """
    if args.months:
        year = args.year if args.year is not None else START_YEAR
        invalid = [month for month in args.months if not 1 <= month <= 12]
        if invalid:
            raise SystemExit(f"Meses invalidos: {invalid}. Deben estar entre 1 y 12.")
        return [(year, month) for month in args.months]

    if args.start:
        try:
            start_year, start_month = (int(part) for part in args.start.split("-"))
        except ValueError:
            raise SystemExit("Formato invalido en --start. Usa YYYY-MM (ej: 2024-11).")
        return build_periods(start_year, start_month, args.months_count or MONTHS_TO_PROCESS)

    return build_periods(
        args.year if args.year is not None else START_YEAR,
        START_MONTH,
        args.months_count or MONTHS_TO_PROCESS,
    )


if __name__ == "__main__":
    arguments = parse_args()
    run_pipeline(taxi_type=arguments.taxi_type, periods=resolve_periods(arguments))
