from __future__ import annotations
import sys, json, traceback
from typing import Dict, Any, List

def _get_dbutils():
    try:
        return dbutils                       
    except NameError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return spark._jsparkSession.dbutils()

dbutils = _get_dbutils()

TIMEOUT  = 0
COMMON_ARGS: Dict[str, str] = {
}

NOTEBOOKS: List[str] = [
    "src/01_ingestion_raw",
    "src/02_elt_bronze",
    "src/03_elt_silver",
    "src/04_elt_gold",
]

def run_notebook(path: str, args: Dict[str, Any]) -> None:
    print(f"Iniciando notebook: {path}")
    try:
        result_json = dbutils.notebook.run(path, TIMEOUT, args)
        if result_json:
            res = json.loads(result_json)
            print(f"resultado: {res}")
        print(f"Concluído: {path}\n")
    except Exception as exc:  
        print(f"Falha no notebook {path}\n{traceback.format_exc()}")
        raise

def main() -> None:
    for nb in NOTEBOOKS:
        run_notebook(nb, COMMON_ARGS)

if __name__ == "__main__":
    extra = dict(zip(sys.argv[1::2], sys.argv[2::2])) 
    COMMON_ARGS.update({k.lstrip("-"): v for k, v in extra.items()})
    main()
