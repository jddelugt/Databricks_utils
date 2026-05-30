# =============================================================================
# CONFIGURATIE — alle instellingen op één centrale plek
# =============================================================================

CONFIG = {
    # Waarden die als 'leeg/null' worden beschouwd (case-insensitive)
    "null_like_values": {"", "null", "none", "nan", "n/a", "na", "nvt", "n.v.t.", "unknown"},

    # Drempel voor categorisch: kolom is categorisch als
    # (aantal unieke niet-null waarden / totaal niet-null waarden) < drempel
    "categorical_threshold": 0.05,

    # Minimum aantal rijen om categorisch te overwegen
    "categorical_min_rows": 50,

    # Drempel voor typeconflict-waarschuwing:
    # als meer dan dit percentage niet-null waarden NIET het dominante type zijn → waarschuwing
    "conflict_threshold": 0.02,  # 2%

    # Datumformaten om te proberen (volgorde is belangrijk: meest specifiek eerst)
    "date_formats": [
        "yyyy-MM-dd",
        "dd-MM-yyyy",
        "MM/dd/yyyy",
        "dd/MM/yyyy",
        "yyyyMMdd",
        "dd.MM.yyyy",
        "yyyy/MM/dd",
    ],

    # Datetime-formaten
    "datetime_formats": [
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm",
        "dd-MM-yyyy HH:mm:ss",
        "MM/dd/yyyy HH:mm:ss",
    ],

    # Boolean-varianten (alles lowercase)
    "boolean_true_values":  {"true", "ja", "yes", "1", "y", "j"},
    "boolean_false_values": {"false", "nee", "no", "0", "n"},

    # Aantal rijen om te samplen voor de analyse (None = alle rijen)
    "sample_size": 10_000,
}

# =============================================================================
# IMPORTS
# =============================================================================

import json
import re
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# =============================================================================
# HULPFUNCTIES — type-testers op Python-niveau (werken op een sample)
# =============================================================================

def _is_null_like(value: str) -> bool:
    """Geeft True als de waarde als leeg/null beschouwd moet worden."""
    return str(value).strip().lower() in CONFIG["null_like_values"]


def _is_integer(value: str) -> bool:
    try:
        int(value.strip())
        return True
    except (ValueError, AttributeError):
        return False


def _is_float(value: str) -> bool:
    try:
        float(value.strip().replace(",", "."))
        return True
    except (ValueError, AttributeError):
        return False


def _is_boolean(value: str) -> bool:
    v = str(value).strip().lower()
    return v in CONFIG["boolean_true_values"] | CONFIG["boolean_false_values"]


def _is_date(value: str) -> Optional[str]:
    """Geeft het datumformaat terug als de waarde een datum is, anders None."""
    for fmt in CONFIG["date_formats"]:
        py_fmt = (
            fmt.replace("yyyy", "%Y")
               .replace("MM", "%m")
               .replace("dd", "%d")
        )
        try:
            datetime.strptime(value.strip(), py_fmt)
            return fmt
        except (ValueError, AttributeError):
            pass
    return None


def _is_datetime(value: str) -> Optional[str]:
    """Geeft het datetime-formaat terug als de waarde een datetime is, anders None."""
    for fmt in CONFIG["datetime_formats"]:
        py_fmt = (
            fmt.replace("yyyy", "%Y")
               .replace("MM", "%m")
               .replace("dd", "%d")
               .replace("HH", "%H")
               .replace("mm", "%M")
               .replace("ss", "%S")
               .replace("'T'", "T")
               .replace(".SSS", "")
        )
        try:
            datetime.strptime(value.strip().split(".")[0], py_fmt)
            return fmt
        except (ValueError, AttributeError):
            pass
    return None


def _is_json(value: str) -> bool:
    v = value.strip()
    if not (v.startswith("{") or v.startswith("[")):
        return False
    try:
        json.loads(v)
        return True
    except (ValueError, AttributeError):
        return False


# =============================================================================
# KERNFUNCTIE — analyseer één kolom op basis van een sample
# =============================================================================

def _analyze_column(column_name: str, values: list) -> dict:
    """
    Analyseert een lijst van ruwe string-waarden voor één kolom.
    Geeft een dict terug met type-scores en metadata.
    """
    total = len(values)
    if total == 0:
        return {"dominant_type": "string", "format": None, "conflict": False,
                "null_pct": 1.0, "conflict_pct": 0.0, "categorical": False}

    null_count = sum(1 for v in values if _is_null_like(str(v)))
    non_null_values = [str(v) for v in values if not _is_null_like(str(v))]
    non_null_count = len(non_null_values)
    null_pct = null_count / total

    if non_null_count == 0:
        return {"dominant_type": "string", "format": None, "conflict": False,
                "null_pct": null_pct, "conflict_pct": 0.0, "categorical": False}

    # --- Type-scores tellen ---
    scores = {"integer": 0, "float": 0, "boolean": 0,
              "date": 0, "datetime": 0, "json": 0, "string": 0}
    date_formats_found = {}
    datetime_formats_found = {}

    for v in non_null_values:
        if _is_boolean(v):
            scores["boolean"] += 1
        elif _is_integer(v):
            scores["integer"] += 1
        elif _is_float(v):
            scores["float"] += 1
        else:
            dt_fmt = _is_datetime(v)
            if dt_fmt:
                scores["datetime"] += 1
                datetime_formats_found[dt_fmt] = datetime_formats_found.get(dt_fmt, 0) + 1
            else:
                d_fmt = _is_date(v)
                if d_fmt:
                    scores["date"] += 1
                    date_formats_found[d_fmt] = date_formats_found.get(d_fmt, 0) + 1
                elif _is_json(v):
                    scores["json"] += 1
                else:
                    scores["string"] += 1

    # --- Dominant type bepalen (hoogste score wint) ---
    dominant_type = max(scores, key=scores.get)
    dominant_count = scores[dominant_type]
    conflict_count = non_null_count - dominant_count
    conflict_pct = conflict_count / non_null_count

    # --- Meest voorkomend formaat bepalen (voor date/datetime) ---
    chosen_format = None
    if dominant_type == "date" and date_formats_found:
        chosen_format = max(date_formats_found, key=date_formats_found.get)
    elif dominant_type == "datetime" and datetime_formats_found:
        chosen_format = max(datetime_formats_found, key=datetime_formats_found.get)

    # --- Categorisch check ---
    unique_count = len(set(non_null_values))
    is_categorical = (
        dominant_type == "string"
        and total >= CONFIG["categorical_min_rows"]
        and (unique_count / non_null_count) < CONFIG["categorical_threshold"]
    )

    conflict = conflict_pct > CONFIG["conflict_threshold"]

    return {
        "dominant_type": dominant_type,
        "format": chosen_format,
        "conflict": conflict,
        "conflict_pct": conflict_pct,
        "null_pct": null_pct,
        "categorical": is_categorical,
        "scores": scores,
    }


# =============================================================================
# CODEGENERATIE — zet analyse-resultaat om naar PySpark cast-code
# =============================================================================

def _generate_cast_line(column_name: str, analysis: dict) -> str:
    """Genereert één withColumn-regel op basis van de analyse."""
    t = analysis["dominant_type"]
    fmt = analysis["format"]
    col_ref = f'"{column_name}"'
    col_expr = f'F.col("{column_name}")'

    lines = []

    # Waarschuwing als er typeconflicten zijn
    if analysis["conflict"]:
        pct = round(analysis["conflict_pct"] * 100, 1)
        lines.append(
            f"    # ⚠️  WAARSCHUWING: kolom '{column_name}' heeft {pct}% conflicterende waarden "
            f"naast het dominante type '{t}'. Controleer handmatig."
        )

    # Cast-expressie per type
    if t == "integer":
        cast_expr = f"{col_expr}.cast(T.IntegerType())"
    elif t == "float":
        cast_expr = f"{col_expr}.cast(T.DoubleType())"
    elif t == "boolean":
        cast_expr = f"{col_expr}.cast(T.BooleanType())"
    elif t == "date":
        cast_expr = f'F.to_date({col_expr}, "{fmt}")'
    elif t == "datetime":
        cast_expr = f'F.to_timestamp({col_expr}, "{fmt}")'
    elif t == "json":
        cast_expr = f"F.from_json({col_expr}, schema_{column_name})"
        lines.append(
            f"    # ℹ️  JSON-kolom '{column_name}': definieer 'schema_{column_name}' "
            f"handmatig als StructType voor correcte parsing."
        )
    elif analysis["categorical"]:
        cast_expr = f"{col_expr}.cast(T.StringType())  # categorisch"
    else:
        cast_expr = f"{col_expr}.cast(T.StringType())"

    lines.append(f'    df = df.withColumn({col_ref}, {cast_expr})')
    return "\n".join(lines)


# =============================================================================
# HOOFDFUNCTIE — publieke API
# =============================================================================

def generate_cast_code(df: DataFrame) -> str:
    """
    Analyseert alle kolommen van een DataFrame en genereert
    kant-en-klare PySpark cast-code.

    Parameters
    ----------
    df : DataFrame
        De te analyseren PySpark DataFrame (alle kolommen als string).

    Returns
    -------
    str
        Een string met gegenereerde PySpark-code die je direct kunt
        overnemen in je pipeline.
    """
    sample_size = CONFIG["sample_size"]

    # Sample ophalen (collect naar driver voor Python-analyse)
    if sample_size and df.count() > sample_size:
        sample_df = df.sample(fraction=sample_size / df.count(), seed=42).limit(sample_size)
    else:
        sample_df = df

    rows = sample_df.collect()
    columns = df.columns

    # Per kolom analyseren
    analyses = {}
    for col_name in columns:
        values = [row[col_name] for row in rows]
        analyses[col_name] = _analyze_column(col_name, values)

    # Code genereren
    cast_lines = []
    for col_name in columns:
        cast_lines.append(_generate_cast_line(col_name, analyses[col_name]))

    code_body = "\n".join(cast_lines)

    output = f"""\
# =============================================================================
# GEGENEREERDE CAST-CODE — automatisch gegenereerd door generate_cast_code()
# Controleer waarschuwingen (⚠️) handmatig voordat je deze code gebruikt.
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql import types as T


def cast_dataframe(df):
{code_body}
    return df
"""
    return output


# =============================================================================
# GEBRUIK — voorbeeld
# =============================================================================

# generated_code = generate_cast_code(df)
# print(generated_code)
#
# Plak de gegenereerde cast_dataframe()-functie daarna in je pipeline.
