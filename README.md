# Databricks Utils

Centrale bibliotheek met herbruikbare hulpfuncties voor Databricks-pipelines.

## Structuur

## Gebruik

Voeg bovenaan je Databricks-notebook het volgende toe:

```python
import sys
sys.path.append("/Workspace/Repos/<jouw-gebruiker>/Databricks_utils")

from utils.infer_column_types import generate_cast_code
```

Vervang `<jouw-gebruiker>` door je Databricks-gebruikersnaam.

## Functies

### `generate_cast_code(df)`
Analyseert alle kolommen van een PySpark DataFrame en genereert kant-en-klare cast-code.

**Gebruik:**
```python
generated_code = generate_cast_code(df)
print(generated_code)
```

**Output:** Een Python-string met een `cast_dataframe()`-functie die je direct kunt overnemen in je pipeline.
