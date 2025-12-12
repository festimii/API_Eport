import pyodbc
import pandas as pd
import math


# =====================================================================
# CLEANING HELPERS
# =====================================================================

def clean_str(value):
    """
    Clean text values: empty, whitespace, nan-like → None.
    """
    if value is None:
        return None

    s = str(value).strip().lower()
    if s in ("", " ", "nan", "na", "n/a", "none"):
        return None

    return str(value).strip()


def clean_float(value):
    """
    Convert cleaned string to float or None.
    """
    s = clean_str(value)
    if s is None:
        return None
    try:
        return float(s.replace(",", "."))
    except:
        return None


def clean_int(value):
    """
    Convert cleaned string to int or None.
    """
    s = clean_str(value)
    if s is None:
        return None
    try:
        return int(float(s))
    except:
        return None


def clean_all(df):
    """
    Clean entire DataFrame aggressively:
    - Removes "", " ", "nan", NaN, None
    """
    return df.applymap(lambda v: clean_str(v))


# =====================================================================
# FINAL SANITIZER FOR SQL PARAMETERS (CRITICAL)
# =====================================================================

def sanitize(params):
    """
    Ensures NOTHING invalid goes to SQL.
    Converts:
      - numpy.nan → None
      - float('nan') → None
      - "" → None
    """
    fixed = []
    for v in params:
        if v is None:
            fixed.append(None)
            continue

        # Fix numpy.nan or float nan
        if isinstance(v, float) and math.isnan(v):
            fixed.append(None)
            continue

        # Fix empty string
        if isinstance(v, str) and v.strip() == "":
            fixed.append(None)
            continue

        fixed.append(v)
    return fixed


# =====================================================================
# PLANOGRAM IMPORTER
# =====================================================================

class PlanogramImporter:

    def __init__(self, excel_path, connection_string):
        self.excel_path = excel_path
        self.connection_string = connection_string

    def load_excel(self):
        df = pd.read_excel(self.excel_path, dtype=str)
        df.columns = [col.strip() for col in df.columns]

        required = ["Internal_ID", "Module_ID", "X", "Y", "Z", "Planogram_ID"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # CLEAN ALL CELLS
        df = clean_all(df)

        # CLEAN NUMERIC COLUMNS
        df["Module_ID"] = df["Module_ID"].apply(clean_int)
        df["X"] = df["X"].apply(clean_float)
        df["Y"] = df["Y"].apply(clean_float)
        df["Z"] = df["Z"].apply(clean_float)

        # DEFAULT Sifra_Art
        if "Sifra_Art" not in df.columns:
            df["Sifra_Art"] = df["Internal_ID"]

        return df

    # -------------------------------------------------------------

    def insert_into_db(self, df: pd.DataFrame):
        conn = pyodbc.connect(self.connection_string)
        cursor = conn.cursor()

        cursor.fast_executemany = True

        update_sql = """
            UPDATE dbo.PlanogramLayout
            SET Module_ID=?, X=?, Y=?, Z=?, Planogram_ID=?
            WHERE Internal_ID=? AND Sifra_Art=?
        """

        insert_sql = """
            INSERT INTO dbo.PlanogramLayout
            (Sifra_Art, Internal_ID, Module_ID, X, Y, Z, Planogram_ID)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        failed = []

        for _, row in df.iterrows():

            # BASE PARAMETERS
            params_update = [
                row["Module_ID"],
                row["X"],
                row["Y"],
                row["Z"],
                row["Planogram_ID"],
                row["Internal_ID"],
                row["Sifra_Art"]
            ]

            params_update = sanitize(params_update)

            try:
                cursor.execute(update_sql, params_update)

                if cursor.rowcount == 0:
                    params_insert = [
                        row["Sifra_Art"],
                        row["Internal_ID"],
                        row["Module_ID"],
                        row["X"],
                        row["Y"],
                        row["Z"],
                        row["Planogram_ID"]
                    ]

                    params_insert = sanitize(params_insert)

                    cursor.execute(insert_sql, params_insert)

            except Exception as e:
                print("\n❌ ERROR ROW DETECTED")
                print("Row:", row.to_dict())
                print("Param Types:", [type(v) for v in params_update])
                print("Param Values:", params_update)
                print("SQL Error:", str(e))

                failed.append({**row.to_dict(), "Error": str(e)})

        conn.commit()
        cursor.close()
        conn.close()

        if failed:
            pd.DataFrame(failed).to_excel("failed_rows.xlsx", index=False)
            print("\n⚠ Exported failed rows to failed_rows.xlsx")

    # -------------------------------------------------------------

    def run(self):
        print("Reading Excel and cleaning data…")
        df = self.load_excel()
        print(f"Loaded {len(df)} rows.")

        print("Importing into SQL Server (bulk mode)…")
        self.insert_into_db(df)

        print("\n✔ Import completed successfully.")


# =====================================================================
# RUN
# =====================================================================


if __name__ == "__main__":
    EXCEL_PATH = r"C:\Users\Administrator\Desktop\PLANOGRAMET_1.xlsx"
    CONNECTION = (
        "DRIVER={SQL Server};"
        "SERVER=192.168.100.10;"
        "DATABASE=wtrgksvf;"
        "UID=festim.beqiri;PWD=Festimeliza123;"
    )

    PlanogramImporter(EXCEL_PATH, CONNECTION).run()
