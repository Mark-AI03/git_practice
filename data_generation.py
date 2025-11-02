import os
import random
import pandas as pd


def generate_raw_car_equipment_data(rows: int = 30, seed: int = 7) -> pd.DataFrame:
    """Return a deliberately messy car-equipment DataFrame for ETL practice."""
    rng = random.Random(seed)

    models_by_make = {
        "Toyota": ["Corolla", "Camry", "RAV4", "Prius"],
        "Honda": ["Civic", "Accord", "CR-V", "Pilot"],
        "Ford": ["Focus", "Fusion", "Escape", "Explorer"],
        "BMW": ["320i", "X5", "X3", "M3"],
        "Tesla": ["Model S", "Model 3", "Model X", "Model Y"],
    }
    trims = ["Base", "Sport", "Premium", "Limited"]
    colors = ["Red", "Blue", "Black", "White", "Silver", "Gray"]
    transmissions = ["automatic", "manual", "CVT", "dual clutch"]
    fuels = ["gasoline", "diesel", "hybrid", "electric"]
    infotainment = ["Basic Audio", "Touchscreen", "Premium Audio", "Navigation"]

    duplicate_rows_needed = max(2, rows // 6)
    base_rows = rows - duplicate_rows_needed

    data = []
    for idx in range(base_rows):
        make = rng.choice(list(models_by_make.keys()))
        model = rng.choice(models_by_make[make])
        msrp = rng.randrange(22000, 70000, 500)

        row = {
            "car_id": f"C{idx+1:03}",
            "make": make,
            "model": model,
            "model_year": rng.choice(range(2013, 2024)),
            "trim_level": rng.choice(trims),
            "exterior_color": rng.choice(colors),
            "transmission": rng.choice(transmissions),
            "fuel_type": rng.choice(fuels),
            "infotainment_system": rng.choice(infotainment),
            "msrp": msrp,
        }

        # Inject common data-quality issues.
        if rng.random() < 0.20:
            row["model"] = f" {row['model']}  "                     # extra spaces
        if rng.random() < 0.18:
            row["exterior_color"] = None                            # nulls
        if rng.random() < 0.15:
            row["model_year"] = str(row["model_year"])              # mixed types
        if rng.random() < 0.12:
            row["msrp"] = f"${msrp:,}"                              # string numbers
        if rng.random() < 0.10:
            row["msrp"] = "N/A"                                     # nonnumeric placeholder
        if rng.random() < 0.14:
            row["trim_level"] = rng.choice(
                ["standart", "Premuim", "luxary", row["trim_level"]]
            )                                                       # misspellings
        if rng.random() < 0.12:
            row["transmission"] = rng.choice(
                ["Automtic", "manuall", "automatic ", "AUTOMATIC"]
            )                                                       # inconsistent casing/spelling
        if rng.random() < 0.10:
            row["fuel_type"] = rng.choice(["gasolen", "deisel", row["fuel_type"]])
        if rng.random() < 0.08:
            row["infotainment_system"] = None

        data.append(row)

    for _ in range(duplicate_rows_needed):
        duplicate_row = rng.choice(data)
        data.append(duplicate_row.copy())  # intentional duplicate values

    rng.shuffle(data)
    return pd.DataFrame(data).reset_index(drop=True)


if __name__ == "__main__":
    df = generate_raw_car_equipment_data()
    print(df.head())
    output_dir = "generated_data"
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(os.path.join(output_dir, "raw_car_equipment.csv"), index=False)
