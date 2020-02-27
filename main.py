import os
import agate
import csv
import warnings
from datetime import datetime
from pathlib import Path
import configparser

import requests
import cchardet as chardet
from agate.warns import UnnamedColumnWarning
from goodtables import validate


def parse_csv(file_path):
    warnings.filterwarnings("ignore", category=UnnamedColumnWarning)
    # deactivate type testing, this puts too much constraint on parsing
    # especially for lat/lon columns with commas
    tester = agate.TypeTester(types=(agate.Text,), limit=0)
    with file_path.open("rb") as f:
        encoding = chardet.detect(f.read()).get("encoding")

    try:
        table = agate.Table.from_csv(
            file_path, encoding=encoding, sniff_limit=None, column_types=tester
        )
    except Exception as e:
        print("❌ CSV parse error for %s (%s)" % (file_path, e))
        return None, None
    else:
        return table, encoding
    finally:
        warnings.resetwarnings()


current_path = (
    Path(os.getenv("WORKING_DIR")) if os.getenv("WORKING_DIR") else Path().absolute()
)

config = configparser.ConfigParser()
config.read("config.ini")
assert "default" in config.sections()
assert "secrets" in config.sections()

domain = config.get("default", "domain")

url = "https://%s/api/1/datasets/?tag=irve&page_size=1000" % domain
r = requests.get(url)
datasets = r.json()["data"]

# make a new directory each DAY the download phase is done
data_path = current_path / "data" / datetime.now().strftime("%Y%m%d")
data_path.mkdir(parents=True, exist_ok=True)

total_resources = 0
dl_resources = 0

downloaded = []
for d in datasets:
    if not d["organization"] and not d["owner"]:
        print("❌", "orphan dataset", d["slug"])
        continue
    # ignore our consolidated dataset
    if d["id"] == config.get("default", "dataset_id"):
        print("⚠️ ignored our own dataset")
        continue
    orga = d["organization"]["slug"] if d["organization"] else d["owner"]["slug"]
    slug = d["slug"]
    for r in d["resources"]:
        total_resources += 1
        rurl = r["url"]
        rid = r["id"]
        # ODS style NB: won't work more than once for CKAN
        if "format=csv" in rurl:
            filename = rurl.split("/")[-3] + ".csv"
        else:
            filename = rurl.split("/")[-1]
        ext = filename.split(".")[-1]
        if ext != "csv":
            print("⚠️ ignored file %s" % rurl)
            continue
        r = requests.get(rurl, allow_redirects=True)
        p = Path(data_path) / slug
        p.mkdir(exist_ok=True)
        written_filename = "%s.%s" % (rid, ext)
        with open("%s/%s" % (p, written_filename), "wb") as f:
            dl_resources += 1
            f.write(r.content)
            downloaded.append(filename)
            print("✅ downloaded file [%s] %s" % (filename, rurl))
print("✅✅✅ Done", total_resources, dl_resources)

for child in [x for x in data_path.iterdir() if x.is_dir()]:
    csvs = list(child.glob("*.csv"))
    for csv_file in csvs:
        table, encoding = parse_csv(csv_file)
        if not table:
            continue
        missing_pivot = []
        for pivot in ["id_station", "id_pdc", "date_maj"]:
            if pivot not in table.column_names:
                missing_pivot.append(pivot)
        if missing_pivot:
            print(f"😨 Missing pivot {csv_file}")
            continue

        try:
            res = validate(
                [{"source": csv_file, "schema": "schema.json", "encoding": encoding}]
            )
        except Exception:
            continue

        if res["valid"]:
            print(f"✅ {csv_file} ({len(table.rows)} rows)")
        else:
            print(f"🛑 {csv_file} ({len(table.rows)} rows)")
