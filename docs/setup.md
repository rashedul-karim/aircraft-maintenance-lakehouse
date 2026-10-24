# Setup on Databricks Free Edition

About 20 minutes the first time. Everything here is free.

## 1. Create the workspace

1. Sign up at the [Databricks Free Edition page](https://login.databricks.com/?intent=CE_SIGN_UP). Databricks creates a workspace with serverless compute and storage.
2. Open **Catalog** in the left menu and note the catalog name. It is usually `workspace`. If it is different, use that name in the `catalog` box at the top of each notebook.

## 2. Clone this repo into the workspace

1. Go to **Workspace**, then **Create** → **Git folder**.
2. Paste the repo URL: `https://github.com/rashedul-karim/aircraft-maintenance-lakehouse`
3. If Databricks asks you to connect GitHub, install the Databricks GitHub app and choose **Only select repositories**, then pick this repo.

The notebooks import code from `src/`, so they must run from inside this Git folder, not from a single imported notebook.

## 3. Create the schemas and the landing volume

1. Open `notebooks/00_setup` and click **Run all**. Accept serverless compute if asked.
2. It creates `pdm_bronze`, `pdm_silver`, `pdm_gold` and the volume `pdm_bronze.landing`.

## 4. Upload the NASA files

The Free Edition only allows outbound internet to a short list of trusted sites, so the files are uploaded by hand rather than downloaded by the notebook.

1. On your computer, download `CMAPSSData.zip` from the [NASA Open Data Portal](https://data.nasa.gov/dataset/cmapss-jet-engine-simulated-data) and unzip it.
2. In Databricks, open **Catalog** → your catalog → **pdm_bronze** → **Volumes** → **landing**.
3. Click **Upload to this volume** and select these 12 files:
   - `train_FD001.txt` to `train_FD004.txt`
   - `test_FD001.txt` to `test_FD004.txt`
   - `RUL_FD001.txt` to `RUL_FD004.txt`

   The readme and the PDF are not needed.
4. Run the last cell of `00_setup` again. It should list 12 files.

## 5. Run the bronze load

Open `notebooks/01_bronze_ingest` and click **Run all**. It should end with:

```
Rows loaded      : 265,256
Rejected lines   : 0
Test RUL problems: 0
Readme mismatch  : FD004 test: readme says 249, files contain 248
Readme mismatch  : FD004 train: readme says 248, files contain 249
Bronze load OK
```

The two "readme mismatch" lines are expected; see the README's data quality findings.

## Troubleshooting

| Problem | Fix |
|---|---|
| `ModuleNotFoundError: cmapss_parsing` | The notebook is not running from the Git folder. Open it from the cloned repo. |
| `Catalog 'workspace' not found` | Your catalog has another name. Type it in the `catalog` box at the top. |
| `0 files` or path not found | Check the files are in the `landing` volume of `pdm_bronze`, not in a subfolder. |
| Compute stops for the day | Free Edition has a daily usage limit. Work is kept; continue the next day. |

## Running the tests on your own computer

Needs Python 3.10+ and Java 17.

```bash
pip install -r requirements-dev.txt
pytest -q
```
