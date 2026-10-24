# Data dictionary

## Source files

The NASA C-MAPSS files are plain text: one row per engine per operating cycle, 26 numbers separated by spaces, with trailing spaces at the end of each line.

| File | Meaning |
|---|---|
| `train_FD00x.txt` | Engines run until failure. The last cycle of each engine is its failure point. |
| `test_FD00x.txt` | Engines stopped some time before failure. |
| `RUL_FD00x.txt` | One number per line: the true remaining cycles after the last test cycle. Line N belongs to test engine N. |

## pdm_bronze.engine_cycles

One row per engine per cycle, as delivered, with types set. Grain: (`dataset`, `split`, `unit_id`, `cycle`).

| Column | Type | Description |
|---|---|---|
| `dataset` | string | Subset: FD001 to FD004 |
| `split` | string | `train` or `test` |
| `source_file` | string | File the row came from |
| `ingested_at` | timestamp | When the row was loaded |
| `unit_id` | int | Engine number, unique within dataset and split |
| `cycle` | int | Operating cycle, starting at 1 |
| `op_setting_1` to `op_setting_3` | double | Operating settings (altitude, Mach number, throttle angle) |
| `sensor_01` to `sensor_21` | double | Sensor readings, described below |

Engine numbers restart in every file, so engine 1 in `train_FD001` and engine 1 in `test_FD001` are different engines. Always join on `dataset`, `split` and `unit_id` together.

### Sensors

Names as given in Table 2 of Saxena et al. (2008). The data files themselves do not label the columns; mapping them in the order of that table is the convention used across the literature.

| Column | Symbol | Description | Unit |
|---|---|---|---|
| `sensor_01` | T2 | Total temperature at fan inlet | °R |
| `sensor_02` | T24 | Total temperature at LPC outlet | °R |
| `sensor_03` | T30 | Total temperature at HPC outlet | °R |
| `sensor_04` | T50 | Total temperature at LPT outlet | °R |
| `sensor_05` | P2 | Pressure at fan inlet | psia |
| `sensor_06` | P15 | Total pressure in bypass duct | psia |
| `sensor_07` | P30 | Total pressure at HPC outlet | psia |
| `sensor_08` | Nf | Physical fan speed | rpm |
| `sensor_09` | Nc | Physical core speed | rpm |
| `sensor_10` | epr | Engine pressure ratio (P50/P2) | – |
| `sensor_11` | Ps30 | Static pressure at HPC outlet | psia |
| `sensor_12` | phi | Ratio of fuel flow to Ps30 | pps/psi |
| `sensor_13` | NRf | Corrected fan speed | rpm |
| `sensor_14` | NRc | Corrected core speed | rpm |
| `sensor_15` | BPR | Bypass ratio | – |
| `sensor_16` | farB | Burner fuel-air ratio | – |
| `sensor_17` | htBleed | Bleed enthalpy | – |
| `sensor_18` | Nf_dmd | Demanded fan speed | rpm |
| `sensor_19` | PCNfR_dmd | Demanded corrected fan speed | rpm |
| `sensor_20` | W31 | HPT coolant bleed | lbm/s |
| `sensor_21` | W32 | LPT coolant bleed | lbm/s |

LPC/HPC = low/high-pressure compressor, LPT/HPT = low/high-pressure turbine.

## pdm_bronze.test_true_rul

Grain: (`dataset`, `unit_id`). Test engines only.

| Column | Type | Description |
|---|---|---|
| `dataset` | string | FD001 to FD004 |
| `unit_id` | int | Test engine number (line number in the RUL file) |
| `true_rul` | int | Cycles the engine would still have run after its last test cycle |
| `source_file` | string | RUL file name |
| `ingested_at` | timestamp | When the row was loaded |

## pdm_bronze.engine_cycles_rejects

Lines that could not be parsed. Empty after a clean load.

| Column | Type | Description |
|---|---|---|
| `source_file` | string | File the line came from |
| `raw_line` | string | The line exactly as read |
| `reject_reason` | string | `expected 26 fields, got N`, `non-numeric value` or `unexpected file name` |
| `ingested_at` | timestamp | When the load ran |
