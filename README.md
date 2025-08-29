[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

## Background
This is a connector that maps Aetna data to the Tuva Project Input layer on version `0.15.x`. 

This connector follows a general project structure for Tuva Project connectors, including the medical claims and eligibility data models that The Tuva Project Package expects as inputs, intermediate models with aggregation and deduplication for medical claims and eligibility, and the final models for claims and eligibility expected by the input layer.

## Notes on Aetna data files
1. The data dictionaries used in building this connector can be found [here](https://www.aetna.com/info/aetinfo/). This is a connector for the Universal Medical Eligibility 1000 File Record Layout for `eligibility` and the Universal Medical/Dental 1480 File Record Layout for `medical_claim`.
2.  **Headers**: Typically, Aetna claims and eligibility files we've seen come with no headers. In the staging layer, we've named the columns in close alignment to the specifications of these files. There are some situations where naming columns exactly as they show up in the spec would cause collisions (e.g. `last_name` corresponding to both `member_last_name` and `subscriber_last_name`). In cases like these, we've altered the names to disambiguate between source columns and prevent these collisions.
3. **Handling multiple files**: This connector does not do any deduplication based on `file_date` or other file ingestion-specific data fields. It's possible that changing deduplication logic to key off of this information could be helpful in deduplication depending on your use case / volume of data.
4. **Fixed width files**: these files are fixed length. In some cases where trimming whitespace during file ingestion is not handled, adding `trim()` calls in staging may assist in removing whitespaces where needed.

## Aetna Adjustments, Denials, and Reversals (ADR) Logic
In handling ADR for Aetna medical claims, two scenarios are possible:

**There is an adjustment issued to the original claim**
In this case, the same root claim ID is used for issuing the new claim, and the net sum of the financial amounts is the correct total for the claim.

The "root" claim ID is the first 9 characters of `src_clm_id`, and these claims are linked by a pointer (`prev_clm_seg_id`).

**The original claim is voided and a new claim is issued**
In this case, a "void" record is issued for the original claim. A claim is "void" if there is a "D" in `claim_line_status_code`.

The claim is then reprocessed under a new claim ID (first 9 characters of `src_clm_id`).

The ADR logic we've written in this connector handles both scenarios.

## High-level project structure
The typical workflow and project structure for mapping raw data to the Tuva Data Model within this connector is done in 3 stages (found within the models directory): `staging`, `intermediate`, and `final`.

**Staging**
Model(s) with little transformation.
* Convert non varchar/string column to the appropriate data types (e.g. dates, numbers)
* Map source() raw data to the Tuva Data Model

**Intermediate**
Model(s) that perform major transformation to the source data.
* Deduplication of Adjustments, Denials, and Reversals (ADR) for claims
* Any other consequential transformations

**Final**
Model(s) that are used by the Tuva Project (i.e. the Input Layer).
* Models in this layer are expected by The Tuva Project Package Input Layer

## 🔌 Database Support
- Snowflake
<br/><br/>

## ✅ Quickstart Guide

### **Step 1: Prerequisites**

Before you begin, ensure you have the following:

1.  **Access to your data warehouse:** Credentials and network access to your data warehouse instance (e.g. Snowflake, BigQuery).
2.  **Aetna data:** Your raw Aetna data must be loaded into specific tables within your data warehouse.
3.  **dbt CLI Installed:** You need dbt (version 1.9 recommended) installed on your machine or environment where you'll run the transformations. See [dbt Installation Guide](https://docs.getdbt.com/docs/installation) for help with installation.
4.  **Git:** You need Git installed to clone this project repository.
5.  **Authentication Details:** These details will be important in connecting to dbt with a `profiles.yml` file.

### **Step 2: Clone the Repository**

Open your terminal or command prompt and clone this project:

```bash
git clone https://github.com/tuva-health/aetna_connector.git
cd aetna_connector
```

### **Step 3: Create and Activate Virtual Environment**

It's highly recommended to use a Python virtual environment to manage project dependencies. This isolates the project's packages from your global Python installation.

1. Create the virtual environment (run this inside the aetna_connector directory):

```bash
# Use python3 if python defaults to Python 2
python -m venv venv
```
This creates a venv directory within your project folder.

2. Activate the virtual environment:
* macOS / Linux (bash/zsh):
```source venv/bin/activate```
* Windows (Command Prompt):
```venv\Scripts\activate.bat```
* Windows (PowerShell):
```.\venv\Scripts\Activate.ps1```
* Windows (Git Bash):
```source venv/Scripts/activate```

You should see (venv) prepended to your command prompt, indicating the environment is active.

### **Step 4: Install Python Dependencies** 

With the virtual environment active, install the required Python packages, including dbt and the warehouse-specific dbt adapter (e.g. `dbt-snowflake`, `dbt-bigquery`).

### **Step 5: Configure profiles.yml for Data Warehouse Connection**

dbt needs to know how to connect to your data warehouse. In general, this is done via a profiles.yml file, which you need to create. This file should NOT be committed to Git, as it contains sensitive credentials.

* **Location:** By default, dbt looks for this file in ~/.dbt/profiles.yml (your user home directory, in a hidden .dbt folder).
* **Content:** See the [dbt docs](https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml).

### **Step 6: Install dbt Package Dependencies**

This project relies on external dbt packages (The Tuva Project and dbt_utils). Run the following command in your terminal from the project directory (the one containing dbt_project.yml):
```Bash
dbt deps
```
This command reads packages.yml and downloads the necessary code into the dbt_packages/ directory within your project.

### **Step 7: Test the Connection**

Before running transformations, verify that dbt can connect to Snowflake using your profiles.yml settings:
```Bash
dbt debug
```

Look for "Connection test: OK connection ok". If you see errors, double-check your profiles.yml settings (account, user, role, warehouse, authentication details, paths).

## Running the Project
Once setup is complete, you can run the dbt transformations:

Full Run (Recommended First Time), this command will:
* Run all models (.sql files in models/).
* Run all tests (.yml, .sql files in tests/).
* Materialize tables/views in your target data warehouse as configured.

```bash
dbt build
```

This might take some time depending on the data volume and warehouse size.

#### Run Only Models:
If you only want to execute the transformations without running tests:
```bash
dbt run
```

#### Run Only Tests:
To execute only the data quality tests:
```Bash
dbt test
```

#### Running Specific Models:
You can run specific parts of the project using dbt's node selection syntax. For example:
* Run only the staging models: `dbt run -s path:models/staging`
* Run a specific model and its upstream dependencies: `dbt run -s +your_model_name`

### Tuva Resources:
- The Tuva Project [docs](https://thetuvaproject.com/)
- The Tuva Project [dbt docs](https://tuva-health.github.io/tuva/#!/overview/)
- The Tuva Project [package](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/)

### dbt Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Running SQLFluff
1. Create a virtual environment: `python3 -m venv .venv`
2. Activate the virtual environment: `source .venv/bin/activate`
3. Install the required packages: `pip install -r requirements.txt`
4. Create a profiles.yml file in `~/.dbt`. The profiles.yml.tmpl file is there as a reference.
5. Run SQLFluff on your target file (e.g. medical claims): `sqlfluff fix models/final/medical_claim.sql`
