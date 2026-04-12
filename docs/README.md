# data-ingestion

## Installation
```bash
pip install -r requirements.txt

```
# Project Structure
```bash
data-ingestion/
│
├── airflow/        # Airflow DAGs and configurations
├── config/         # Configuration files (DB, AWS, etc.)
├── data/           # Local data storage (optional / staging)
├── docs/           # Project documentation
├── etl/            # Extract, Transform, Load scripts
├── infra/          # Infrastructure as Code (Terraform)
├── myenv/          # Virtual environment (should be ignored in git)
├── pipelines/      # Pipeline definitions / orchestration logic
├── tests/          # Unit and integration tests
└── requirements.txt # Python dependencies
```