# Project Images

This folder contains screenshots and images for the Sports Analytics Pipeline project.

## Image Guidelines

### Landing Page Screenshot
- **Filename**: `landing-page.png` or `dashboard-main.png`
- **Recommended Size**: 1920x1080 or 1440x900
- **Format**: PNG for best quality
- **Content**: Should show the Streamlit dashboard with:
  - Title and header
  - Sample charts (win rates, ELO trends, point differentials)
  - Sidebar with filters
  - Clean, professional appearance

### Additional Screenshots (Optional)
- `airflow-dag.png` - Airflow DAG view
- `dbt-lineage.png` - dbt model lineage graph
- `architecture-diagram.png` - System architecture visualization
- `data-quality-tests.png` - dbt test results

## How to Capture Screenshots

### For Streamlit Dashboard:
1. Run the pipeline to populate data:
   ```bash
   make run-pipeline
   ```

2. Start the Streamlit app:
   ```bash
   make app
   ```

3. Navigate to http://localhost:8501

4. Select a few teams and seasons to show meaningful data

5. Take a full-page screenshot showing:
   - Header with title
   - At least one chart/visualization
   - Sidebar filters (partially visible)

### For Airflow:
1. Start Airflow:
   ```bash
   make airflow-up
   ```

2. Navigate to http://localhost:8080

3. Login (airflow/airflow)

4. Go to DAGs view and capture the sports_pipeline_dag

## Using Images in Documentation

Once you've added images here, update the main README.md to include them:

```markdown
## Screenshots

### Analytics Dashboard
![NBA Analytics Dashboard](images/landing-page.png)

### Data Pipeline
![Airflow DAG](images/airflow-dag.png)
```

## Image Optimization

Before committing, optimize images to reduce repository size:
- Use PNG for screenshots with text
- Compress images using tools like:
  - `pngquant` for PNG files
  - Online tools like TinyPNG
- Keep images under 1MB each if possible