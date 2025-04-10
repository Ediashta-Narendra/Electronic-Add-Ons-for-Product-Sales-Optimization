<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Electronic Add-Ons Products for Sales Optimization</title>
</head>
<body>
    <header>
        <h1>Electronic Add-Ons Products for Sales Optimization - Data Modeling and Analysis</h1>
    </header>

    <section>
        <h2>Overview</h2>
        <p>
            This project focuses on end-to-end data management and analytics related to the sales performance of electronic add-on products. It was initiated in response to a government regulation in late 2024 which restricts the sale of certain smartphone brands and models, which may indirectly influence consumer behavior in purchasing complementary products.
        </p>   
        <p>
            To adapt to these market changes, the analysis aims to uncover insights into customer preferences, the correlation between add-on purchases and restricted product categories (i.e. smartphone), and overall sales trends. The primary business goal is to optimize product bundling strategies and improve sales targeting for electronic add-ons based on evidence from the data.
        </p>
        <p>
            The entire data pipeline was automated using Apache Airflow, covering the full ETL process — starting from extracting transactional records from a PostgreSQL database, transforming and cleaning the dataset, and finally loading it into an Elasticsearch index. 
        </p>    
        <p>
            The data obtained from Kaggle spans from 2023 to 2024, comprising 20,000 rows, and serves as the foundation for downstream analysis and optimization. Raw dataset is included in the repository as <code>electronic_data_raw.csv</code>, while the cleaned version—generated through the ETL process—is saved as <code>electronic_data_clean.csv</code>. The final, processed data is then loaded into an Elasticsearch index for further analysis and exploration.
        </p>
        <p>
            To ensure data quality, expectations were defined and validated using the Great Expectations framework, with the process documented in the notebook <code>Great Expectation.ipynb</code>.
        </p>
        <p>
            For business stakeholders, a dashboard was developed in Kibana using data indexed in Elasticsearch. This dashboard provides real-time insights and visualizations on add-on sales distribution, top categories, performance before and after the regulation, and consumer segmentation, all of which support data-driven decision-making for future sales strategies.
        </p>

        <h2>Analytical Business Problem Breakdown</h2>
        <ul>
            <li>Which electronic add-on product categories contribute most significantly to overall sales revenue?</li>
            <li>How much do add-on purchases contribute to total revenue, and what is the average add-on spend per transaction?</li>
            <li>What are the most frequent purchase patterns for add-on items (e.g., bundling, cross-category preferences)?</li>
            <li>Is there a significant relationship between customer loyalty membership and the quantity/value of add-on purchases?</li>
            <li>How do add-on product sales trend over time (monthly, quarterly), and how are they affected by smartphone restrictions?</li>
            <li>What consumer behavior patterns can be identified based on the chosen shipping methods (e.g., express vs regular)?</li>
        </ul>

        <h2>Data Modeling Process</h2>
        <p>
            The modeling process involved several preprocessing steps including feature selection, encoding of categorical variables, handling of missing values, and date normalization. The cleaned dataset was enriched with calculated fields such as revenue contribution per category, transaction value per order, and frequency of specific item bundles.
        </p>
        <p>
            All data preparation tasks were embedded in the Airflow DAG pipeline. Once transformed, the dataset was stored in Elasticsearch with custom schema fields aligned for Kibana dashboard compatibility (e.g., keyword type for aggregations, date formats for time-series).
        </p>

        <h2>Features</h2>
        <ul>
            <li><strong>Automated Data Cleaning (via Airflow Pipeline)</strong> – Handles missing values, standardizes formats, and structures raw data efficiently for analysis.</li>
            <li><strong>Data Validation using Great Expectations</strong> – Validates schema and ensures data quality throughout the pipeline.</li>
            <li><strong>Data Analysis and Visualization using Kibana</strong> – Provides business users with real-time, interactive dashboards and insights.</li>
        </ul>

        <h2>Tech Stacks</h2>
        <ul>
            <li><strong>PostgreSQL</strong> – Source database containing raw transactional records</li>
            <li><strong>Apache Airflow</strong> – Orchestration tool for automating the ETL pipeline</li>
            <li><strong>Pandas & Python</strong> – Data transformation and feature engineering</li>
            <li><strong>Elasticsearch</strong> – Indexing and storing structured data for analysis</li>
            <li><strong>Kibana</strong> – Dashboarding and visualization layer</li>
            <li><strong>Great Expectations</strong> – Data validation framework to ensure quality</li>
            <li><strong>Jupyter Notebook</strong> – Exploratory data analysis and documentation</li>
        </ul>

        <h2>Deliverables</h2>
        <ul>
            <li><code>DAG for Electronic Add-Ons for Product Sales Optimization.py</code> – Airflow DAG file to orchestrate ETL pipeline</li>
            <li><code>electronic_data_raw.csv</code> – Unprocessed dataset from the data source</li>
            <li><code>electronic_data_clean.csv</code> – Final cleaned dataset after transformation</li>
            <li><code>Great Expectation.ipynb</code> – Notebook for validating schema and expectations</li>
            <li><code>Kibana Dashboard Capture/</code> – Folder containing snapshots of the dashboard or exported objects</li>
            <li><code>README.md</code> – Project documentation file for quick overview and instructions</li>
        </ul>
    </section>
</body>
</html>
