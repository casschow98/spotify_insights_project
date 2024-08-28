<a name="top"></a> <!-- Custom anchor -->
# Spotify Insights Data Pipeline Project
This project is a developed data pipeline that retrieves data from the Spotify Web API and presents insights on my listening history in a streamlit app.

## Table of Contents
[Objective](#objective)
<details>
  <summary><a href="#streamlit-application">Streamlit Application</a></summary>
  
  - [Figure 1](#figure-1). Table listing the top ten most listened to songs in the final streamlit application.
  - [Figure 2](@figure-2). Analysis of the audio features of the top 10 songs listened to.
</details>
<details>
  <summary><a href="#data-stack">Data Stack</a></summary>
  
  - [Figure 3](#figure-3). Diagram modelling the tools used in this project.
</details>

[Data Sources](#data-sources)

[Setup](#setup)
<details>
  <summary><a href="#workflow-orchestration">Workflow Orchestration</a></summary>
  
  - [Figure 6](#figure-6). Airflow DAG modelling the tasks in this workflow.
  - [Figure 7](#figure-7). Comparison between geojson and newline-delimited geojson format, processed using the geojson2ndjson command-line tool.
  - [Figure 8](#figure-8). Airflow DAG graph for processing the recreation trails dataset.
  - [Figure 9](#figure-9). Airflow DAG graph for processing the wildfire perimeters dataset.
  - [Figure 10](#figure-10). Airflow DAG graph for running the DBT models (staging and core)
</details>
<details>
  <summary><a href="#data-warehouse-transformations">Data Warehouse Transformations</a></summary>

  - [Figure 11](#figure-11). Spark job python script.
</details>


## Objective
The purpose of this project was to design and develop a modern data pipeline that interacts with the Spotify Web API and displays user listening history and audio analysis (specifically, using my personal spotify account).

## Streamlit Application
Click [here](https://spotify-insights-project-cchow.streamlit.app/) to view.

<a name="figure-1"></a>

[![](images/spotify_top_ten.png)](https://spotify-insights-project-cchow.streamlit.app/)

Figure 1. Table listing the top ten most listened to songs in the final streamlit application.

<a name="figure-2"></a>
![](images/spotify_bar_graph.png)

Figure 2. Analysis of the audio features of the top 10 songs listened to.


## Data Stack
- **Development Platform**: Docker
- **Infrastructure as Code (IAC)**: Terraform
- **Orchestration**: Apache Airflow
- **Data Lake**: Google Cloud Storage
- **Data Warehouse**: Google Big Query
- **Transformations**: Apache Spark
- **Data Visualization**: Streamlit Cloud

### Architecture
<a name="figure-3"></a>

![](images/data-stack-diagram.jpg)
Figure 3. Diagram modelling the tools used in this project.

## Data Sources
- Access tokens for two types of API requests are renewed every time that the directed acyclic graph (DAG) is run
- **Client Credentials**
  - The purpose of using this type of request is to obtain audio feature data for each track using track_id's
  - This type of API request requires a user to obtain a Client Secret and Client ID obtained from the My App page of the user's dashboard. It is after converted to base-64 encoding
- **Authorization Code**
  - The purpose of using this type of request is to obtain the recently played tracks for a specific user profile
  - This type of API request also requires the Client Secret and Client ID converted to base-64 encoding as described above
  - It is also required to obtain an authorization code through accessing an authorization url using: client id, redirect uri, and scope (in this case, scope is user-read-recently-played)
  - Example:
     - "https://accounts.spotify.com/authorize?client_id={CLIENT_ID}&response_type=code&redirect_uri={REDIRECT_URI}&scope={SCOPE}"
  - With the base-64 encoded client id and client secret and the authorization code, user can obtain temporary access token and refresh token
  - Refresh token is used in this project to make requests to the API without the need to repeatedly access the url and obtain a new authorization code prior
 
  
- See [Spotify Documentation](https://developer.spotify.com/documentation/web-api/concepts/authorization) for information on submitting requests to the Spotify Web API.

## Setup
- **Google Cloud Platform**
  - Services account and project
  - IAM user permissions and API's
  - Credentials keyfile and ssh client
  - VM instance
- **VM Instance**
  - Anaconda, Docker, Terraform, Spark installation
  - GCP credentials retrieval
- **Docker**
  - Docker build context and volume mapping
- **Terraform**
  - Configure GCP provider with credentials
  - Resource configuration (i.e., storage bucket, dataset)

## Workflow Orchestration
- Apache Airflow was used as a workflow orchestrator to manage the tasks of data ingestion, storage, and transformation
- Tasks were configured using Python and Spark operators and defined in a Directed Acyclic Graphs (DAG)

<a name="figure-4"></a>

![](images/spotify_dag.png)
Figure 4. Airflow DAG modelling the tasks in this workflow.

## Data Warehouse Transformations
- Apache Spark was used to perform a basic transformation on the main table in bigquery and write summarizing data of the top ten tracks to a new table in bigquery
- The spark job operates on a standalone cluster (see dags/spark/spark_job.py for configurations)

<a name="figure-5"></a>

![](images/spotify_dag.png)
Figure 4. Airflow DAG modelling the tasks in this workflow.
