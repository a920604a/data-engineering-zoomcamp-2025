import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import duckdb


# Question 1: dlt Version
print("dlt version:", dlt.__version__)


# Question 2: Define & Run the Pipeline (NYC Taxi API)
# Use the @dlt.resource decorator to define the API source.
@dlt.resource(name="rides")   # <--- The name of the resource (will be used as the table name)
def ny_taxi():
    # Implement automatic pagination using dlt's built-in REST client.
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   # <--- yield data to manage memory


pipeline = dlt.pipeline(destination="duckdb")

# Load the extracted data into DuckDB for querying.
load_info = pipeline.run(ny_taxi)
print(load_info)


# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
conn.sql("DESCRIBE").df()

ret = conn.sql("SHOW TABLES").df()
print(f"How many tables were created? {ret}")

# Question 3: Explore the loaded data
df = pipeline.dataset(dataset_type="default").rides.df()
df
print(f"What is the total number of records extracted? :{df.shape[0]}")



# Question 4: Trip Duration Analysis
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)