from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_events_aggregated_sink(t_env):
    table_name = 'green_trips_aggregated2'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            -- window_start TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            taxi_trips_streak BIGINT
            -- PRIMARY KEY (window_start, PULocationID, DOLocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime  	TIMESTAMP(3),
            lpep_dropoff_datetime 	TIMESTAMP(3),
            PULocationID			INT,
            DOLocationID			INT,
            passenger_count			INT,
            trip_distance			DOUBLE,
            tip_amount				DOUBLE,
            event_watermark AS lpep_dropoff_datetime,
            WATERMARK for event_watermark  as event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    # t_env.from_path("green_trips").print_schema()
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    # env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        t_env.execute_sql(f"""
            INSERT INTO {aggregated_table}
            SELECT
                --window_start,
                PULocationID,
                DOLocationID,
                COUNT(*) AS taxi_trips_streak
            FROM {source_table}
            GROUP BY
                --window_start,
                PULocationID,
                DOLocationID,
                SESSION(event_watermark, INTERVAL '5' MINUTE);
        """).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()