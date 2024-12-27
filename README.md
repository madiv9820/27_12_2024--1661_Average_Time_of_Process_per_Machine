# 1661. Average Time of Process per Machine

- ### Intuition
    The goal is to calculate the **average processing time** for each machine based on the time it takes to complete a specific process. This processing time is measured as the difference between the **start** and **end** timestamps for each activity associated with a machine.

    - #### Key Concepts:
        1. **Start and End Activities**:
                - Every process on a machine involves two key activities: a **start** and an **end**. 
                - The **start** activity marks the beginning of a task, and the **end** activity marks its completion.
                - Each activity has a **timestamp** that indicates when the event occurred.

        2. **Processing Time**:
                - The **processing time** is the time difference between the `timestamp_start` and `timestamp_end` for each process.
                - This processing time is calculated for each machine and each process, and then we aggregate (e.g., average) the results across all processes for each machine.

        3. **Aggregate Across Machines**:
                - We want to find the **average processing time** for each machine, which requires grouping the data by `machine_id` and calculating the average of all the individual processing times for that machine.

- ### Approach
    The overall approach involves the following steps:
    - #### Step 1: **Data Filtering**
        - **Identify Start and End Activities**:
            - First, we need to **separate** the "start" activities and the "end" activities. Each type of activity has a specific `activity_type` (either 'start' or 'end').
            - We filter the data into two subsets:
                - One for `start` activities.
                - One for `end` activities.

    - #### Step 2: **Join the Data**
        - **Match Start and End Events**:
            - Since each `start` activity has a corresponding `end` activity (for the same process and machine), we need to **pair** each `start` with its corresponding `end`.
            - We can do this using a **join** operation based on common attributes:
                - **machine_id**: The ID of the machine executing the process.
                - **process_id**: The ID of the process being executed.
            - This join will ensure that the start and end events for the same process are matched correctly.

    - #### Step 3: **Calculate Time Differences**
        - **Calculate Processing Time**:
            - Once the start and end events are matched, the next step is to calculate the **processing time** for each machine.
            - This is simply the difference between the `timestamp_end` and `timestamp_start`.
            - The time difference can be computed in various units:
                - **Seconds** (most common in many systems).
                - **Minutes**, **hours**, or **milliseconds**, depending on the desired level of precision.

    - #### Step 4: **Aggregate Results**
        - **Group by Machine**:
            - After calculating the processing times for each process, the next step is to **group the data by `machine_id`**.
            - For each group (i.e., each machine), we calculate the **average processing time** across all its processes.
            - This aggregation will give us a single result per machine, representing its average processing time.

    - #### Step 5: **Format and Output**
        - **Format the Result**:
            - We can round the calculated average processing time to a desired number of decimal places for clarity (e.g., 3 decimal places).
            - The final output should consist of two columns:
                - **machine_id**: The identifier of the machine.
                - **processing_time**: The average processing time for that machine.

- ### **Key Concepts for Different Environments**
    - #### 1. **SQL Approach**
        - **SQL** is particularly useful when working with structured data stored in relational databases.
            - **Filtering**: Use `WHERE` clause to filter for `start` and `end` activities.
            - **Joining**: Use `JOIN` to match `start` and `end` events based on `machine_id` and `process_id`.
            - **Time Difference**: Use `TIMESTAMPDIFF()` or subtract the `timestamp` columns directly to calculate the time difference.
            - **Aggregation**: Use `GROUP BY` to group by `machine_id` and `AVG()` to calculate the average processing time.

    - #### 2. **PySpark Approach**
        - **PySpark** is ideal for processing large datasets distributed across a cluster.
            - **Filtering**: Use `.filter()` to filter for `start` and `end` events.
            - **Aliasing**: Use `.alias()` to refer to the same DataFrame with different aliases (`a` for `start` and `b` for `end`).
            - **Joining**: Use `.join()` to combine the `start` and `end` events based on `machine_id` and `process_id`.
            - **Time Difference**: Use `.cast('long')` to convert the timestamp to a numeric value (milliseconds), then calculate the difference.
            - **Aggregation**: Use `.groupBy()` followed by `.agg()` with `avg()` to calculate the average processing time.

    - #### 3. **Pandas (Python) Approach**
        - **Pandas** is a flexible and efficient library for data manipulation, ideal for processing data in memory.
            - **Filtering**: Use boolean indexing (e.g., `activity[activity.activity_type == 'start']`) to filter `start` and `end` events.
            - **Merging**: Use `.merge()` to join the two DataFrames (`a` for `start` and `b` for `end`) on `machine_id` and `process_id`.
            - **Time Difference**: Subtract the `timestamp_start` from `timestamp_end` to get a `timedelta` object and convert it to seconds using `.dt.total_seconds()`.
            - **Aggregation**: Use `.groupby()` to group by `machine_id` and `.mean()` to calculate the average processing time for each machine.
            - **Rounding**: Use `.round()` to round the result to the desired number of decimal places.

- ### **Considerations**
    - **Handling Missing Data**:
        - Ensure that every `start` event has a corresponding `end` event. If not, you may need to handle or filter out such cases.
  
    - **Time Unit Selection**:
        - The choice of time unit (seconds, minutes, etc.) depends on the business requirements. Generally, **seconds** is the most common unit for processing time.

    - **Performance**:
        - For large datasets, consider the performance implications. SQL and PySpark are optimized for large-scale data, whereas Pandas is suited for smaller datasets that fit into memory.

- ### Code Implementation
    - **SQL:**
        ```sql []
        SELECT 
            a.machine_id,  -- Select the machine_id column from the 'a' table (start activities)
            
            -- Calculate the average time difference between start and end activities for each machine
            ROUND(AVG(b.timestamp - a.timestamp), 3) AS processing_time  -- Calculate the time difference between start and end for each activity pair and compute the average for each machine. Round the result to 3 decimal places.

        FROM Activity a  -- Alias 'a' for the start activities in the Activity table.

        -- Join the 'a' (start) table with 'b' (end) table based on the following conditions:
        JOIN Activity b ON
            a.machine_id = b.machine_id  -- Ensure the machine_id matches between the start and end activities
            AND a.process_id = b.process_id  -- Ensure the process_id matches between the start and end activities
            AND a.activity_type = 'start'  -- Only consider rows where the activity_type in 'a' is 'start'
            AND b.activity_type = 'end'  -- Only consider rows where the activity_type in 'b' is 'end'

        GROUP BY a.machine_id  -- Group by machine_id, so the average is computed for each machine.
        ```
    - **PySpark:**
        ```python3 []
        def find_Average_Time(activity: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
            # Step 1: Define the join condition
            # We are joining the 'start' and 'end' activities on machine_id, process_id, and activity_type
            condition = (
                (col('a.machine_id') == col('b.machine_id')) &  # Match machine_id between 'start' and 'end' activities
                (col('a.process_id') == col('b.process_id')) &  # Match process_id between 'start' and 'end' activities
                (col('a.activity_type') == 'start') &           # Ensure 'a' refers to 'start' activity type
                (col('b.activity_type') == 'end')               # Ensure 'b' refers to 'end' activity type
            )
            
            # Step 2: Perform the join on the activity DataFrame with itself using the defined condition
            output = activity.alias('a').join(
                activity.alias('b'),  # Join 'a' (start) with 'b' (end) on the condition
                on = condition, 
                how = 'inner'  # Use inner join to only include rows with matching start and end activities
            )\
            .select(  # Select relevant columns from both 'a' (start) and 'b' (end) activities
                col('a.machine_id'),
                col('a.timestamp').alias('start_timestamp'),  # Rename 'a.timestamp' to 'start_timestamp'
                col('b.timestamp').alias('end_timestamp')     # Rename 'b.timestamp' to 'end_timestamp'
            )\
            .groupby(
                col('a.machine_id') # Group the data by machine_id
            )\
            .agg(
                # Calculate the average processing time by subtracting start and end timestamps
                round(avg((col('end_timestamp') - col('start_timestamp'))), 3).alias('processing_time')
            )
            
            # Step 3: Return the result with the machine_id and its corresponding average processing time
            return output
        ```
    - **Pandas**
        ```python3 []
        def find_Average_Time(activity: pd.DataFrame) -> pd.DataFrame:
            # Step 1: Filter 'start' and 'end' activities
            a = activity[activity.activity_type == 'start'].copy()  # DataFrame for 'start' activities
            b = activity[activity.activity_type == 'end'].copy()    # DataFrame for 'end' activities
                
            # Step 2: Merge the 'start' and 'end' activities based on 'machine_id' and 'process_id'
            output = a.merge(b, on = ['machine_id', 'process_id'], how = 'inner', suffixes = ['_start', '_end'])
            
            # Step 3: Calculate the time difference between 'start' and 'end' timestamps
            output['timeDiff'] = (output.timestamp_end - output.timestamp_start) 
            
            # Step 4: Select necessary columns and group by 'machine_id' to calculate the average processing time
            output = output[['machine_id', 'timeDiff']]
            
            # Group by 'machine_id' and calculate the mean of 'timeDiff'
            output = output.groupby('machine_id', as_index = False).agg({'timeDiff': 'mean'})
            
            # Step 5: Round the processing time to 3 decimal places
            output['processing_time'] = output['timeDiff'].round(3)
            
            # Drop the intermediate 'timeDiff' column
            output.drop(columns=['timeDiff'], inplace = True)
            
            return output
        ```