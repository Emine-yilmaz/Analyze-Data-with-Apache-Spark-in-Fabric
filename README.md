# Analyze Data with Apache Spark in Fabric

This lab demonstrates how to ingest data into a Microsoft Fabric lakehouse and use PySpark to read and analyze the data. You will learn to create DataFrames, explore and transform data using PySpark functions and Spark SQL, and visualize the results using built-in notebook features and Python libraries like Matplotlib and Seaborn.

**Estimated Completion Time:** 45 minutes

**Prerequisites:**

* A Microsoft Fabric trial.

## Steps

### 1. Create a Workspace

1.  Navigate to the [Microsoft Fabric home page](https://app.fabric.microsoft.com/home?experience=fabric) and sign in with your Fabric credentials.
2.  From the left menu bar, select **Workspaces** (🗇) and then **New workspace**.
3.  Give the new workspace a name and in the **Advanced** section, select the appropriate **Licensing mode**. If you have started a Microsoft Fabric trial, select **Trial**.
4.  Select **Apply** to create an empty workspace.

### 2. Create a Lakehouse and Upload Files

1.  From your new workspace, select **+ New item** and then **Lakehouse**. Give the lakehouse a name and select **Create**.
2.  Download the datafiles from [https://github.com/MicrosoftLearning/dp-data/raw/main/orders.zip](https://github.com/MicrosoftLearning/dp-data/raw/main/orders.zip).
3.  Extract the zipped archive to get a folder named `orders` containing three CSV files: `2019.csv`, `2020.csv`, and `2021.csv`.
4.  Return to your new lakehouse. In the **Explorer** pane, next to the **Files** folder, select the **...** menu, then **Upload**, and **Upload folder**. Navigate to the `orders` folder on your local machine and select **Upload**.
5.  After uploading, expand **Files** and select the `orders` folder to verify the CSV files are present.

### 3. Create a Notebook

1.  Select your workspace, then select **+ New item** and **Notebook**.
2.  Click the default notebook name above the **Home** tab to change it to a descriptive name (e.g., `Sales Data Analysis`).
3.  Select the first cell (code cell) and use the **M↓** button in the top-right toolbar to convert it to a markdown cell.
4.  Edit the markdown cell with the following content:

    ```markdown
    # Sales order data exploration
    Use this notebook to explore sales order data
    ```

5.  Click anywhere outside the cell to render the markdown.

### 4. Create a DataFrame

1.  Select your new workspace from the left bar and then select your lakehouse to open the Explorer pane.
2.  Expand **Lakehouses**, then **Files**, and select the `orders` folder.
3.  From the **...** menu for `2019.csv`, select **Load data > Spark**. This will generate code in a new cell.
4.  Modify the generated code to correctly read the CSV with the header:

    ```python
    df = spark.read.format("csv").option("header","true").load("Files/orders/2019.csv")
    # df now is a Spark DataFrame containing CSV data from "Files/orders/2019.csv".
    display(df)
    ```

5.  Run the cell using the **▷ Run cell** button. Observe the output.
6.  To define the schema explicitly, replace the existing code with the following:

    ```python
    from pyspark.sql.types import *

    orderSchema = StructType([
        StructField("SalesOrderNumber", StringType()),
        StructField("SalesOrderLineNumber", IntegerType()),
        StructField("OrderDate", DateType()),
        StructField("CustomerName", StringType()),
        StructField("Email", StringType()),
        StructField("Item", StringType()),
        StructField("Quantity", IntegerType()),
        StructField("UnitPrice", FloatType()),
        StructField("Tax", FloatType())
    ])

    df = spark.read.format("csv").schema(orderSchema).load("Files/orders/2019.csv")

    display(df)
    ```

7.  Run the cell and review the output with the correct schema.
8.  Modify the `load` path to use a wildcard to read all CSV files in the `orders` folder:

    ```python
    from pyspark.sql.types import *

    orderSchema = StructType([
        StructField("SalesOrderNumber", StringType()),
        StructField("SalesOrderLineNumber", IntegerType()),
        StructField("OrderDate", DateType()),
        StructField("CustomerName", StringType()),
        StructField("Email", StringType()),
        StructField("Item", StringType()),
        StructField("Quantity", IntegerType()),
        StructField("UnitPrice", FloatType()),
        StructField("Tax", FloatType())
    ])

    df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")

    display(df)
    ```

9.  Run the modified code to load data from all three years.

### 5. Explore Data in a DataFrame

#### 5.1. Filter a DataFrame

1.  Add a new code cell using the **+ Code** button.
2.  Enter and run the following code to select and count customers:

    ```python
    customers = df['CustomerName', 'Email']

    print(customers.count())
    print(customers.distinct().count())

    display(customers.distinct())
    ```

3.  Modify the first line to use the `select` method with a `where` clause to filter by a specific item:

    ```python
    customers = df.select("CustomerName", "Email").where(df['Item']=='Road-250 Red, 52')
    print(customers.count())
    print(customers.distinct().count())

    display(customers.distinct())
    ```

4.  Run the modified code.

#### 5.2. Aggregate and Group Data in a DataFrame

1.  Add a new code cell and enter the following code to calculate product sales:

    ```python
    productSales = df.select("Item", "Quantity").groupBy("Item").sum()

    display(productSales)
    ```

2.  Add another code cell and enter the following code to calculate yearly sales order counts:

    ```python
    from pyspark.sql.functions import *

    yearlySales = df.select(year(col("OrderDate")).alias("Year")).groupBy("Year").count().orderBy("Year")

    display(yearlySales)
    ```

3.  Run the cell and examine the output.

### 6. Use Spark to Transform Data Files

#### 6.1. Use DataFrame Methods and Functions to Transform Data

1.  Add a new code cell and enter the following code to transform the DataFrame:

    ```python
    from pyspark.sql.functions import *

    # Create Year and Month columns
    transformed_df = df.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))

    # Create the new FirstName and LastName fields
    transformed_df = transformed_df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

    # Filter and reorder columns
    transformed_df = transformed_df["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "Email", "Item", "Quantity", "UnitPrice", "Tax"]

    # Display the first five orders
    display(transformed_df.limit(5))
    ```

2.  Run the cell and review the transformed data.

#### 6.2. Save the Transformed Data

1.  Add a new code cell and enter the following code to save the transformed DataFrame in Parquet format:

    ```python
    transformed_df.write.mode("overwrite").parquet('Files/transformed_data/orders')

    print ("Transformed data saved!")
    ```

2.  Run the cell and refresh the **Files** explorer to verify the `transformed_data/orders` folder contains Parquet files.
3.  Add a new cell to read the saved Parquet data:

    ```python
    orders_df = spark.read.format("parquet").load("Files/transformed_data/orders")
    display(orders_df)
    ```

4.  Run the cell to verify the data is loaded correctly.

#### 6.3. Save Data in Partitioned Files

1.  Add a new cell with code to save the DataFrame partitioned by Year and Month:

    ```python
    orders_df.write.partitionBy("Year","Month").mode("overwrite").parquet("Files/partitioned_data")

    print ("Transformed data saved!")
    ```

2.  Run the cell and refresh the **Files** explorer to see the partitioned folder structure (`partitioned_data/Year=xxxx/Month=xx`).
3.  Add a new cell to load data for a specific year:

    ```python
    orders_2021_df = spark.read.format("parquet").load("Files/partitioned_data/Year=2021/Month=*")

    display(orders_2021_df)
    ```

4.  Run the cell and verify that only 2021 data is displayed.

### 7. Work with Tables and SQL

#### 7.1. Create a Table

1.  Add a new code cell and enter the following code to save the original DataFrame as a Delta table named `salesorders`:

    ```python
    # Create a new table
    df.write.format("delta").saveAsTable("salesorders")

    # Get the table description
    spark.sql("DESCRIBE EXTENDED salesorders").show(truncate=False)
    ```

2.  Run the cell and review the table description.
3.  In the **Lakehouses** pane, refresh the **Tables** folder to verify the `salesorders` table is created.
4.  From the **...** menu for the `salesorders` table, select **Load data > Spark**. Run the generated code to query the table:

    ```python
    df = spark.sql("SELECT * FROM [your_lakehouse].salesorders LIMIT 1000")
    display(df)
    ```

#### 7.2. Run SQL Code in a Cell

1.  Add a new code cell and enter the following SQL query using the `%%sql` magic command:

    ```sql
    %%sql
    SELECT YEAR(OrderDate) AS OrderYear,
           SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue
    FROM salesorders
    GROUP BY YEAR(OrderDate)
    ORDER BY OrderYear;
    ```

2.  Run the cell and review the results.

### 8. Visualize Data with Spark

#### 8.1. View Results as a Chart

1.  Add a new code cell and run the following SQL query:

    ```sql
    %%sql
    SELECT * FROM salesorders
    ```

2.  In the results section, change the **View** option from **Table** to **Chart**.
3.  Use the **Customize chart** button to set the following options:
    * **Chart type:** Bar chart
    * **Key:** Item
    * **Values:** Quantity
    * **Aggregation:** Sum
4.  Select **Apply** to view the bar chart.

#### 8.2. Get Started with Matplotlib

1.  Add a new code cell and run the following code to get yearly revenue data:

    ```python
    sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
                    SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue, \
                    COUNT(DISTINCT SalesOrderNumber) AS YearlyCounts \
                FROM salesorders \
                GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \
                ORDER BY OrderYear"
    df_spark = spark.sql(sqlQuery)
    df_spark.show()
    ```

2.  Add a new code cell and use Matplotlib to create a bar plot of revenue by year:

    ```python
    from matplotlib import pyplot as plt

    # matplotlib requires a Pandas dataframe, not a Spark one
    df_sales = df_spark.toPandas()

    # Create a bar plot of revenue by year
    plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'])

    # Display the plot
    plt.show()
    ```

3.  Modify the code to customize the chart:

    ```python
    from matplotlib import pyplot as plt

    # Clear the plot area
    plt.clf()

    # Create a bar plot of revenue by year
    plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')

    # Customize the chart
    plt.title('Revenue by Year')
    plt.xlabel('Year')
    plt.ylabel('Revenue')
    plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
    plt.xticks(rotation=45)

    # Show the figure
    plt.show()
    ```

4.  Modify the code to explicitly create a Figure:

    ```python
    from matplotlib import pyplot as plt

    # Clear the plot area
    plt.clf()

    # Create a Figure
    fig = plt.figure(figsize=(8,3))

    # Create a bar plot of revenue by year
    plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')

    # Customize the chart
    plt.title('Revenue by Year')
    plt.xlabel('Year')
    plt.ylabel('Revenue')
    plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
    plt.xticks(rotation=45)

    # Show the figure
    plt.show()
    ```

5.  Modify the code to include two subplots:

    ```python
    from matplotlib import pyplot as plt

    # Clear the plot area
    plt.clf()

    # Create a figure for 2 subplots (1 row, 2 columns)
    fig, ax = plt.subplots(1, 2, figsize = (10,4))

    # Create a bar plot of revenue by year on the first axis
    ax[0].bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
    ax[0].set_title('Revenue by Year')

    # Create a pie chart of yearly order counts on the second axis
    ax[1].pie(df_sales['YearlyCounts'])
    ax[1].set_title('Orders per Year')
    ax[1].legend(df_sales['OrderYear'])

    # Add a title to the Figure
    fig.suptitle('Sales Data')

    # Show the figure
    plt.show()
    ```

#### 8.3. Use the Seaborn Library

1.  Add a new code cell and use Seaborn to create a bar chart:

    ```python
    import seaborn as sns

    # Clear the plot area
    plt.clf()

    # Create a bar chart
    ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)

    plt.show()
    ```

2.  Modify the code to set a visual theme:

    ```python
    import seaborn as sns

    # Clear the plot area
    plt.clf()

    # Set the visual theme for seaborn
    sns.set_theme(style="whitegrid")

    # Create a bar chart
    ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)

    plt.show()
    ```

3.  Modify the code to create a line chart:

    ```python
    import seaborn as sns

    # Clear the plot area
    plt.clf()

    # Create a line chart
    ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales)

    plt.show()
    ```

### 9. Clean Up Resources

1.  On the notebook menu, select **Stop session** to end the Spark session.
2.  In the bar on the left, select the icon for your workspace.
3.  Select **Workspace settings**.
4.  In the **General** section, scroll down and select **Remove this workspace**.
5.  Select **Delete** to delete the workspace.

Congratulations! You have successfully ingested and analyzed data using Apache Spark in Microsoft Fabric. You have learned how to create DataFrames, perform transformations, use Spark SQL, and visualize data using built-in features and Python libraries.
