from tabulate import tabulate
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

class Utils:
    @staticmethod
    def display_pretty_table(spark_df: SparkDataFrame, num_rows: int = 5) -> None:
        """
        Converts a PySpark DataFrame to a pandas DataFrame and displays it in a pretty tabular format.

        Parameters:
        spark_df (SparkDataFrame): The PySpark DataFrame to display.
        num_rows (int): Number of rows to display (default: 5).
        """
        if not isinstance(spark_df, SparkDataFrame):
            raise ValueError("Input must be a PySpark DataFrame.")
        
        # Convert Spark DataFrame to pandas DataFrame
        pandas_df = pd.DataFrame(spark_df.take(num_rows), columns=spark_df.columns)
        
        # Display the DataFrame in a pretty table format
        print(tabulate(pandas_df, headers='keys', tablefmt='pretty'))

    @staticmethod
    def display(spark_df, num_rows=5):
        """
        Converts a PySpark DataFrame to a pandas DataFrame and displays it.
    
        Parameters:
        - spark_df: PySpark DataFrame
            The input PySpark DataFrame to be converted.
        - num_rows: int, default=5
            The number of rows to convert and display.
    
        Returns:
        - pandas_df: pandas.DataFrame
            The resulting pandas DataFrame.
        """
        try:
            # Limit rows and convert to pandas
            pandas_df = spark_df.limit(num_rows).toPandas()
            #print(pandas_df)
            return pandas_df
        except Exception as e:
            print(f"Error during conversion: {e}")
            return None

