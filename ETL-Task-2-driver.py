"""
                                    ETL Assignment task 2
                                    author: Abhishek Padghane

Problem Definition: Given data with monthly budget columns, pivot this wide data into long data. (Reduce number of
columns)
"""


# loading required libraries

# ETL class contains all the ETL functions
from src.ETL_Task_2 import ETL


# driver program
if __name__ == "__main__":

    # initializing etl process object
    etl = ETL()

    # ------------------------------------------------------------------------------------------------------------
    #                                                   EXTRACT
    # ------------------------------------------------------------------------------------------------------------

    # extracting data
    etl.extract()

    # ------------------------------------------------------------------------------------------------------------
    #                                                  TRANSFORM
    # ------------------------------------------------------------------------------------------------------------

    # transforming data
    etl.transform()

    # ------------------------------------------------------------------------------------------------------------
    #                                                    LOAD
    # ------------------------------------------------------------------------------------------------------------

    # loading data
    etl.load()
