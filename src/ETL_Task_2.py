# importing required libraries

# os for system related operation
import os

# json for loading json file as python dict
import json

# pandas for data processing
import pandas

# dotenv for loading db creadentials as environment variables
import dotenv

# datetime for datetime related operations
import datetime

# sqlalchemy for connecting mariadb
import sqlalchemy


class LoadConfig:
    """
            LoadConfig -> class -> public
            
            This class contains methods which provides with configurational settings

            Constructor: Reads configuration settings and initializes it

            Instance Variable:  1. config -> private

            Instance Methods:
                                1. get_directory() -> public
                                2. get_file() -> public
                                3. get_sheet() -> public
                                4. get_transform_columns() -> public
                                5. get_transform_column() -> public
                                6. get_transform_value() -> public
    """
    
    def __init__(self):
        """
            Constructor
            
            Initializes configuration variable
        """

        # initializing configuration variable by reading configuration file
        self.__config__ = json.loads(open("src/config.json").read())

    def get_directory(self) -> str:
        """
            get_directory -> Instance Method
            return: str
            
            Returns folder/directory where data file is residing
        """
        return self.__config__["data_directory"]

    def get_file(self) -> str:
        """
            get_file -> Instance Method
            return: str
            
            Returns name of data file
        """
        return self.__config__["data_file"]

    def get_sheet(self) -> str:
        """
            get_sheet -> Instance Method
            return: str
            
            Returns excel sheet name of data file
        """
        return self.__config__["data_sheet"]

    def get_transform_columns(self) -> list:
        """
            get_transform_columns -> Instance Method
            return: list
            
            Returns list of columns that need to be pivot/reduce
        """
        return self.__config__["transform_columns"]

    def get_transform_column(self) -> str:
        """
            get_transform_column -> Instance Method
            return: str
            
            Returns column name replaced with pivot/reduced columns
        """
        return self.__config__["transform_column"]

    def get_transform_value(self) -> str:
        """
            get_transform_value -> Instance Method
            return: str
            
            Returns column name for value column
        """
        return self.__config__["transform_value"]


class Logger:
    """
            Logger -> class -> public
            
            This class logs given message

            Constructor: Displays message
    """
    
    def __init__(self, message: str = None):
        """
            Constructor

            Displays the log message with proper log time
            
            params:
                    1. message -> str: Message that needs to be displayed as log message
        """

        # displaying log message
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}: {message}")


class ETL:
    """
            ETL -> class -> public
            
            This class contains functionality for doing etl on data

            Constructor: Initializes required values for doing etl

            Instance Variables: 1. config -> private
                                2. file_directory -> public
                                3. file_name -> public
                                4. transform_columns -> public
                                5. table_name -> public
                                6. transform_column -> public
                                7. transform_value -> public
                                8. new_column -> private
                                9. data_frame -> public

            Instance Methods:   1. extract() -> public
                                2. transform() -> public
                                3. load() -> public
    """
    
    def __init__(self):
        """
            Constructor
            
            Initializes configuration variable, file related values, db related values and pandas.DataFrame API related values

            Attributes:         1. config -> dict: Contains configuration settings
                                2. file_directory -> str: file path/directory/folder
                                3. file_name -> str: file name including format
                                4. transform_columns -> list[str]: List of column names that need to pivot/reduce
                                5. table_name -> str: Table in which transformed data will be loaded
                                6. transform_column -> str: Column name that will replace name of reduced/pivoted column
                                7. transform_value -> str: Column name of value column
                                8. new_column -> str: Name of the column which will contain modified date
                                9. data_frame -> pandas.DataFrame: pandas df in which input file will be fetched
        """

        # initializing configuration
        self.__config__ = LoadConfig()

        # initializing file and data related attributes
        self.file_directory = self.__config__.get_directory()
        self.file_name = self.__config__.get_file()
        self.transform_columns = self.__config__.get_transform_columns()

        # initializing table name
        self.table_name = "_".join(self.file_name.split(".")[0].split("-")).lower()

        # initializing modified column and value column
        self.transform_column = self.__config__.get_transform_column()
        self.transform_value = self.__config__.get_transform_value()

        # initializing name of new column
        self.__new_column__ = "ModifiedDate"

        # initializing data frame variable
        self.data_frame = None

    def extract(self):
        """
            extract -> Instance Method
            
            Performs extract phase of ETL on input data
        """

        # reading input data which is in excel file in a pandas.DataFrame
        self.data_frame = pandas.read_excel(os.path.join(self.file_directory, self.file_name),
                                            sheet_name=self.__config__.get_sheet())
        Logger(f"{self.file_name} file data is successfully loaded to pandas DataFrame from folder "
               f"{self.file_directory}")

    def transform(self):
        """
            transform -> Instance Method
            
            Performs transform phase of ETL on data that is extracted in pandas.DataFrame
            My transformation task is to melt or transform wide data into long data
            Given 'Budget' month wise, transform in such a way that only one column on month should be in output
        """

        # segregating remaining columns to append in output data
        id_columns = list(set(self.data_frame.columns).difference(set(self.transform_columns)))

        # performing melt transformation to make dataset long from wide dataset
        self.data_frame = pandas.melt(self.data_frame, value_vars=self.transform_columns, id_vars=id_columns,
                                      var_name=self.transform_column, value_name=self.transform_value)

        # converting month name like Jan, Feb to yyyy-mm
        self.data_frame[self.transform_column] = self.data_frame[self.transform_column].apply(
            lambda month: self.file_name.split(".")[0][-4:] + "-" +
                          datetime.datetime.strptime(month, "%b").strftime("%m"))

        # adding a new column which contains current datatime
        self.data_frame[self.__new_column__] = datetime.datetime.now()

        Logger(f"{self.file_name} file data is successfully transformed")
        Logger(f"Columns {self.transform_columns} successfully transformed to '{self.transform_column}'")

    def load(self):
        """
            load -> Instance Method

            Performs load phase of ETL on transformed data which is in a pandas.DataFrame
            I need to load the transform data into database. Here i am using mariadb database.
        """

        # loading db creadentials as environment variables
        dotenv.load_dotenv()

        # initializing db connection url string
        db_url = f'mysql+mysqlconnector://{os.environ["USER"]}:{os.environ["PASS"]}@'\
                 f'{os.environ["HOST"]}:{os.environ["PORT"]}/{os.environ["NAME"]}'

        # making db connection
        database_connection = sqlalchemy.create_engine(db_url)

        # loading/saving/writing transformed data to database
        self.data_frame.to_sql(con=database_connection, name=self.table_name, if_exists='replace', index=False)
        
        Logger("Data loaded successfully")
        Logger("{} file data is loaded into {} table in {} database".format(self.file_name, self.table_name,
                                                                            os.environ["NAME"]))
