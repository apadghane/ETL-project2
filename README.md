# ETL Assignment 2
![etl](https://www.astera.com/wp-content/uploads/2019/10/etl.png)<br><br><br>
**Author**: Abhishek Padghane<br><br>
**Task**: In this assigment, Data need to be loaded from source. Transformed in such a fashion that output should be a database supported (sql) file. Transformation that need be done is making dataset long. Its means to pivot the dataset using one column, in this the lenth of the dataset increases and width(columns) decreases.<br><br>
**Tools and Technologies**:
* Python
* Pandas
* SqlAlchemy
* xlrd
* Dotenv

**Files and Folders**:
* **Analytics:** This folder contains data files<br><br>
* **src:** This python package contains functioanlities for performing etl on data<br><br>
* **.envDemo:** Contains credential template for database credentials, insert your db credentials and rename/clone this file as `.env`<br><br>
* **ETL-Task-2-driver.py:** Source code file of this assignment. This python file Contains python code of ETL process<br><br>
* **requirements.txt:** Contains third-party libraries/dependencies for this project<br><br>
* **runtime.txt:** Contains version of python used to build this project<br><br>
* **.gitignore:** Contains file and folder name that should not be included in git repository<br><br>
* **README.md:** Project synopsis

**Installation Instructions**:
1. Download [Python](https://www.python.org/downloads/) and install it on your system<br><br>
2. Download [MariaDB](https://mariadb.org/download/) database and install it on your system<br><br>
2. Install virtual environment by `pip install virtualenv`<br><br>
3. Create virtual environment by `python3 -m virtualenv venv` or `python -m virtualenv venv`<br><br>
4. Activate virtualenv by `venv\Scripts\activate` or `source venv/bin/activate` in shell/bash/terminal<br><br>
4. Install requirements by `pip3 install -r requirements.txt` or `pip install -r requirements.txt`<br><br>
5. Insert your db credentials in `.envDemo` file and rename/clone it to `.env`<br><br>
5. To perform ETL on data we need to run python file, type `python ETL-Task-2-driver.py` in virtualenv activated shell/bash/terminal

**Screen Shots**:
* Dataset created by myself, is provided in Analytics folder
![etl1](https://i.ibb.co/nmhy6yS/etl-2-3.jpg)<br><br>
* Folder and file structure
![etl9](https://i.ibb.co/qF9mdkm/etl-2-4.jpg)<br><br>
* Output of running ETL process
![etl9](https://i.ibb.co/FDpGSZN/etl-2-1.jpg)<br><br>
* Output of ETL process can be seen from HeidiSql Db ui, transformed data can be seen in tables<br>
![etl10](https://i.ibb.co/LQQxKdq/etl-2-2.jpg)