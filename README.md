# Data Wrangling and Processing Project

This project includes a set of Python scripts for data wrangling and processing, as well as accessing and visualizing data from a SQL database in real-time. The project is designed to streamline the data preparation and analysis process, allowing users to easily clean, manipulate, and analyze large data sets.

## Data Pipeline

1. Data source: The Production department generates and collects various types of data that are stored on the company's server. These data contains various production parameters like tempreture of the chip and speed of the machine etc. This data is continuously being collected, and the frequency of collection can vary based on the data source.

2. Data wrangling and cleansing: In this stage, we analyze the input data, clean it, and transform it into a usable format for analysis. The cleaning process involves handling missing values, removing duplicates, and converting data types. Additionally, we may need to combine data from different sources and perform other data transformations to prepare it for further analysis.

3. Data processing and mining: After the data has been cleaned and transformed, we perform analysis on it to derive insights and valuable information. This analysis can include calculating summary statistics, identifying trends and patterns in the data, performing predictive modeling, and more. We use Python scripts to perform these analysis tasks and generate reports that provide insights into the data.

4. Data access to SQL database: The analyzed data is then imported into our company's MySQL server in real-time. The MySQL server acts as a centralized database that stores all of the company's data. By having all the data stored in one place, it makes it easier to access and analyze the data, and allows for real-time updates to be made as new data becomes available.

5. Real-time data visualization: Our MySQL server is connected to Grafana, which is an open-source dashboarding platform. Grafana allows us to create custom dashboards that display data in real-time, making it easy to monitor data streams and analyze trends over time. The Grafana dashboards can be customized to fit the specific needs of the business, and can be shared with team members to facilitate collaboration and decision-making.

These features together can be part of a data pipeline, which is a sequence of steps to transform raw data into a format that is ready for analysis.
## Installation

To use this project, you'll need to have Python 3.x installed on your system, as well as the following Python libraries:

1. Pandas
2. NumPy
3. Datetime
4. Mysql Connector
5. DBF reader
6. Openpyxl

You'll also need to have access to a SQL database and the appropriate credentials to connect to it.
To install the required libraries, you can run the following command:

## Usage

To use the project, you can clone the repository to your local machine:

```gh repo clone gadher80/Data-Wrangling-and-Processing-Project```

Then, navigate to the project directory and run the desired Python scripts using the following command:

```python main.py```


## Contributing

If you'd like to contribute to this project, please feel free to submit a pull request or open an issue. We welcome any feedback or suggestions for improvement!
