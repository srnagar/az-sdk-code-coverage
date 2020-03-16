import os
import dash
import dash_core_components as dc
import dash_html_components as html
import pandas
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
import threading
import time

# Setup pandas display properties
pandas.set_option('display.max_rows', None)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', None)
pandas.set_option('display.max_colwidth', None)

# Constants
LATEST_COVERAGE_URL = "https://azuresdkartifacts.blob.core.windows.net/azure-sdk-for-java/test-coverage/jacoco.csv"
BLOB_CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')
STORAGE_CONNECTION_STRING = os.getenv('STORAGE_CONNECTION_STRING')
AGGREGATE_REPORT_NAME = 'aggregate/jacoco_aggregate.csv'

# Instantiate a new BlobServiceClient using a connection string
blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

# Instantiate a new ContainerClient
container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)


class DataUpdaterThread(object):
    """ A background thread that updates the coverage data periodically
    The run() method will be started and it will run in the background
    periodically until the application exits.
    """
    def __init__(self, instruction_data, branch_data, interval=60 * 60):
        """ Constructor
        :type interval: int
        :param interval: Check interval, in seconds
        """
        self.interval = interval
        self.instruction_data = instruction_data
        self.branch_data = branch_data

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True  # Daemonize thread
        thread.start()  # Start the execution

    def run(self):
        """ Method that runs forever """
        while True:
            coverage_report = self._get_aggregate_report()
            test_dates = list(coverage_report.groupby('TEST_DATE').groups.keys())
            instruction_df = coverage_report.groupby('GROUP').INSTRUCTION_COVERAGE.apply(list).reset_index()
            branch_df = coverage_report.groupby('GROUP').BRANCH_COVERAGE.apply(list).reset_index()
            first = True

            for i in range(len(instruction_df)):
                self.instruction_data.append({'x': test_dates,
                                              'y': instruction_df.iloc[i, 1],
                                              'type': 'line',
                                              'name': "<i>" + instruction_df.iloc[i, 0] + "</i>",
                                              'text': [instruction_df.iloc[i, 0]] * len(test_dates),
                                              'visible': first if first else 'legendonly',
                                              'hovertemplate': "Coverage: %{y:.2f}% <br>Date: %{x}<br>Module: %{text}"})

                self.branch_data.append({'x': test_dates,
                                         'y': branch_df.iloc[i, 1],
                                         'type': 'line',
                                         'name': "<i>" + branch_df.iloc[i, 0] + "</i>",
                                         'text': [branch_df.iloc[i, 0]] * len(test_dates),
                                         'visible': first if first else 'legendonly',
                                         'hovertemplate': "Coverage: %{y:.2f}% <br>Date: %{x}<br>Module: %{text}"})
                first = False
            time.sleep(self.interval)

    @staticmethod
    def _get_aggregate_report():
        # Instantiate a new BlobClient
        blob_client = container_client.get_blob_client(AGGREGATE_REPORT_NAME)
        try:
            blob_client.get_blob_properties()
        except ResourceNotFoundError:
            print("Aggregate report does not exist")
            return None

        download_stream = blob_client.download_blob()
        with open('jacoco-aggregate.csv', "wb") as aggregate_report:
            aggregate_report.write(download_stream.readall())
            aggregate_report.flush()
            print('Downloaded aggregate coverage report from blob storage to local file')

        result_df = pandas.read_csv('jacoco-aggregate.csv')
        result_df['TEST_DATE'] = pandas.to_datetime(result_df['TEST_DATE'], format='%Y-%m-%d')
        return result_df


def build_graph():
    app = dash.Dash()
    instruction_data = []
    branch_data = []
    DataUpdaterThread(instruction_data, branch_data)

    app.layout = html.Div(children=[
        html.H1("Azure SDK for Java - Code Coverage"),

        dc.Graph(id='instruction_coverage',
                 figure={
                     'data': instruction_data,
                     'layout': {
                         'title': 'Instruction Coverage',
                         'hovermode': 'closest',
                         'yaxis': dict(range=[0, 110])
                     }
                 }
         ),
        dc.Graph(id='branch_coverage',
                 figure={
                     'data': branch_data,
                     'layout': {
                         'title': 'Branch Coverage',
                         'hovermode': 'closest',
                         'yaxis': dict(range=[0, 110])
                     }
                 }
         ),
        dc.Interval(
            id='interval-component',
            interval=5 * 1000,  # in milliseconds
            n_intervals=0
        )
    ])
    app.run_server(host='0.0.0.0', port=8080)


if __name__ == "__main__":
    build_graph()