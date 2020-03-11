import os
import dash
import dash_core_components as dc
import dash_html_components as html
import pandas
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

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


def get_aggregate_report():
    # Instantiate a new BlobClient
    blob_client = container_client.get_blob_client(AGGREGATE_REPORT_NAME)
    try:
        blob_client.get_blob_properties()
    except ResourceNotFoundError:
        print("Aggregate report does not exist")
        return None

    with open('jacoco-aggregate.csv', "wb") as aggregate_report:
        download_stream = blob_client.download_blob()
        aggregate_report.write(download_stream.readall())
        aggregate_report.flush()
        print('Downloaded aggregate coverage report from blob storage to local file')


    result_df = pandas.read_csv('jacoco-aggregate.csv')
    result_df['TEST_DATE'] = pandas.to_datetime(result_df['TEST_DATE'], format='%Y-%m-%d')
    return result_df


def build_graph():
    x_values = list(coverage_report.groupby('TEST_DATE').groups.keys())
    instruction_df = coverage_report.groupby('GROUP').INSTRUCTION_COVERAGE.apply(list).reset_index()
    branch_df = coverage_report.groupby('GROUP').BRANCH_COVERAGE.apply(list).reset_index()
    app = dash.Dash()
    instruction_data = []
    branch_data = []
    for i in range(len(instruction_df)):
        instruction_data.append(
            {'x': x_values, 'y': instruction_df.iloc[i, 1], 'type': 'line', 'name': instruction_df.iloc[i, 0],
             'hoverlabel': dict(namelength=-1)})
        branch_data.append({'x': x_values, 'y': branch_df.iloc[i, 1], 'type': 'line', 'name': branch_df.iloc[i, 0],
                            'hoverlabel': dict(namelength=-1)})
    app.layout = html.Div(children=[
        html.H1("Azure SDK for Java - Code Coverage"),

        dc.Graph(id='test',
                 figure={
                     'data': instruction_data,
                     'layout': {
                         'title': 'Instruction Coverage',
                         'hovermode': 'closest',
                         'yaxis': dict(range=[0, 100])
                     }
                 }
                 ),
        dc.Graph(id='test2',
                 figure={
                     'data': branch_data,
                     'layout': {
                         'title': 'Branch Coverage',
                         'hovermode': 'closest',
                         'yaxis': dict(range=[0, 100])
                     }
                 }
                 )
    ])
    app.run_server(debug=True)



if __name__ == "__main__":
    coverage_report = get_aggregate_report()
    build_graph()