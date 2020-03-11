import datetime
import os
import requests
import pandas
import dash
import dash_core_components as dc
import dash_html_components as html
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
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


def download_latest_coverage():
    jacoco_artifact_csv = requests.get(LATEST_COVERAGE_URL)
    print("Latest Jacoco report was generated on " + jacoco_artifact_csv.headers['Last-Modified'])
    last_modified_time = datetime.datetime.strptime(jacoco_artifact_csv.headers['Last-Modified'], "%a, %d %b %Y %H:%M:%S %Z")

    with open('jacoco.csv', 'wb') as coverage_report:
        coverage_report.write(jacoco_artifact_csv.content)
        coverage_report.flush()
        print('Downloaded latest jacoco report from artifacts storage to local file')

    with open('jacoco.csv', "rb") as data:
        try:
            blob_name = 'daily/jacoco-' + str(last_modified_time.date()) + '.csv'
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(data, blob_type="BlockBlob")
            print('Backed up latest jacoco daily report ' + blob_name )
        except ResourceExistsError:
            print('Failed to back up latest jacoco daily report as the file already exists ' + blob_name)
            pass
    return 'jacoco.csv'


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


def transform_latest_coverage(latest_coverage):
    print('Transforming latest coverage data')
    coverage_data = pandas.read_csv(latest_coverage)
    coverage_data['GROUP'] = coverage_data['GROUP'].str.replace("Microsoft Azure Client Library - Test coverage/", "")
    coverage_data['TEST_DATE'] = datetime.date.today()
    coverage_data['INSTRUCTION_TOTAL'] = coverage_data['INSTRUCTION_MISSED'] + coverage_data['INSTRUCTION_COVERED']
    coverage_data['BRANCH_TOTAL'] = coverage_data['BRANCH_MISSED'] + coverage_data['BRANCH_COVERED']
    group_by = coverage_data.groupby(['GROUP', 'TEST_DATE'])
    coverage_df = group_by['INSTRUCTION_MISSED', 'INSTRUCTION_COVERED', 'INSTRUCTION_TOTAL', 'BRANCH_MISSED', 'BRANCH_COVERED', 'BRANCH_TOTAL'].sum()
    coverage_df['INSTRUCTION_COVERAGE'] = (coverage_df['INSTRUCTION_COVERED'] / coverage_df['INSTRUCTION_TOTAL']) * 100
    coverage_df['BRANCH_COVERAGE'] = (coverage_df['BRANCH_COVERED'] / coverage_df['BRANCH_TOTAL']) * 100
    print('Tranformation of latest coverage data completed successfully')
    return coverage_df.reset_index()


def append_latest_to_aggregate(aggregate_coverage_df, latest_coverage_df):
    if aggregate_report_df is None:
        print("No aggregate report found, returning the latest coverage as the final result")
        return latest_coverage_df

    frames = [aggregate_coverage_df, latest_coverage_df]
    final_result = pandas.concat(frames)
    print('Appended latest coverage report to aggregate report successfully')
    final_result['TEST_DATE'] = pandas.to_datetime(final_result['TEST_DATE'], format='%Y-%m-%d')
    final_result.drop_duplicates(subset=['GROUP', 'TEST_DATE'], inplace=True)
    return final_result


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


def upload_updated_coverage_report(coverage_report):
    coverage_report.to_csv('jacoco_aggregate.csv', index=False)
    archive_file = 'jacoco_aggregate_' + str(datetime.date.today()) + '.csv'
    coverage_report.to_csv(archive_file, index=False)

    with open('jacoco_aggregate.csv', "rb") as data:
        blob_client = container_client.get_blob_client(AGGREGATE_REPORT_NAME)
        blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)
        print('Uploaded aggregate report to aggregate/jacoco_aggregate.csv')

    with open(archive_file, "rb") as data:
        blob_client = container_client.get_blob_client('aggregate/' + archive_file)
        blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)
        print("Archived today's aggregate report " + 'aggregate/' + archive_file)


if __name__ == "__main__":
    today = datetime.datetime.strftime(datetime.datetime.today(), '%m/%d/%Y')
    print("Running test coverage report update on " + today)
    latest_coverage = download_latest_coverage()
    latest_coverage_df = transform_latest_coverage(latest_coverage)
    aggregate_report_df = get_aggregate_report()
    coverage_report = append_latest_to_aggregate(aggregate_report_df, latest_coverage_df)
    upload_updated_coverage_report(coverage_report)
    print("Completed test coverage report generation")

