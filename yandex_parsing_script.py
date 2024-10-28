import requests
import time
import csv
import json
import os

api_key = 'api_key'  # Replace with your actual API key
api_url = f"https://tools.pixelplus.ru/api/fastcheck?key={api_key}"
input_file = 'input.csv'  # Input CSV file containing URL and queries
report_id_file = 'report_ids.csv'  # File to store report IDs and URLs
output_file = 'output.csv'  # Output CSV file to store the results
wait_time_between_requests = 10  # Time to wait between sending requests to server


# Function to read the input CSV and group queries by URL
def read_input_csv(file_path):
    url_queries = {}
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        for row in csv_reader:
            url = row['url']
            query = row['query']
            frequency = row.get('frequency of impressions', 'Not available')
            if url in url_queries:
                url_queries[url].append({'query': query, 'frequency': frequency})
            else:
                url_queries[url] = [{'query': query, 'frequency': frequency}]
    return url_queries


# Function to send POST request to the API and retrieve the report ID
def create_task(url, queries):
    requests_list = [query['query'] for query in queries]
    post_data = json.dumps({
        'url': url,
        'lr': 213,  # Region code by Yandex (required)
        'requests': requests_list  # Array of queries to check
    })
    headers = {'Content-Type': 'application/json'}
    retries = 3
    while retries > 0:
        response_post = requests.post(api_url, headers=headers, data=post_data)
        if response_post.status_code == 200:
            report_id = response_post.json().get('report_id')
            if report_id:
                print(f'Task created successfully for {url}. Report ID:', report_id)
                return report_id
            else:
                print(f'Error: Report ID not found in response for {url}.')
        else:
            print(f'Failed to create task for {url}. Retrying in 60 seconds... {response_post.text}')
        time.sleep(60)
        retries -= 1
    print(f'Failed to create task for {url} after multiple attempts.')
    return None


# Function to retrieve the task results from the API
def get_task_results(report_id):
    get_params = {
        'key': api_key,
        'report_id': report_id
    }

    # Attempt to get results with retries
    retries = 5
    while retries > 0:
        response_get = requests.get(api_url, params=get_params)
        if response_get.status_code == 200:
            response_json = response_get.json()
            if 'response' in response_json:
                return response_json
            else:
                print(f'Error: Expected key "response" not found in API response for report ID {report_id}.')
        else:
            print(
                f'Failed to retrieve task result for report ID {report_id}. Retrying in 60 seconds... {response_get.text}')
        time.sleep(60)
        retries -= 1
    print(f'Failed to retrieve task result for report ID {report_id} after multiple attempts.')
    return None


# Function to determine the label based on the position
def determine_label(position):
    if position <= 3:
        return "top-3"
    elif position <= 10:
        return "top-10"
    elif position <= 30:
        return "top-30"
    elif position <= 100:
        return "top-100"
    else:
        return "top-1000"


# Main function to orchestrate the process
def main():
    # Read input data from CSV
    url_queries = read_input_csv(input_file)

    # First loop: create tasks and store report IDs
    with open(report_id_file, mode='w', newline='', encoding='utf-8') as file:
        csv_writer = csv.writer(file, delimiter=';')
        csv_writer.writerow(['url', 'report_id'])  # Write header

        for url, queries in url_queries.items():
            report_id = create_task(url, queries)
            if report_id:
                csv_writer.writerow([url, report_id])
            time.sleep(wait_time_between_requests)  # Wait before the next request

    # Wait one minute between loops
    print("Waiting for one minute before retrieving reports...")
    time.sleep(60)

    # Second loop: retrieve reports using the report IDs
    with open(report_id_file, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        with open(output_file, mode='w', newline='', encoding='utf-8') as output_file_handle:
            # Write the header for the output file
            csv_writer = csv.writer(output_file_handle, delimiter=';')
            csv_writer.writerow(['url', 'query', 'position', 'frequency of impressions', 'label'])

            for row in csv_reader:
                url = row['url']
                report_id = row['report_id']

                # Get task results from API
                response_data = get_task_results(report_id)
                if response_data and 'response' in response_data:
                    for query, query_data in response_data['response']['queries'].items():
                        position = query_data.get('position', 'Not found')
                        frequency = next((q['frequency'] for q in url_queries[url] if q['query'] == query),
                                         'Not available')
                        if position == 'Not found':
                            label = 'N/A'
                        else:
                            label = determine_label(int(position))

                        print(
                            f"URL: {url}, Query: {query}, Position: {position}, Frequency: {frequency}, Label: {label}")
                        csv_writer.writerow([url, query, position, frequency, label])
                else:
                    print(f"Error: No valid data found for report ID {report_id}.")

                time.sleep(wait_time_between_requests)  # Wait before the next request

    # Clean up the temporary report IDs file after processing
    if os.path.exists(report_id_file):
        os.remove(report_id_file)
        print(f"Temporary file {report_id_file} has been deleted.")


if __name__ == "__main__":
    main()
