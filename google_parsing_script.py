import requests
import time
import csv
import json

api_key = 'api_key'  # Replace with your actual API key
api_url = f"https://tools.pixelplus.ru/api/fastcheckgoogle?key={api_key}"
input_file = 'input.csv'  # Input CSV file containing URL and queries
output_file = 'output.csv'  # Output CSV file to store the results
wait_time_between_requests = 10  # Time to wait between sending requests to server
retry_delay = 60  # Delay in seconds before retrying when server issues occur

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
        'requests': requests_list,  # Array of queries to check
        'search_engine': 'google.ru'
    })
    headers = {'Content-Type': 'application/json'}

    print(f"Sending to server for URL {url}: {post_data}")  # Print JSON being sent

    retries = 3
    while retries > 0:
        response_post = requests.post(api_url, headers=headers, data=post_data)
        print(f"Received from server for URL {url}: {response_post.text}")  # Print server response

        if response_post.status_code == 200:
            report_id = response_post.json().get('report_id')
            if report_id:
                print(f'Task created successfully for {url}. Report ID:', report_id)
                return report_id
            else:
                print(f'Error: Report ID not found in response for {url}. Retrying in {retry_delay} seconds...')
        else:
            print(f'Failed to create task for {url}. Retrying in {retry_delay} seconds... {response_post.text}')
        time.sleep(retry_delay)
        retries -= 1
    print(f'Failed to create task for {url} after multiple attempts.')
    return None

# Function to retrieve the task results from the API
def get_task_results(report_id):
    get_params = {
        'key': api_key,
        'report_id': report_id
    }

    retries = 5
    while retries > 0:
        print(f"Requesting results for report ID: {report_id} with params: {get_params}")  # Print GET request details
        response_get = requests.get(api_url, params=get_params)
        print(f"Received result for report ID {report_id}: {response_get.text}")  # Print server response

        if response_get.status_code == 200:
            response_json = response_get.json()
            if 'response' in response_json:
                return response_json
            else:
                print(f'Error: Expected key "response" not found in API response for report ID {report_id}. Retrying in {retry_delay} seconds...')
        else:
            print(f'Failed to retrieve task result for report ID {report_id}. Retrying in {retry_delay} seconds... {response_get.text}')
        time.sleep(retry_delay)
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

    # Open output file to write results
    with open(output_file, mode='w', newline='', encoding='utf-8') as output_file_handle:
        csv_writer = csv.writer(output_file_handle, delimiter=';')
        csv_writer.writerow(['url', 'query', 'position', 'frequency of impressions', 'label'])  # Write header

        # Loop through each URL and send tasks individually
        for url, queries in url_queries.items():
            # Create task and get report ID
            report_id = create_task(url, queries)
            if report_id:
                # Wait before requesting the report data
                print("Waiting before retrieving report data...")
                time.sleep(wait_time_between_requests)

                # Retrieve and process task results
                response_data = get_task_results(report_id)
                if response_data and 'response' in response_data:
                    # Ensure queries match URL
                    correct_queries = [q['query'] for q in url_queries[url]]

                    for query, query_data in response_data['response']['queries'].items():
                        # Validate that query belongs to the current URL
                        if query not in correct_queries:
                            print(f"Warning: Query '{query}' does not belong to URL {url}. Skipping.")
                            continue

                        position = query_data.get('position', 'Not found')
                        frequency = next((q['frequency'] for q in url_queries[url] if q['query'] == query),
                                         'Not available')
                        label = determine_label(int(position)) if position != 'Not found' else 'N/A'

                        print(f"URL: {url}, Query: {query}, Position: {position}, Frequency: {frequency}, Label: {label}")
                        csv_writer.writerow([url, query, position, frequency, label])
                else:
                    print(f"Error: No valid data found for report ID {report_id}.")
            time.sleep(wait_time_between_requests)  # Wait before the next task

if __name__ == "__main__":
    main()
