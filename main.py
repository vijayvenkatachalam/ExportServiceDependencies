import requests
import csv
import json
import logging


def load_config(config_file='config.json'):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )


def get_graphql_data(endpoint, token, start_time, end_time):
    # Define the GraphQL query
    query = '''
    {
      entities(
        scope: "SERVICE"
        limit: 100
        between: {
          startTime: "%s"
          endTime: "%s"
        }
      ) {
        results {
          entityId: id
          name: attribute(expression: { key: "name" })
          duration: metric(expression: { key: "duration" }) {
            p99: percentile(size: 99) {
              value
              __typename
            }
            __typename
          }
          errorCount: metric(expression: { key: "errorCount" }) {
            avg {
              value
              __typename
            }
            __typename
          }
          numCalls: metric(expression: { key: "numCalls" }) {
            avg {
              value
              __typename
            }
            __typename
          }
          applicationType: attribute(expression: { key: "applicationType" })
          outgoingEdges_SERVICE: outgoingEdges(neighborType: SERVICE) {
            results {
              neighbor {
                entityId: id
                name: attribute(expression: { key: "name" })
                __typename
              }
              __typename
            }
            __typename
          }
          outgoingEdges_BACKEND: outgoingEdges(neighborType: BACKEND) {
            results {
              neighbor {
                entityId: id
                name: attribute(expression: { key: "name" })
                __typename
              }
              __typename
            }
            __typename
          }
          incomingEdges_SERVICE: incomingEdges(neighborType: SERVICE) {
            results {
              neighbor {
                entityId: id
                name: attribute(expression: { key: "name" })
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
    }
    ''' % (start_time, end_time)

    # Set up the headers with the Bearer token
    headers = {
        'Authorization': f'{token}',
        'Content-Type': 'application/json'
    }

    # Send the request to the GraphQL endpoint
    response = requests.post(endpoint, headers=headers, json={'query': query})

    if response.status_code == 200:
        logging.info("GraphQL query executed successfully.")
        return response.json()
    else:
        logging.error(f"Query failed with status code {response.status_code}.")
        raise Exception(f"Query failed to run by returning code of {response.status_code}. {query}")


def process_response_data(data):
    services_info = []

    # Parse through the response data
    for result in data['data']['entities']['results']:
        service_info = {
            'entityId': result['entityId'],
            'name': result['name'],
            'duration_p99': result['duration']['p99']['value'],
            'errorCount_avg': result['errorCount']['avg']['value'],
            'numCalls_avg': result['numCalls']['avg']['value'],
            'applicationType': result.get('applicationType', 'N/A'),
            'outgoingEdges_SERVICE': [edge['neighbor']['name'] for edge in result['outgoingEdges_SERVICE']['results']],
            'outgoingEdges_BACKEND': [edge['neighbor']['name'] for edge in result['outgoingEdges_BACKEND']['results']],
            'incomingEdges_SERVICE': [edge['neighbor']['name'] for edge in result['incomingEdges_SERVICE']['results']]
        }

        services_info.append(service_info)

    # Sort the services by numCalls_avg in descending order
    services_info.sort(key=lambda x: x['numCalls_avg'], reverse=True)

    return services_info


def export_to_csv(services_info, filename):
    # Define the CSV headers
    headers = [
        'Entity ID', 'Service Name', 'Duration P99', 'Error Count Avg',
        'Number of Calls Avg', 'Application Type', 'Outgoing Services',
        'Outgoing Backends', 'Incoming Services'
    ]

    # Write the data to a CSV file
    try:
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)

            for service in services_info:
                writer.writerow([
                    service['entityId'],
                    service['name'],
                    service['duration_p99'],
                    service['errorCount_avg'],
                    service['numCalls_avg'],
                    service['applicationType'],
                    ', '.join(service['outgoingEdges_SERVICE']),
                    ', '.join(service['outgoingEdges_BACKEND']),
                    ', '.join(service['incomingEdges_SERVICE'])
                ])
        logging.info(f"Service dependency information has been successfully exported to '{filename}'.")
    except Exception as e:
        logging.error(f"Failed to write CSV file: {e}")
        raise


def main():
    # Load configuration from file
    config = load_config()

    # Set up logging
    setup_logging(config.get("log_file", "service_dependency.log"))

    try:
        # Get data from GraphQL endpoint
        data = get_graphql_data(
            config['graphql_endpoint'],
            config['bearer_token'],
            config['start_time'],
            config['end_time']
        )

        # Process the data
        services_info = process_response_data(data)

        # Export data to CSV file
        export_to_csv(services_info, config['output_csv'])
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
