from flask import Flask, Response, request, jsonify
import matplotlib.pyplot as plt
from prometheus_api_client import PrometheusConnect
from io import BytesIO
import logging
from datetime import datetime, timedelta
import re
from PIL import Image

app = Flask(__name__)
prom = PrometheusConnect(url="http://localhost:1000", disable_ssl=True)

# Setup logging
logging.basicConfig(level=logging.DEBUG)

def parse_time(value):
    """Parse a time string like '2min', '3sec', '5day', '1year' into a timedelta object."""
    pattern = r'(\d+)([a-z]+)'
    match = re.match(pattern, value)
    if not match:
        raise ValueError(f"Invalid time format: {value}")
    
    amount, unit = int(match.group(1)), match.group(2)
    
    if unit == 'sec':
        return timedelta(seconds=amount)
    elif unit == 'min':
        return timedelta(minutes=amount)
    elif unit == 'h':
        return timedelta(hours=amount)
    elif unit == 'day':
        return timedelta(days=amount)
    elif unit == 'year':
        return timedelta(days=amount * 365)
    else:
        raise ValueError(f"Unsupported time unit: {unit}")

@app.route('/')
def home():
    return "Welcome to the Prometheus Matplotlib Graph Server!"

@app.route('/graph')
def graph():
    query = request.args.get('query')
    start_time = request.args.get('start', '1min')
    end_time = request.args.get('end', 'now')
    title = request.args.get('title', 'Prometheus Query Result')  # Default title if not provided
    width = request.args.get('width', 14, type=int)
    height = request.args.get('height', 8, type=int)
    xlabel = request.args.get('xlabel', 'Time')
    ylabel = request.args.get('ylabel', 'Value')
    legend = request.args.get('legend', 'true').lower() == 'true'

    if not query:
        return "Please provide a Prometheus query using the 'query' parameter."

    try:
        # Convert start_time and end_time to datetime objects
        if start_time == 'now':
            start_time = datetime.now()
        else:
            start_time = datetime.now() - parse_time(start_time)

        if end_time == 'now':
            end_time = datetime.now()
        else:
            end_time = datetime.now() - parse_time(end_time)

        # Fetch data from Prometheus
        data = prom.custom_query_range(
            query=query,
            start_time=start_time,
            end_time=end_time,
            step='60s'
        )

        if not data:
            app.logger.error(f"No data returned from Prometheus for query: {query}")
            return jsonify({"error": "No data returned from Prometheus"}), 500

        # Create the graph with the specified figure size
        fig, ax = plt.subplots(figsize=(width, height))

        # Loop through each series in the data
        for series in data:
            times = [datetime.fromtimestamp(value[0]) for value in series['values']]
            values = [float(value[1]) for value in series['values']]

            # Find the first name which is not 'instance' or 'job'
            label = 'Unknown'
            for key in series['metric']:
                if key not in ['instance', 'job']:
                    label = series['metric'][key]
                    break

            ax.plot(times, values, label=label)

        ax.set(xlabel=xlabel, ylabel=ylabel, title=title)  # Set the custom title
        ax.grid()

        # Automatically adjust x-axis labels to prevent overlap
        fig.autofmt_xdate(rotation=45)

        # Save the plot without legend
        graph_output = BytesIO()
        plt.savefig(graph_output, format='png', bbox_inches='tight')
        graph_output.seek(0)
        
        # Now add the legend and save only the legend
        fig_legend = plt.figure(figsize=(width, 2))
        ax_legend = fig_legend.add_subplot(111)
        ax_legend.legend(*ax.get_legend_handles_labels(), loc='center', frameon=True)
        ax_legend.axis('off')

        legend_output = BytesIO()
        plt.savefig(legend_output, format='png', bbox_inches='tight')
        legend_output.seek(0)

        # Combine the graph and the legend
        graph_img = Image.open(graph_output)
        legend_img = Image.open(legend_output)
        combined_img = graph_img

        # Create a new image with a white background
        if legend:
            total_height = graph_img.height + legend_img.height
            combined_img = Image.new('RGB', (graph_img.width, total_height), "white")
            combined_img.paste(graph_img, (0, 0))
            combined_img.paste(legend_img, (int((graph_img.width/2)-(legend_img.width/2)), graph_img.height))

        # Save the combined image to BytesIO
        combined_output = BytesIO()
        combined_img.save(combined_output, format='png')
        combined_output.seek(0)

        return Response(combined_output, mimetype='image/png')
    except Exception as e:
        app.logger.error(f"An unexpected error occurred: {e}")
        return jsonify({"error": "An unexpected error occurred"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
