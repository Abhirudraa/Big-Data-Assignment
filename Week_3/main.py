import functions_framework
from google.cloud import storage


@functions_framework.cloud_event
def find_longest_lines(cloud_event):
    # get info of file from gcs
    data = cloud_event.data

    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"{file_name}")
    print(f"{bucket_name}")
    if file_name == "output.txt":
        print("Ignoring output.txt.")
        return

    # Connect to Google Cloud Storage
    storage_client = storage.Client()

    # Get the bucket and uploaded file
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download the uploaded file as text
    content = blob.download_as_text()

    # Split the file into individual lines
    lines = content.splitlines()

    # Variables used to find the longest line(s)
    max_length = -1
    longest_lines = []

    # go through evry line
    for line in lines:
        line_length = len(line)

        if line_length > max_length:
            max_length = line_length
            longest_lines = [line]

        elif line_length == max_length:
            longest_lines.append(line)

    #  output text
    output = f"Longest line length: {max_length}\n"
    output += "Longest line(s):\n"

    for line in longest_lines:
        output += line + "\n"

    # print results to Cloud Logging
    print(f"Longest line length: {max_length}")
    print("Longest line(s):")

    for line in longest_lines:
        print(line)

    # Save the results
    output_blob = bucket.blob("output.txt")
    output_blob.upload_from_string(
        output,
        content_type="text/plain"
    )
