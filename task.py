from google.cloud import storage
BUCKET_NAME = "week1-longest-line-84721"
INPUT_FILE = "test.txt"
OUTPUT_FILE = "output.txt"

# cloud connection

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
print("Connected to GCS bucket:", BUCKET_NAME)


input_blob = bucket.blob(INPUT_FILE)
input_blob.download_to_filename(INPUT_FILE)
print("Downloaded:", INPUT_FILE)

# longest line
max_length = -1
longest_lines = []

with open(INPUT_FILE, "r", encoding="utf-8") as file:

    for line in file:

        # Remove only newline characters
        line = line.rstrip("\r\n")

        # Calculate character length
        current_length = len(line)

        if current_length > max_length:
            max_length = current_length
            longest_lines = [line]

        elif current_length == max_length:
            longest_lines.append(line)

print("Longest line length:", max_length)

for line in longest_lines:
    print(line)

# output

with open(OUTPUT_FILE, "w", encoding="utf-8") as file:

    file.write(f"Longest line length: {max_length}\n")
    file.write("Longest line(s):\n")

    for line in longest_lines:
        file.write(line + "\n")

print("Output file created:", OUTPUT_FILE)
output_blob = bucket.blob(OUTPUT_FILE)
output_blob.upload_from_filename(OUTPUT_FILE)

