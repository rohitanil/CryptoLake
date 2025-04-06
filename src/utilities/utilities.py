def on_send_success(record_metadata):
    print(
        f"Message published successfully to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")


def on_send_error(exception):
    print(f"Error publishing message: {exception}")

