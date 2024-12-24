import yaml


def on_send_success(record_metadata):
    print(
        f"Message published successfully to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")


def on_send_error(exception):
    print(f"Error publishing message: {exception}")


try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


def config_reader(path):
    try:
        with open(path, 'r') as stream:
            dictionary = yaml.safe_load(stream)
            if dictionary is not None:
                secret = dictionary.get("crypto").get("api_token")
                return secret
            else:
                print("YAML file is empty or invalid.")
    except FileNotFoundError:
        print("The specified YAML file was not found.")
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
