import yaml

class ParseYaml:

    @staticmethod   
    def parse(file_path):
        with open(file_path, 'r') as file:
            try:
                return yaml.safe_load(file)
            except yaml.YAMLError as exc:
                print(exc)