import glob
import os
import pandas as pd
import xml.etree.ElementTree as ET


class ETL:
    def __init__(self, data_dir="data", target_file="transformed_data.csv"):
        self.data_dir = data_dir
        self.target_file = target_file

    def extract_from_csv(self, file_to_process):
        dataframe = pd.read_csv(file_to_process)
        return dataframe

    def extract_from_json(self, file_to_process):
        dataframe = pd.read_json(file_to_process, lines=True)
        return dataframe

    def extract_from_xml(self, file_to_process):
        dataframe = pd.DataFrame(columns=["name", "height", "weight"])
        tree = ET.parse(file_to_process)
        root = tree.getroot()
        for person in root:
            name = person.find("name").text
            height = float(person.find("height").text)
            weight = float(person.find("weight").text)
            dataframe = pd.concat([dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])],
                                  ignore_index=True)

        return dataframe

    def extract(self):
        extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

        # process all csv files, except the target file
        for csvfile in glob.glob(os.path.join(self.data_dir, "*.csv")):
            if os.path.basename(csvfile) != self.target_file:
                extracted_data = pd.concat([extracted_data, self.extract_from_csv(csvfile)], ignore_index=True)

        # process all json files
        for jsonfile in glob.glob(os.path.join(self.data_dir, "*.json")):
            extracted_data = pd.concat([extracted_data, self.extract_from_json(jsonfile)], ignore_index=True)

        # process all xml files
        for xmlfile in glob.glob(os.path.join(self.data_dir, "*.xml")):
            extracted_data = pd.concat([extracted_data, self.extract_from_xml(xmlfile)], ignore_index=True)

        return extracted_data

    def transform(self, data):
        data["height"] = round(data.height * 0.0254, 2)
        data["weight"] = round(data.weight * 0.453592, 2)
        return data

    def load(self, transformed_data):
        transformed_data.to_csv(os.path.join(self.data_dir, self.target_file), index=False)
