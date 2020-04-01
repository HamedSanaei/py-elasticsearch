import csv
import json
from datetime import datetime
from pathlib import Path


class CsvToJson:
    def convert(self):
        csvPath = "data/QueryResults.csv"
        jsonPath = "data/QueryResults.json"
        data = []
        with open(csvPath) as csvFile:
            csvReader = csv.DictReader(csvFile)
            for rows in csvReader:
                # id = rows['Id']
                data.append(rows)

        # create new json File and Write data on it
        with open(jsonPath, 'w') as jsonFile:
            # make it more readable and pretty
            jsonFile.write(json.dumps(data, indent=4))

    @classmethod
    def convertToArray(cls, csvFilePath):
        csvPath = csvFilePath
        # jsonPath="data/QueryResults.json"
        data = []
        with open(csvPath) as csvFile:
            csvReader = csv.DictReader(csvFile)
            for rows in csvReader:
                # id = rows['Id']
                data.append(rows)
        return data

    @classmethod
    def convertToArrayDictionary(cls, jsonPath):
        data = []
        with open(jsonPath.absolute()) as f:
            data = json.load(f)
        return data

    @classmethod
    def convertScoreStringToint(cls, jsonPath):
        data = []
        with open(jsonPath) as f:
            data = json.load(f)
            for d in data:
                d['Score'] = int(d['Score'])

        with open(jsonPath, 'w') as jsonFile:
            # make it more readable and pretty
            jsonFile.write(json.dumps(data, indent=4))

    @classmethod
    def convertParentIdStringToInt(cls, jsonPath):
        data = []
        with open(jsonPath) as f:
            data = json.load(f)
            for d in data:
                d['ParentId'] = int(d['ParentId'])

        with open(jsonPath, 'w') as jsonFile:
            # make it more readable and pretty
            jsonFile.write(json.dumps(data, indent=4))

    @classmethod
    def convertCreationDateStringToInt(cls, jsonPath):
        data = []
        with open(jsonPath) as f:
            data = json.load(f)
            for d in data:
                d['CreationDate'] = int(datetime.strptime(
                    d['CreationDate'], '%Y-%m-%d %H:%M:%S').utcnow().timestamp())*1000

        with open(jsonPath, 'w') as jsonFile:
            # make it more readable and pretty
            jsonFile.write(json.dumps(data, indent=4))

    @classmethod
    def convertCreationDateStringTomilisecond(cls, jsonPath):
        data = []
        with open(jsonPath) as f:
            data = json.load(f)
            for d in data:
                d['CreationDate'] = d['CreationDate']*1000

        with open(jsonPath, 'w') as jsonFile:
            # make it more readable and pretty
            jsonFile.write(json.dumps(data, indent=4))


# dd = CsvToJson()
# dd.convertCreationDateStringTomilisecond("data/QueryResults2.json")
