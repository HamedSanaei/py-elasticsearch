import csv
import json

class CsvToJson:
    def  convert(self):
        csvPath= "data/QueryResults.csv"
        jsonPath="data/QueryQueryResults.json"
        data = []
        with open(csvPath) as csvFile:
            csvReader = csv.DictReader(csvFile)
            for rows in csvReader:
                id = rows['Id']
                data.append(rows)
            

        # create new json File and Write data on it
        with open(jsonPath, 'w') as jsonFile:
            # make it more readable and pretty
            jsonFile.write(json.dumps(data,indent=4))

    @classmethod
    def convertToArray(cls,csvFilePath):
        csvPath= csvFilePath
        jsonPath="data/QueryQueryResults.json"
        data = []
        with open(csvPath) as csvFile:
            csvReader = csv.DictReader(csvFile)
            for rows in csvReader:
                id = rows['Id']
                data.append(rows)
        return data

