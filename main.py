import json
import time

import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.textio import ReadFromText

class RowToDict(beam.DoFn):
    def process(self, element):
        Id, sigla, nome, regiao = element.split(';')
        yield {
            'id': Id,
            'acronym': sigla,
            'name': nome,
            'region': regiao
        }

class Desserialize(beam.DoFn):
    def process(self, element):
        acceptable_string = element['region'].replace("'", '"')

        element['region'] = json.loads(acceptable_string)

        yield element

class Tablerize(beam.DoFn):
    def process(self, element):
        region_id = element['region']['id']
        region_name = element['region']['nome']
        region_acronym = element['region']['sigla']

        element['region_id'] = region_id
        element['region_name'] = region_name
        element['region_acronym'] = region_acronym

        del element['region']

        yield element

class JsonObject():
    def __init__(self) -> None:
        self.objects = []

    def append(self, obj: dict) -> None:
        self.objects.append(obj)

    def to_file(self, path: str):
        with open(path, 'w+', encoding='utf8') as file:
            json.dump(self.objects, file, ensure_ascii=False)

with beam.Pipeline(options=PipelineOptions()) as p:

    json_object = JsonObject()
    filename = f'output/{time.time()}_ufs.json'

    rows = (
        p 
        | 'Read CSV file' >> ReadFromText('./digest/ufs.csv', skip_header_lines=1)
        | 'Split data' >> beam.ParDo(RowToDict())
        | 'Convert region to dict' >> beam.ParDo(Desserialize())
        | 'Tablerize current dict' >> beam.ParDo(Tablerize())
        | 'Append to JSON object' >> beam.Map(lambda obj: json_object.append(obj))
        | 'Save to File' >> beam.Map(lambda _: json_object.to_file(filename))
    )


