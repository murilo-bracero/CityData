import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.textio import ReadFromText

class RowToDict(beam.DoFn):
    def process(self, element):
        print(element)
        # return [{
        #     'id': Id,
        #     'sigla': sigla,
        #     'nome': nome,
        #     'regiao': regiao
        # }]


with beam.Pipeline(options=PipelineOptions()) as p:

    rows = (
        p 
        | 'Read CSV file' >> ReadFromText('./digest/ufs.csv', skip_header_lines=1)
        | 'Split data' >> beam.ParDo(RowToDict())
        | 'Print data' >> beam.Map(print)
    )


