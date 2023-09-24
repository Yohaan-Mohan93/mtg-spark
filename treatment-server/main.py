from pyspark.sql import SparkSession
from factory import Factory
import sys


if __name__ == "__main__":
    website = sys.argv[1]
    session = SparkSession.builder.appName('mtg-treatment-server').getOrCreate()
    factory = Factory()
    treatment_server = factory.get_factory(website)
    if sys.argv[2] == 'load':
        treatment_server.load_non_foil_data(session, sys.argv[3])
    elif sys.argv[2] == 'foil_load':
        treatment_server.load_foil_data(session, sys.argv[3])
