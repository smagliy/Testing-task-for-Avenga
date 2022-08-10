from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pathlib import Path

spark = SparkSession.builder.appName('Testing task').getOrCreate()


class ParserTripData(object):
    def __init__(self):
        self.df = spark.read.option('header', 'true').csv('taxi_tripdata.csv')\
            .na.drop(how='any', subset=['VendorID'])
        self.header_result = ['Vendor', 'Payment Type', 'Payment Rate', 'Next Payment Rate',
                              'Max Payment Rate', 'Percents to next rate']
        self.dict_vendorsid = {1: 'Creative Mobile Technologies, LLC', 2: 'VeriFone Inc'}
        self.dict_patmenttypeid = {1: 'Credit card', 2: 'Cash', 3: 'No charge',
                                   4: 'Dispute', 5: 'Unknown', 6: 'Voided trip'}
        self.path = str(Path.cwd()) + '/result'

    def main(self):
        self.add_all_records_in_dataframe_and_save_in_csv()

    def add_all_records_in_dataframe_and_save_in_csv(self):
        df = spark.createDataFrame(self.all_records(), self.header_result)
        df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save(self.path)
        return df

    def all_records(self):
        list_records = list()
        for id_v in self.dict_vendorsid.keys():
            for id_type in self.dict_patmenttypeid.keys():
                if self.parse_on_vendor_and_payment_type(id_v, id_type):
                    list_records.append(self.parse_on_vendor_and_payment_type(id_v, id_type))
        return list_records

    def parse_on_vendor_and_payment_type(self, id_vendor: int, id_paymenttype: int):
        df_parse = self.df.filter((self.df['VendorID'] == id_vendor) &
                                  (self.df['payment_type'] == id_paymenttype))
        payment_rate = df_parse.withColumn('Payment Rate', col('fare_amount')/col(
            'trip_distance')).groupBy().mean('Payment Rate').collect()[0]['avg(Payment Rate)']
        next_payment_rate = df_parse.withColumn('Next Payment Rate', (col(
            'fare_amount') + col('extra') + col('extra') + col('mta_tax') + col(
            'improvement_surcharge'))/col('trip_distance')).groupBy().mean(
            'Next Payment Rate').collect()[0]['avg(Next Payment Rate)']
        max_payment_rate = df_parse.withColumn('Max Payment Rate', col('total_amount')/col(
            'trip_distance')).groupBy().mean('Max Payment Rate').collect()[0]['avg(Max Payment Rate)']
        try:
            percent_to_next_rate = 100 - ((100 * payment_rate) / next_payment_rate)
            return (self.dict_vendorsid[id_vendor], self.dict_patmenttypeid[id_paymenttype],
                    payment_rate, next_payment_rate, max_payment_rate, percent_to_next_rate)
        except Exception:
            print(f'In {self.dict_vendorsid[id_vendor]} does '
                  f'not have {self.dict_patmenttypeid[id_paymenttype]} records')


if __name__ == '__main__':
    ParserTripData().main()

