from kernel_client import KernelLauncher, Kernel

class SentimentProvider:
    def __init__(self, enterprise_gateway_host, kernelspec):
        self.enterprise_gateway_host = enterprise_gateway_host
        self.kernelspec = kernelspec

        self.launcher = None
        self.kernel = None

        self.initialized = False

    def start(self):
        self.launcher = KernelLauncher(self.enterprise_gateway_host)
        self.kernel = self.launcher.launch(self.kernelspec)
        self.initialized = True

        self._load_data()

    def stop(self):
        self.launcher.shutdown(self.kernel.kernel_id)


    def _load_data(self):
        # load list of business establishments

        #self.kernel.execute("business = spark.read.option('header', 'true').option('inferSchema', 'true').csv('file:///opt/data/yelp_business.csv')")
        #self.kernel.execute("business_small = business.filter(business.city == 'Brooklyn')")
        #self.kernel.execute("reviews = spark.read.option('header', 'true').option('inferSchema', 'true').csv('file:///opt/data/yelp_review.csv')")
        #self.kernel.execute("business_reviews = business_small.join(reviews, business_small.business_id == reviews.business_id, 'leftouter')")

        self.kernel.execute("city_business_reviews = spark.read.parquet('yelp/reviews')")
        print('>>> Data load finished')

    def calculate_sentiment(self, business_id):
        code = []
        code.append("from pyspark.sql.functions import udf, col")
        code.append("from pyspark.sql.types import DoubleType")

        code.append("@udf(returnType=DoubleType())")
        code.append("def afinn_score(text):")
        code.append("    from afinn import Afinn")
        code.append("    afinn = Afinn()")
        code.append("    if text:")
        code.append("        return afinn.score(text)")
        code.append("    else:")
        code.append("        return 0")

        code.append("sentiment = city_business_reviews.filter(city_business_reviews.business_id == '{}').withColumn('sentiment', afinn_score(col('text')))".format(business_id))

        code.append("sentiment.select('date', 'text', 'sentiment').show()")

        sentiment = self.kernel.execute('\n'.join(code))

        print(">>> Sentiment is ready")
        return sentiment

# provider = None
#
# try:
#     provider = SentimentProvider('lresende-elyra:8888', 'spark_python_yarn_cluster')
#     provider.start()
#
#     sentiment = provider.calculate_sentiment('fzPwuU8295tpDUyCFaIGxw')
#     print('')
#     print('>>>>>>>')
#     print(sentiment)
# finally:
#     if provider:
#         provider.stop()
