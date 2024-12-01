import grpc
from pyspark.sql.connect.client import ChannelBuilder
from pyspark.sql.connect.session import SparkSession


class CustomChannelBuilder(ChannelBuilder):
    def __init__(self, url: str):
        super().__init__(url)

    def toChannel(self) -> grpc.Channel:
        with open('./ca-cert-chain.crt', 'rb') as f:
            creds = grpc.ssl_channel_credentials(f.read())
        return grpc.secure_channel("example.iomete.com:443", creds)


sc_url = "sc://..."

print("> Creating session")

spark = (
    SparkSession
    .builder
    .channelBuilder(CustomChannelBuilder(sc_url))
    .getOrCreate()
)

print("> Session created")

spark.sql("show databases").show()
