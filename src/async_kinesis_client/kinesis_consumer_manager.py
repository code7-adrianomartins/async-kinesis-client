import time
import logging
import string
import random

log = logging.getLogger(__name__)


class consumerManager():
    def __init__(self, stream_name, stream_arn, boto3_session, consumer_name):
        self.stream_name = stream_name
        self.boto3_session = boto3_session
        self.stream_arn = stream_arn
        self.random_consumer_name = f'{consumer_name}_{self.radom_generator()}'

    def radom_generator(self):
        length = 5
        caracteres = string.ascii_lowercase + string.ascii_uppercase + string.digits
        name = "".join(random.sample(caracteres, length))
        return name

    async def consumer_exists(self):
        async with self.boto3_session.client('kinesis') as client:
            try:
                response = await client.describe_stream_consumer(
                    StreamARN=self.stream_arn,
                    ConsumerName=self.random_consumer_name
                )
                return True
            except client.exceptions.ResourceNotFoundException:
                return False

    async def register_consumer(self, force=False):
        # check if exists
        async with self.boto3_session.client('kinesis') as client:
            if await self.consumer_exists():
                response = await client.describe_stream_consumer(
                    StreamARN=self.stream_arn,
                    ConsumerName=self.random_consumer_name
                )
                consumer_arn = await response['ConsumerDescription']['ConsumerARN']
                log.debug(f'Consumer {consumer_arn} exists')
                if force:
                    await self.deregister_consumer()
                else:
                    return consumer_arn, self.random_consumer_name

            # register consumer
            response = await client.register_stream_consumer(
                StreamARN=self.stream_arn,
                ConsumerName=self.random_consumer_name
            )
            consumer_arn = response['Consumer']['ConsumerARN']
            consumer_status = response['Consumer']['ConsumerStatus']

            # wait until consumer active
            while consumer_status != 'ACTIVE':
                time.sleep(5)
                response = await client.describe_stream_consumer(
                    StreamARN=self.stream_arn,
                    ConsumerName=self.random_consumer_name
                )
                consumer_status = response['ConsumerDescription']['ConsumerStatus']

            return consumer_arn, self.random_consumer_name

    async def deregister_consumer(self):
        async with self.boto3_session.client('kinesis') as client:        
            response = await client.deregister_stream_consumer(
                StreamARN=self.stream_arn,
                ConsumerName=self.random_consumer_name
            )

            while self.consumer_exists():
                time.sleep(2)
