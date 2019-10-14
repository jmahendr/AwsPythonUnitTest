import unittest
import os
from src.lambda_function import readS3
from moto import mock_s3
import boto3
import json


@mock_s3
class TestExtract(unittest.TestCase):
    DIR_PATH = os.path.dirname(os.path.realpath(__file__))

    #set up env constants
    ENV_STATE_MC_ARN = "stateMcArn"
    vots_bucket = "vots_bucket"
    landata_bucket = "landata_bucket"
    vots_file_pattern = "vots_file_pattern"
    landata_file_pattern = "landata_file_pattern"

    SOURCE_BUCKET = "source_bucket"
    DEST_BUCKET = "destination_bucket"
    DEST_KEY = "s3/key/"



    def setUp(self):
        #set up environment variables for lambada
        os.environ[] = "stateMcArn"
        os.environ[TestExtract.ENV_DEST_KEY] = TestExtract.DEST_KEY
        os.environ[TestExtract.ENV_DEST_BUCKET] = TestExtract.DEST_BUCKET

        conn = boto3.resource('s3')
        conn.create_bucket(Bucket=TestExtract.SOURCE_BUCKET)
        conn.create_bucket(Bucket=TestExtract.DEST_BUCKET)

    def tearDown(self):
        del os.environ[TestExtract.ENV_DEST_KEY]
        del os.environ[TestExtract.ENV_DEST_BUCKET]

        self.remove_bucket(TestExtract.SOURCE_BUCKET)
        self.remove_bucket(TestExtract.DEST_BUCKET)

    @staticmethod
    def remove_bucket(bucket_name):
        s3_bucket = boto3.resource('s3').Bucket(bucket_name)
        s3_bucket.objects.all().delete()
        s3_bucket.delete()

    @staticmethod
    def get_s3_event(bucket, key):
        return {
           'Records':[
              {
                 'eventVersion':'2.1',
                 'eventSource':'aws:s3',
                 'awsRegion':'ap-southeast-2',
                 'eventTime':'2019-10-14T05:19:19.412Z',
                 'eventName':'ObjectCreated:Put',
                 'userIdentity':{
                    'principalId':'AWS:AROA5UL7ZBLQV2CGOICTB:joshua.francis@victorianlrs.com.au'
                 },
                 'requestParameters':{
                    'sourceIPAddress':'203.221.75.242'
                 },
                 'responseElements':{
                    'x-amz-request-id':'C66DF3912A38B137',
                    'x-amz-id-2':'C+g2KFr/hdP7HlbBKNOXXAyLv6coFXzFTTu+nFI3bakziVNNuSMyj4ipMRvRuQN9RwLqOqArdp4='
                 },
                 's3':{
                    's3SchemaVersion':'1.0',
                    'configurationId':'6bebe8b6-d215-4599-b924-d1c3ec251977',
                    'bucket':{
                       'name':'vots-int-finance-int',
                       'ownerIdentity':{
                          'principalId':'A5J5GMO30M2VY'
                       },
                       'arn':'arn:aws:s3:::vots-int-finance-int'
                    },
                    'object':{
                       'key':'vots_20190923161143.txt',
                       'size':4400,
                       'eTag':'0b7a9cc0d574abbe5daa2ac20ac4c5d5',
                       'sequencer':'005DA405575C234096'
                    }
                 }
              }
           ]
        }

    @staticmethod
    def read_object_bytewise(path):
        return open(path, 'rb')

    @staticmethod
    def put_data_to_s3_object(object_, bucket, s3_source_key):
        boto3.client('s3').put_object(Body=object_,
                                      Bucket=bucket, Key=s3_source_key)

    @staticmethod
    def get_s3_object(bucket, key):
        return boto3.client('s3').get_object(Bucket=bucket, Key=key)

    @staticmethod
    def read_s3_object(bucket, key):
        object_ = TestExtract.get_s3_object(bucket, key)
        return object_['Body'].read()

    def put_email_to_s3(self, test_email_path, email_name):
        with self.read_object_bytewise(test_email_path) as email_object:
            self.put_data_to_s3_object(email_object,
                                       TestExtract.SOURCE_BUCKET, email_name)

    def test_extract_the_contents_of_an_email_successfully(self):
        # arrange
        email_name = 'test_extract_the_contents_of_an_email_successfully.eml'
        test_email_path = os.path.join(self.DIR_PATH, 'resources', email_name)
        self.put_email_to_s3(test_email_path, email_name)

        event = self.get_s3_event(TestExtract.SOURCE_BUCKET, email_name)

        # act
        s3_key_extracted_message = readS3.handler(event, None)

        # assert
        email_as_json = self.read_s3_object(TestExtract.DEST_BUCKET, s3_key_extracted_message)
        expected_json = {'id': 'test_extract_the_contents_of_an_email_successfully.eml',
                         'from': 'vclaes1986@gmail.com',
                         'to': 'vincent.v.claes@gmail.com',
                         'cc': '', 'subject': 'Hey how are you doing',
                         'date': '2019-07-09 13:42:54+02:00',
                         'body': '\nCash Me Outside How Bout Dah'}
        self.assertDictEqual(json.loads(email_as_json), expected_json)


if __name__ == '__main__':
    unittest.main()