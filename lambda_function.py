from __future__ import print_function
from datetime import datetime
import json
import boto3


def print_with_timestamp(*args):
    print(datetime.utcnow().isoformat(), *args)


def lambda_handler(event, context):
    print_with_timestamp('Starting - py-inbound-ses-spam-filter.py')

    ses_notification = event['Records'][0]['ses']
    message_id = ses_notification['mail']['messageId']
    receipt = ses_notification['receipt']

    print_with_timestamp('Processing message:', message_id)

    # Check if any spam check failed
    if (receipt['spfVerdict']['status'] == 'FAIL' or
            receipt['dkimVerdict']['status'] == 'FAIL' or
            receipt['spamVerdict']['status'] == 'FAIL' or
            receipt['virusVerdict']['status'] == 'FAIL'):

        send_bounce_params = {
            'OriginalMessageId': message_id,
            'BounceSender': 'mailer-daemon@<MYDOMAIN>.com',
            'MessageDsn': {
                'ReportingMta': 'dns; <MYDOMAIN>.com',
                'ArrivalDate': datetime.now().isoformat()
            },
            'BouncedRecipientInfoList': []
        }

        for recipient in receipt['recipients']:
            send_bounce_params['BouncedRecipientInfoList'].append({
                'Recipient': recipient,
                'BounceType': 'ContentRejected'
            })

        print_with_timestamp('Bouncing message with parameters:')
        print_with_timestamp(json.dumps(send_bounce_params))

        try:
            ses_client = boto3.client('ses')
            bounceResponse = ses_client.send_bounce(**send_bounce_params)
            print_with_timestamp("Bounce for message ", message_id, " sent, bounce message ID: ", bounceResponse['MessageId'])
            return {'disposition': 'stop_rule_set'}
        except Exception as e:
            print_with_timestamp(e)
            print_with_timestamp("An error occurred while sending bounce for message: ", message_id)
            raise e
    else:
        print_with_timestamp('Accepting message:', message_id)
