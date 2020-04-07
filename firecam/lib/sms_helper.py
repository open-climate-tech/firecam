# Copyright 2018 The Fuego Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""
Send SMS (phone text messages)

"""
from twilio.rest import Client
import logging
import time

def sendSms(settings, toNumber, message, attachments=[]):
    """Send SMS (phone text) message to given number using Twilio API

    Args:
        settings: settings module with pointers to credential files
        toNumber (str): Phone number in '+1...' format
        message (str): Message body
        attachments (list): optional list of attachements files

    Returns:
        Twilio API result
    """
    if not sendSms.client:
        sendSms.client = Client(settings.twilioAccountSid, settings.twilioAuthToken)

    retriesLeft = 5
    while retriesLeft > 0:
        retriesLeft -= 1
        try:
            message = sendSms.client.messages.create(from_ = settings.smsFromNumber, to = toNumber,
                                                     body = message, media_url = attachments)
            return message
        except Exception as e:
            logging.error('Error sending sms. %d retries left. %s', retriesLeft, str(e))
            if retriesLeft > 0:
                time.sleep(5) # wait 5 seconds before retrying
    logging.error('Too many sms send failures')
sendSms.client = None
