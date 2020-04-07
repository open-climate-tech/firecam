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
Send and receive email

"""
import smtplib
import imaplib
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pathlib
import logging
import base64
import time
from googleapiclient.http import MediaIoBaseUpload
from io import BytesIO


def addAttachments(msg, attachments):
    """Add given attachements to the given email message as MIME parts

    Args:
        msg: MIME email message
        attachments (list): list of attachements files
    """
    for filePath in attachments:
        #convert image to base64 encoding
        attachFh = open(filePath, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachFh).read())
        encoders.encode_base64(part)
        filename = pathlib.PurePath(filePath).name
        filename = filename.replace(';','_') # gmail considers ';' to mark end of filename
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        msg.attach(part)


def createMimeMsg(fromAddr, toAddrs, bccAddrs, subject, body):
    """Create a MIME formatted message with given from,to,bcc,subject and body text

    Args:
        fromAddr (str): address to send from
        toAddrs (list): list of addresses in 'To'
        bccAddrs (list): list of addresses in 'Bcc'
        subject (str): subject of the email
        body (str): Text in the body of the email

    Returns:
        MIME object
    """
    msg = MIMEMultipart()
    msg['From'] = fromAddr
    msg['To'] = ', '.join(list(toAddrs))
    msg['Bcc'] = ', '.join(list(bccAddrs))
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    return msg


def sendEmail(mailService, toAddrs, bccAddrs, subject, body, attachments=[]):
    """Send an email using GMail API and oauth2 service authentication
       to given visible and bcc recepients with given subject,body, and attachments

    Args:
        mailService: Gmail service (from getGoogleServices()['mail'])
        visibleToAddrs (str or list): email address(es) that appear in "To"
        realToAddrs (str or list): email address(es) to which email is actually sent to
        subject (str): subject of the email
        body (str): body of the email
        attachments (list): optional list of attachements files
    """
    if isinstance(toAddrs, str):
        toAddrs = [toAddrs]
    if isinstance(bccAddrs, str):
        bccAddrs = [bccAddrs]

    msg = createMimeMsg('me', toAddrs, bccAddrs, subject, body)
    addAttachments(msg, attachments)

    retriesLeft = 5
    while retriesLeft > 0:
        retriesLeft -= 1
        try:
            # Using resumable uploadType because we have large attachments that don't always
            # succeed using the simple method
            media = MediaIoBaseUpload(BytesIO(msg.as_bytes()), mimetype='message/rfc822', resumable=True)
            result = mailService.users().messages().send(userId='me', body={}, media_body=media).execute()
            return
        except Exception as e:
            logging.error('Error sending email. %d retries left. %s', retriesLeft, str(e))
            if retriesLeft > 0:
                time.sleep(5) # wait 5 seconds before retrying
    logging.error('Too many email send failures')


def sendEmailSmtp(fromAccount, visibleToAddrs, realToAddrs, subject, body, attachments=[]):
    """Send an email using SMTP API and user/password authentication
       to given visible and bcc recepients with given subject,body, and attachments

    Args:
        fromAccount (tuple): tuple of (email, password) of from account
        visibleToAddrs (str or list): email address(es) that appear in "To"
        realToAddrs (str or list): email address(es) to which email is actually sent to
        subject (str): subject of the email
        body (str): body of the email
        attachments (list): optional list of attachements files
    """
    (fromEmail, fromPass) = fromAccount
    if isinstance(visibleToAddrs, str):
        visibleToAddrs = [visibleToAddrs]
    if isinstance(realToAddrs, str):
        realToAddrs = [realToAddrs]

    msg = createMimeMsg(fromEmail, visibleToAddrs, [], subject, body)
    addAttachments(msg, attachments)

    #send the message
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
    except Exception as e:
        print("Setting Server Failed", e)
        return

    try:
        server.starttls()
    except Exception as e:
        print("Start tls failed", e)
        return

    try:
        server.login(fromEmail, fromPass)
    except Exception as e:
        print("Server Access Failed", e)
        return

    try:
        text = msg.as_string()
    except Exception as e:
        print("Message String Failed", e)
        return

    try:
        server.sendmail(fromEmail, realToAddrs, text)
    except Exception as e:
        print("Sending Email Failed", e)
        # print("From Addess ", fromEmail)
        # print("To Address", realToAddrs)
        # print("Text", text)
        return
        
    try:
        server.quit()
    except Exception as e:
        print("Quiting Server Failed", e)


def check_email(email, passwd):
    '''check():
        Inputs: None

        Outputs:
            - locations: string list of locations of the fires that triggered the    emails
            - answers: boolean list of yes (1) or no (0) for the fire being there
    '''
    mail = imaplib.IMAP4_SSL('imap.gmail.com', 993)
    # imaplib module implements connection based on IMAPv4 protocol
    mail.login(email, passwd)
    # >> ('OK', [username at gmail.com Vineet authenticated (Success)'])
    mail.list() # Lists all labels in GMail
    mail.select('inbox') # Connected to inbox.
    result, data = mail.uid('search', None, "UNSEEN")
    locations = []
    answers = []
    # search and return uids instead
    i = len(data[0].split()) # data[0] is a space separate string
    if i!=0:
        for x in range(i):
            latest_email_uid = data[0].split()[x] # unique ids wrt label selected
            result, email_data = mail.uid('fetch', latest_email_uid, '(RFC822)')
            # fetch the email body (RFC822) for the given ID
            raw_email = email_data[0][1]
            #continue inside the same for loop as above
            raw_email_string = raw_email.decode('utf-8')
            # converts byte literal to string removing b''
            email_message = email.message_from_string(raw_email_string)
            subject = email_message['subject']
            locations.append(subject[35:])
            # this will loop through all the available multiparts in mail
            for part in email_message.walk():
                if part.get_content_type() == "text/plain": # ignore attachments/html
                    body = part.get_payload(decode=True)
                    ans = body[0:3]

            if ans == 'YES':
                answers.append(1)
            else:
                answers.append(0)

            mail.store(latest_email_uid, '+FLAGS', '\\Deleted')

        mail.expunge()
    mail.close()
    mail.logout()

    return locations, answers
