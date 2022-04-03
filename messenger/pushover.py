# import modules
import os
import urllib
import http.client

# define functions
def pushover(message, priority=0, title=None, device=None):
    """Send notification to device via the Pushover API (https://pushover.net/api).
    
    Parameters:
    message (str): The body of the noficiation.
    priority (int): Optional. Message priority, an integer from -2 to 2, see: https://pushover.net/api#priority). Defaults to 0 (normal priority).
    title (str): Optional. The title of the notification. If None (the default), the application's name will be used.
    device (str): Optional. The name of the device to send the notification to. If None (the default), the notification will be sent to all devices.
    """

    # load Pushover configuration
    app_token = os.environ['PO_TOKEN']
    user_key = os.environ['PO_KEY']

    # assemble body
    body = {
        'token': app_token,
        'user': user_key,
        'message': message,
        'priority': priority,
        'title': title,
        'device': device
    }

    # remove unused parameters
    if (title is None):
        body.pop('title')
    if (device is None):
        body.pop('device')
    
    # encode body
    body_enc = urllib.parse.urlencode(body)

    # send notification
    conn = http.client.HTTPSConnection('api.pushover.net:443')
    conn.request('POST', '/1/messages.json', body_enc, { 'Content-type': 'application/x-www-form-urlencoded' })
    status = conn.getresponse().status

    # check response
    if (status == 200):
        print('Notification sent successfully.')
    else:
        print('Status: ' + str(status))
        print('Notification did not send successfully.')
    
    # close connection
    conn.close()