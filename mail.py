import smtplib, ssl, os
from os import error

#Process ID
pid = 25377


port = 465  # For SSL
smtp_server = "smtp.gmail.com"
sender_email = "bdemirel732@gmail.com"  # Enter your address
receiver_email = "b.demirel@jacobs-university.de"  # Enter receiver address
password = "kndpnhcacwwikkgp"
message = """\
Subject: Dragon Execution

The execution of your python script on dragon server has been completed. Please check results and clean the files up."""

try:
    while True:
        os.kill(pid, 0)
except error as e:
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)
