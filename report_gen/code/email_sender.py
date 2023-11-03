import logging
import tomli
import smtplib
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os

logger = logging.getLogger("email_sender");
logging.basicConfig(level=logging.DEBUG)  # move to log config file using python functionality

def get_config():
    with open("./config/config.toml", "rb") as f:
        toml_conf = tomli.load(f)
    logger.info(f"config:{toml_conf}")
    return toml_conf

def connect_smtp(conf):
    smtp_conf = conf['smtp']
    host = smtp_conf['host']
    port = smtp_conf['port']
    timeout = smtp_conf['timeout']

    logger.info(f"SMTP connecting to {host}:{port} (to={timeout}s)")
    conn = smtplib.SMTP(host,port,timeout=timeout)

    logger.info("Connected - Setting up TLS")
    (code,resp) = conn.starttls() #setting up to TLS connection
    
    logger.info("TLS connection established")
    conn.ehlo()

    username = smtp_conf['username']
    password = smtp_conf['password']
    logger.info(f"Authenticating for username {username}")
    conn.login(username,password)
    logger.info("Authentication successful")

    return conn

def send_email(conf,subject,body,filenames):
    addr_from = conf['smtp']['username']
    email = create_email(addr_from,conf['to'],subject,body,filenames)
    
    conn = connect_smtp(conf)
    conn.send_message(email)

def create_email(addr_from,addr_to,subject,body,filenames):
    email = MIMEMultipart()

    email['From'] = addr_from
    email['To'] = addr_to
    email['Subject'] = subject
    email.attach(MIMEText(body,'plain')) #body
    for filename in filenames:
        with open(filename,'rb') as fp:
            email.attach(MIMEApplication(fp.read(), Name=os.path.basename(filename)))
    logger.info(f"Create email: {email}")
    return email

