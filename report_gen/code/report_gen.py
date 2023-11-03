import logging
import tomli
import influxdb_client
import re
import datetime
import os
import pandas as pd
import numpy as np
import sched
import time
import email_sender

logging.basicConfig(level=logging.DEBUG)  # move to log config file using python functionality
logger = logging.getLogger("report_gen");
# logging.getLogger("matplotlib").setLevel(logging.WARNING)

def get_config():
    with open("./config.toml", "rb") as f:
        toml_conf = tomli.load(f)
    logger.info(f"config:{toml_conf}")
    return toml_conf

def analyse(conf):
    # INFLUX CLIENT STUFF GOES HERE
    bucket = conf["source"]["bucket"]
    org = conf["source"]["org"]
    token = conf["source"]["token"]
    url = conf["source"]["url"]

    client = influxdb_client.InfluxDBClient(
            url=url,
            token=token,
            org=org
            )
    #CHECK AND ENSURE THE REPORT GENERATION DIRECTORY EXISTS
    if 'out' not in os.listdir():
        os.mkdir('out')
    
    # TIMINGS
    window_str = conf["report"].get("window","1d")
    window = get_time_delta(window_str)
    
    interval_str = conf["report"].get("interval","1h")
    interval = get_time_delta(interval_str)

    anchor_time_str = conf["report"].get("time","12:00")
    anchor_time = datetime.time.fromisoformat(anchor_time_str)

    email_conf = conf["email"]

    logger.debug(f"window_str {window_str}")
    logger.debug(f"window {window}")

    next_time = datetime.datetime.combine(datetime.date.today(),anchor_time)
    now = datetime.datetime.now()
    if next_time > now:
        while next_time-interval > now:
            next_time = next_time - interval
    else:
        while next_time < now:
            next_time = next_time + interval
    
    def t_now():
        return datetime.datetime.now().timestamp()
    
    scheduler = sched.scheduler(t_now,time.sleep)

    while True:
        logger.info(f"Next running at {next_time.isoformat()}")
        scheduler.enterabs(next_time.timestamp(),1,do_analysis,kwargs={"client":client,"org":org,"bucket":bucket,"window":window,"email_conf":email_conf})
        scheduler.run()
        next_time = next_time+interval

def do_analysis(client,org,bucket,window,email_conf):
    logger.info("Running Analysis")
    
    query_api = client.query_api()
    
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    start = (now - window).isoformat()
    end = now.isoformat()
    logger.info(f"Analysis window is from {start} to {end}")

    rate_fn = rate_report(query_api,org,bucket,start,end)
    prod_fn = production_report(query_api,org,bucket,start,end)

    email_sender.send_email(email_conf,f"Production Report {datetime.date.today()}","Report Attached",[rate_fn,prod_fn])

def production_report(query_api,org,bucket,start,end):
        query = f'''from(bucket: "{bucket}")
          |> range(start: {start}, stop: {end})
          |> filter(fn: (r) => r["_measurement"] == "production")
          |> pivot(columnKey: ["_field"], valueColumn: "_value", rowKey: ["_time"])
          |> drop(columns: ["_start","_stop","_measurement"])'''
        logger.debug(f"flux_query is {query}")
 
        timer_start = time.time()
        prod_df = query_api.query_data_frame(org=org, query=query)
        timer_end = time.time()
        
        logger.debug(f"influx query took: {timer_end - timer_start}s")
        logger.debug(f"prod_df {prod_df.keys()}")                
        
        del prod_df["result"]
        del prod_df["table"]

        prod_df['Date'] = pd.to_datetime(prod_df['_time']).dt.date
        prod_df['Time'] = pd.to_datetime(prod_df['_time']).dt.time

        logger.debug(f"prod_df {prod_df}")                

        query = f'''from(bucket: "{bucket}")
          |> range(start: {start}, stop: {end})
          |> filter(fn: (r) => r["_measurement"] == "batch_details")
          |> pivot(columnKey: ["_field"], valueColumn: "_value", rowKey: ["_time"])
          |> drop(columns: ["_start","_stop","_measurement"])'''
  
        timer_start = time.time()
        batch_df = query_api.query_data_frame(org=org, query=query)
        timer_end = time.time()
        
        logger.debug(f"influx query took: {timer_end - timer_start}s")

        del batch_df["result"]
        del batch_df["table"]
        del batch_df["quantity"]
        logger.debug(f"batch_df {batch_df}")

        prod_df.sort_values(by='_time',ascending=True)
        batch_df.sort_values(by='_time',ascending=True)
        
        ind_list = [
                {
                    "i":prod_df.index[0],
                    "batch":"not_set",
                    "product":"not_set",
                    "expires":"not_set",
                    }
                ]

        for i in batch_df.index:
            next_ind = prod_df['_time'].searchsorted(batch_df['_time'][i], side='left')
            ind_list.append({
                "i":next_ind,
                "batch":batch_df.iloc[i]["batch"],
                "product":batch_df.iloc[i]["product"],
                "expires":batch_df.iloc[i]["expires"],
                })
        
        ind_list.append({"i":prod_df.index[-1]+1})

        logger.debug(f"ind_list {ind_list}")

        batch = []
        product = []
        expires = []
        start_ind = ind_list[0]
        
        for next_ind in ind_list[1:]:
            batch = batch + [start_ind['batch']]*(next_ind["i"]-start_ind["i"])
            product = product + [start_ind["product"]]*(next_ind["i"]-start_ind["i"])
            expires = expires + [start_ind["expires"]]*(next_ind["i"]-start_ind["i"])
            start_ind = next_ind

        prod_df['batch'] = batch
        prod_df['product'] = product
        prod_df['expires'] = expires
        
        return generate_report(f"produced-{datetime.date.today()}", prod_df)

def rate_report(query_api,org,bucket,start,end):
        query = f'''from(bucket: "{bucket}")
          |> range(start: {start}, stop: {end})
          |> filter(fn: (r) => r["_measurement"] == "production")
          |> filter(fn: (r) => r["_field"] == "count")
          |> aggregateWindow(every:5m, fn: sum, createEmpty: true)
          |> pivot(columnKey: ["_field"], valueColumn: "_value", rowKey: ["_time"])
          |> drop(columns: ["_start","_stop","_measurement"])
          '''
    
        logger.debug(f"flux_query is {query}")
 
        timer_start = time.time()
        prod_rate_df = query_api.query_data_frame(org=org, query=query)
        timer_end = time.time()
        
        logger.debug(f"influx query took: {timer_end - timer_start}s")
        logger.debug(f"prod_rate_df {prod_rate_df.keys()}")                
        
        del prod_rate_df["result"]
        del prod_rate_df["table"]

        prod_rate_df['Date'] = pd.to_datetime(prod_rate_df['_time']).dt.date
        prod_rate_df['Time'] = pd.to_datetime(prod_rate_df['_time']).dt.time

        return generate_report(f"production_rate-{datetime.date.today()}", prod_rate_df)

    
def generate_report(name,data):
    # os.mkdir(f'out/{name}')
    filename = f"out/{name}.csv"
    data.to_csv(filename)
    logger.debug(f"generated {filename}")
    return filename
    
def get_time_delta(val):
    days_match = re.match(".*?(\d+)d.*?",val)
    days = int(days_match.group(1)) if days_match is not None else 0
    hours_match = re.match(".*?(\d+)h.*?",val)
    hours = int(hours_match.group(1)) if hours_match is not None else 0
    minutes_match = re.match(".*?(\d+)m.*?",val)
    minutes = int(minutes_match.group(1)) if minutes_match is not None else 0
    seconds_match = re.match(".*?(\d+)s.*?",val)
    seconds = int(seconds_match.group(1)) if seconds_match is not None else 0
    return datetime.timedelta(days=days,hours=hours,minutes=minutes,seconds=seconds)

def run():
    conf = get_config()
    if conf["logging"]:
        if conf["logging"] == "info":
            logging.basicConfig(level=logging.INFO)
    analyse(conf)

if __name__ == "__main__":
    run()
