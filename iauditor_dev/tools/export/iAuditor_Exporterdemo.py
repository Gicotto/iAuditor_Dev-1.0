from os import write

import requests, json
import polars as pl
import iAuditor_Exporter_Config as config
import configparser
from datetime import datetime


def read_last_call_time(api_config):
    try:
        last_call_time = api_config.get('API', 'last_call_time')
        print(f"Last call time is: {last_call_time}")
        return last_call_time
    except (configparser.NoOptionError, ValueError):
        return None


def write_last_call_time(api_config):
    formatted_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z').replace(':', '%3A')
    print(f"Formatted time is: {formatted_time}")

    """    
    set to true when ready to run as prod/cron job
    """
    #api_config.set('API', 'last_call_time', formatted_time)
    # with open(config.exporter_config, 'w') as configfile:
        #api_config.write(configfile)


def create_dataframe(json_payload, keys_dict, key):
    # Convert JSON payload to DataFrame
    print(f"Payload is: {json_payload}")
    df = pl.DataFrame(json_payload['data'], schema=config.datatypes[key])

    # Set Nulls
    df = df.with_columns([
        pl.when(pl.col(col).cast(pl.Utf8) == "").then(None).otherwise(pl.col(col)).alias(col)
        for col in df.columns if df[col].dtype == pl.Utf8
    ])

    # Drop columns that are not needed
    allowed_columns = config.json_keys[key]
    print(f"Table used is {key}")
    print(f"Allowed columns are: {allowed_columns}")
    columns_to_drop = [col for col in df.columns if col not in allowed_columns]
    print(f"Columns to drop are: {columns_to_drop}")
    # df = df.drop(columns_to_drop)

    print()

    # Add column - exported_at
    # if key in config.add_exported_at:
    #     df = df.with_columns(
    #         pl.lit(config.current_time).cast(pl.Datetime).alias("exported_at")
    #     )

    return  df.head(2)


def call_api(base_url, key):
    api_config = configparser.RawConfigParser()
    api_config.read(config.exporter_config)
    modified_after = read_last_call_time(api_config)
    if modified_after is None:
        raise ValueError(
            "The 'modified_after' parameter is None. Ensure 'last_call_time' is correctly set in the config.")
    print(f"Base Url is: {base_url}")

    keep_url = ["https://api.safetyculture.io/feed/schedule_assignees", "https://api.safetyculture.io/feed/users"]
    if base_url in keep_url:
        url = base_url
    else:
        url = f"{base_url}?modified_after={modified_after}"
    print(f"URL is: {url}")

    api_token = api_config.get('API', 'key')

    headers = {"accept": "application/json",
               "authorization": f"Bearer {api_token}"}
    response = requests.get(url, headers=headers)

    parsed_json = response.json()
    write_last_call_time(api_config)
    return parsed_json


def main():

    for key in config.urls:
        # Call API
        # key = "inspection_items"
        url = config.urls[key]
        parsed_json = call_api(base_url=url, key=key)

        # Create DataFrame
        dataframe = create_dataframe(json_payload=parsed_json, keys_dict=config.json_keys[key], key=key)
        print(f"Created dataframe: {dataframe} \n")

        # Write to CSV in tools/export directory (i.e. iauditor_dev/tools/export)
        dataframe.write_csv(f'{key}_raw.csv')


if __name__ == "__main__":
    main()
