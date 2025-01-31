import pandas as pd
from espo_api_client import EspoAPI
import re
import os
import json
import time
from dotenv import load_dotenv
import click
from datetime import datetime, timedelta
import sys
import logging

load_dotenv()

espo_client_ukr = EspoAPI(os.getenv("ESPOURL_UKR"), os.getenv("ESPOAPIKEY_UKR"))
espo_client_svk = EspoAPI(os.getenv("ESPOURL_SVK"), os.getenv("ESPOAPIKEY_SVK"))

entities_to_migrate = [
    "Contact",
    "GeneralCases",
    "Voucher",
    "CommunityActivity",
    "ShelterInfo",
    "ShelterVerification",
    "Shelter",
    "ShelterVisit",
    "FinalEvaluation",
    "Payment",
    "VocationalTraining",
    "HealthMonitor",
    "SlovakWinter",
]

# map teams from ukr to svk espo
teams_ukr = espo_client_ukr.request("GET", "Team")["list"]
teams_svk = espo_client_svk.request("GET", "Team")["list"]
teams_map = {}
for team_ukr in teams_ukr:
    for team_svk in teams_svk:
        if team_ukr["name"] == team_svk["name"]:
            teams_map[team_ukr["id"]] = team_svk["id"]


# loop over entities
for entity in entities_to_migrate[:1]:

    # if there is a field 'country', filter records by country
    params = {
        "where": [{"type": "equals", "attribute": "country", "value": "Slovakia"}]
    }
    records = espo_client_ukr.request("GET", entity, params)["list"]

    for record in records[:1]:

        # replace ukr teamId with svk teamId
        params = {"select": "teamsIds"}
        recordTeamsIds = espo_client_ukr.request(
            "GET", f"{entity}/{record['id']}", params
        )["teamsIds"]
        teamsIds = []
        for teamId in recordTeamsIds:
            if teamId in teams_map.keys():
                teamsIds.append(teams_map[teamId])
        record["teamsIds"] = teamsIds

        # fix phone number
        if entity == "Contact":
            if (
                record["phoneNumber"][:4] == "+421"
                or record["phoneNumber"][:4] == "+380"
            ):
                pass
            elif (
                record["phoneNumber"][:3] == "421" or record["phoneNumber"][:3] == "380"
            ):
                record["phoneNumber"] = "+%s" % record["phoneNumber"]
            elif record["phoneNumber"][:2] == "09":
                record["phoneNumber"] = "+421%s" % record["phoneNumber"][1:]
            elif record["phoneNumber"][:2] == "06":
                record["phoneNumber"] = "+380%s" % record["phoneNumber"][1:]
            else:
                pass

        # create new record in svk espo
        new_record = espo_client_svk.request("POST", entity, record)

        # migrate stream
        recordStream = espo_client_ukr.request(
            "GET", f"{entity}/{record['id']}/stream"
        )["list"]
        for post in recordStream:
            post["parentId"] = new_record["id"]
            espo_client_svk.request("POST", "Note", post)
