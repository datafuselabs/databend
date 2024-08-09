#!/usr/bin/env python3

import base64
import json
import time

import requests
from pprint import pprint

# Define the URLs and credentials
query_url = "http://localhost:8000/v1/query"
login_url = "http://localhost:8000/v1/session/login"
renew_url = "http://localhost:8000/v1/session/renew"
auth = ("root", "")


def print_error(func):
    def wrapper(*args, **kwargs):
        print(f"---- {func.__name__}{args[:1]}")
        resp = func(*args, **kwargs)
        err = resp.get("error")
        if err:
            pprint(err)
        return resp

    return wrapper


@print_error
def do_login():
    payload = {}
    response = requests.post(
        login_url,
        auth=auth,
        headers={"Content-Type": "application/json"},
        json=payload,
    )
    return response.json()


@print_error
def do_renew(_case_id, refresh_token, session_token):
    payload = {"session_token": session_token}
    response = requests.post(
        renew_url,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {refresh_token}"},
        json=payload,
    )
    return response.json()


@print_error
def do_query(query, session_token):
    query_payload = {"sql": query, "pagination": {"wait_time_secs": 11}}
    response = requests.post(
        query_url,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {session_token}"},
        json=query_payload,
    )
    return response.json()


def fake_expired_token():
    expired_claim = {"exp": int(time.time()) - 10, "tenant": "", "user": "", "nonce": "", "sid": ""}
    return "bend-v1-" + base64.b64encode(json.dumps(expired_claim).encode("utf-8")).decode("utf-8")


def main():
    login_resp = do_login()
    pprint(sorted(login_resp.keys()))
    print(login_resp.get("refresh_token_validity_in_secs"))
    session_token = login_resp.get("session_token")
    refresh_token = login_resp.get("refresh_token")
    # print(session_token)

    # ok
    query_resp = do_query("select 1", session_token)
    pprint(query_resp.get("data"))
    # errors
    do_query("select 2", "xxx")
    do_query("select 3", "bend-v1-xxx")
    do_query("select 4", fake_expired_token())
    do_query("select 5", refresh_token)

    renew_resp = do_renew(1, refresh_token, session_token)
    pprint(sorted(renew_resp.keys()))
    print(renew_resp.get("refresh_token_validity_in_secs"))
    new_session_token = renew_resp.get("session_token")
    new_refresh_token = renew_resp.get("refresh_token")

    # old session_token still valid
    query_resp = do_query("select 6", session_token)
    pprint(query_resp.get("data"))

    query_resp = do_query("select 7", new_session_token)
    pprint(query_resp.get("data"))

    # errors
    do_renew(2, "xxx", session_token)
    do_renew(3, "bend-v1-xxx", session_token)
    do_renew(4, fake_expired_token(), session_token)
    do_renew(5, session_token, session_token)

    # test new_refresh_token works
    do_renew(6, new_refresh_token, session_token)


if __name__ == "__main__":
    main()
