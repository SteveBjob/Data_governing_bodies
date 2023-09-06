# Databricks notebook source
import json
import requests

# COMMAND ----------

class Employee_Data(Token):
    def __init__(self, url: str):
        super().__init__(url, grant_type, client_id, client_secret, content_type)
        self.url = url
        self.x_tid = ""
        self.Authorization = ""
        self.response_body = {}
        self.response_headers = {}

    def header_info(self, mytoken:str):
        self.access_token = mytoken.response_body['access_token']
        self.token_type = mytoken.response_body['token_type']
        self.x_tid = mytoken.response_headers['X-Tid']
        self.Authorization = self.token_type+" "+self.access_token

    def payload(self, start:str, end:str):
        return json.dumps({
            "paramList": [
              {
                "param": "pin",
                "value": ""
              },
              {
                "param": "startrange",
                "value": start
              },
              {
                "param": "endrange",
                "value": end
              }
            ]
          })
        
    def headers(self):
        return {
          'Content-Type': 'application/json',
          'x-tid': self.x_tid,
          'Authorization': self.Authorization,
        }

    def send_request(self, start:str, end:str):
        try:
            response = requests.post(self.url, headers=self.headers(), data=self.payload(start,end))
            response.raise_for_status()  # Raise an exception for bad response status
            self.response_body = response.json()
            self.response_headers = response.headers
            return self.response_body, self.response_headers
        except requests.exceptions.RequestException as e:
            print("Request error:", e)
            return None

    def __str__(self):
        return (
            f"Token(\n"
            f"    url={self.url}\n"
            f"    X_tid={self.x_tid}\n"
            f"    Authorization={self.Authorization}\n"
            f")"
        )
