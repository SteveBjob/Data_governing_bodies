# Databricks notebook source
import requests

# COMMAND ----------

class Token:
    def __init__(self, url: str, grant_type: str, client_id: str, client_secret: str, content_type: str):
        self.url = url
        self.grant_type = grant_type
        self.client_id = client_id
        self.client_secret = client_secret
        self.content_type = content_type
        self.response_body = {}
        self.response_headers = {}

    def payload(self):
        return {
            "grant_type": self.grant_type,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

    def headers(self):
        return {
            'Content-Type': self.content_type
        }

    def send_request(self):
        try:
            response = requests.post(self.url, headers=self.headers(), data=self.payload())
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
            f"    grant_type={self.grant_type}\n"
            f"    client_id={self.client_id}\n"
            f"    client_secret={self.client_secret}\n"
            f"    content_type={self.content_type}\n"
            f")"
        )
