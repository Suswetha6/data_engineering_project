import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(
    "service_account.json", scopes=SCOPES
)

client = gspread.authorize(creds)

sheet = client.open_by_key(
    "1LhXrKvLHL4kVMS1VnOVxpLSl0A4QPKvUpLRyd3zcc2w"
).sheet1

print(sheet.get_all_values())
