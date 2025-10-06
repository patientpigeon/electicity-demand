import requests
# response = requests.get(
#     "https://api.electricitymaps.com/v3/power-breakdown/latest?zone=US-NW-PGE",
#     headers={
#         "auth-token": "JNONTmNnMPy4pCnVqSmT"
#     }
# )
# print(response.json())


response = requests.get(
    "https://api.electricitymaps.com/v3/power-breakdown/latest?zone=US-NW-PGE",
    headers={"auth_token": "JNONTmNnMPy4pCnVqSmT"},
)
print(response.json())

# print(requests.get( "https://api.electricitymaps.com/v3/power-breakdown/latest?zone=US-NW-PGE",
#                    headers={"auth_token": "JNONTmNnMPy4pCnVqSmT"}).json())
