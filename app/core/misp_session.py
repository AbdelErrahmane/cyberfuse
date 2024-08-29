import requests
import urllib3

# Disable SSL warnings (optional but recommended when verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_misp_session(url,authkey):
    # Set up headers for the API requests, including the authentication key
    headers = {
        'Authorization': authkey,
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    # Create a session object to keep track of the authentication
    session = requests.Session()
    session.headers.update(headers)

    response = session.get(f"{url}/feeds", verify=False)
    if response.status_code == 200:
        print("Successfully!")  # Updated message
        return {"session": session}
    else:
        return{"connexion error":f"Request failed with status code: {response.status_code}"}
    
