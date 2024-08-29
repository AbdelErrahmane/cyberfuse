from core.misp_session import get_misp_session
from dotenv import load_dotenv
import os
load_dotenv()
url = os.getenv('MISP_URL')
authkey = os.getenv('MISP_AUTHKEY')


def check_misp_connexion():
    session = get_misp_session(url,authkey)
    if "session" in session:
        return {"connexion": "Successful"}
    else:
        raise ValueError("MISP session not found")

def get_feeds_connexion():
    session = get_misp_session(url,authkey)
    if "session" in session:
        response = session.get(f"{url}/feeds", verify=False)
        if response.status_code == 200:
            print("Request successful!")
            return response.json()
        else:
            print(f"Request failed with status code: {response.status_code}")
        pass
    else:
        raise ValueError("MISP session not found")