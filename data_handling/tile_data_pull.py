# Third Party Imports
import asyncio
from aiohttp import ClientSession
from pytile import async_login
from dotenv import load_dotenv

# Native Imports
from datetime import datetime
import json
import os

load_dotenv()
email = os.getenv("TILE_EMAIL")
pwd = os.getenv("TILE_PWD")
# tilenames = {
#     '0287c8181aa557e7': 'Maya',
#     '02df4813aa180c3a': 'Maya’s Backpack',
#     '06c5863b0ea97d00': 'John',
#     '06e9828702df2f1f': 'John’s Backpack',
#     'p!0028e4d51b64dafa7db22c75e373903b': 'John’s iPhone',
#     'p!27a7386a743b1de5fd19cf5c3873dea8': 'Maya’s iPhone',
#     }
async def main(email: str, pwd: str) -> None:
    """
    Function to request and save data from Tile

    Parameters
    -----------
        email (str): email to login to tile webstie
        pwed (str): password to login to tile website

    Returns
    -----------
        None
    """
    # Open client to request data
    async with ClientSession() as session:
        # login
        print('Logging in...')
        api = await async_login(email, pwd, session)
        # request data
        tiles = await api.async_get_tiles()

        # handle and save data from request return
        print("Collecting Data...")
        tile_history = {}
        for tile_uuid, tile in tiles.items():
            start = datetime(2024, 10, 1, 0, 0, 0)
            end = datetime.today()
            history = await tile.async_history(start, end)
            tile_history[tile_uuid] = history
        
        print("Saving Data...")
        with open(f'data/raw/data_{datetime.now().date()}.json', 'w') as f:
            json.dump(tile_history, f)
        print(f"Data successfully saved to 'data/raw/data_{datetime.now().date()}.json'")
asyncio.run(main(email, pwd))