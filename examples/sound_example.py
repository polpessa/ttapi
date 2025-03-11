from TikTokApi import TikTokApi
import asyncio
import os
import json

ms_token = os.environ.get("ms_token", None)  # set your own ms_token
sound_id = "100254895"


async def sound_videos():
    results = []
    async with TikTokApi() as api:
        await api.create_sessions(ms_tokens=[ms_token], num_sessions=1, sleep_after=3, browser=os.getenv("TIKTOK_BROWSER", "chromium"))
        async for sound in api.sound(id=sound_id).videos(count=30):
            print(sound)
            print(sound.as_dict)
            results.append(sound.as_dict)
    
    with open("sound.json", "w") as f:
        json.dump(results, f, indent=4)


if __name__ == "__main__":
    asyncio.run(sound_videos())
